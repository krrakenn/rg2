import sqlite3
import json
import logging
from datetime import datetime, timedelta
from query_runner import run_sql
from sheets_automation2 import automate_report, init_db, generate_layout_mapping
import time

DB_FILE = "automation.db"
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def inject_date_range(sql_query, frequency):
    from sql_generator import generate_sql
    
    today = datetime.now()
    
    if frequency.lower() == "daily":
        start_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")
        end_date = today.strftime("%Y-%m-%d")
        period = "last 1 day"
    elif frequency.lower() == "weekly":
        start_date = (today - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = today.strftime("%Y-%m-%d")
        period = "last 7 days"
    else:
        start_date = (today - timedelta(days=30)).strftime("%Y-%m-%d")
        end_date = today.strftime("%Y-%m-%d")
        period = "last 30 days"
    
    prompt = f"""Update this SQL query with correct date ranges for {period}:

Original Query:
{sql_query}

Use these dates:
- Start Date: {start_date}
- End Date: {end_date}

Replace any hardcoded date values in WHERE clauses with these new dates.
Keep everything else exactly the same.

Return ONLY the updated SQL query, no explanation:"""
    
    updated_query = generate_sql("", prompt, "")
    return updated_query.strip()


def get_due_automations():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    now = datetime.now()
    due_automations = []
    
    cursor.execute("""
        SELECT 
            id, sheet_url, sql_query, refresh_frequency, 
            query_type, last_run
        FROM automations
    """)
    
    automations = cursor.fetchall()
    conn.close()
    
    for auto in automations:
        auto_id, sheet_url, sql_query, frequency, query_type, last_run = auto
        
        if last_run is None:
            is_due = True
        else:
            last_run_dt = datetime.fromisoformat(last_run)
            
            if frequency.lower() == "daily":
                is_due = (now - last_run_dt).days >= 1
            elif frequency.lower() == "weekly":
                is_due = (now - last_run_dt).days >= 7
            elif frequency.lower() == "monthly":
                is_due = (now - last_run_dt).days >= 30
            else:
                is_due = False
        
        if is_due:
            due_automations.append({
                "id": auto_id,
                "sheet_url": sheet_url,
                "sql_query": sql_query,
                "frequency": frequency,
                "query_type": query_type
            })
    
    return due_automations


def run_automation(automation):
    auto_id = automation["id"]
    sheet_url = automation["sheet_url"]
    sql_query = automation["sql_query"]
    frequency = automation["frequency"]
    query_type = automation["query_type"]
    
    try:
        logger.info(f"Running automation {auto_id}")
        
        final_query = sql_query
        if query_type == "with_date":
            final_query = inject_date_range(sql_query, frequency)
            logger.info(f"Date range injected for {frequency}")
        
        logger.info(f"Executing query...")
        result_df = run_sql(final_query)
        
        logger.info(f"Pushing to Google Sheet: {sheet_url}")
        automate_report(
            sheet_url=sheet_url,
            result_df=result_df,
            sql_query=final_query,
            refresh_frequency=frequency,
            query_type=query_type
        )
        
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE automations 
            SET last_run = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (auto_id,))
        conn.commit()
        conn.close()
        
        logger.info(f"Automation {auto_id} completed successfully")
        return {"status": "success", "auto_id": auto_id}
        
    except Exception as e:
        logger.error(f"Automation {auto_id} failed: {str(e)}")
        return {"status": "failed", "auto_id": auto_id, "error": str(e)}


def scheduler_loop():
    init_db()
    logger.info("Scheduler started")
    
    while True:
        try:
            due_automations = get_due_automations()
            
            if due_automations:
                logger.info(f"Found {len(due_automations)} automations to run")
                
                for automation in due_automations:
                    run_automation(automation)
            
            time.sleep(300)
            
        except Exception as e:
            logger.error(f"Scheduler error: {str(e)}")
            time.sleep(300)


if __name__ == "__main__":
    scheduler_loop()        