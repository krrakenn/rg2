import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
import json
from utils import get_secret
import time
import sqlite3

DB_FILE = "automation.db"
service_account_info = get_secret("SERVICE_ACCOUNT_JSON")
scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS automations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sheet_url TEXT NOT NULL,
            sql_query TEXT NOT NULL,
            refresh_frequency TEXT NOT NULL,
            layout_mapping TEXT NOT NULL,
            query_type TEXT DEFAULT 'no_date',
            last_run TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_updated TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()


def store_automation(sheet_url, sql_query, refresh_frequency, layout_mapping, query_type="no_date"):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO automations
        (sheet_url, sql_query, refresh_frequency, layout_mapping, query_type, last_updated)
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """, (sheet_url, sql_query, refresh_frequency, json.dumps(layout_mapping), query_type))

    conn.commit()
    automation_id = cursor.lastrowid
    conn.close()

    return automation_id


def generate_layout_mapping(df):
    mapping = {}
    df = df.copy()

    if df.shape[1] > 1 and pd.api.types.is_string_dtype(df.iloc[:, 0]):
        entity_col = df.columns[0]
        for _, row in df.iterrows():
            entity = row[entity_col]
            for metric in df.columns[1:]:
                key = f"{entity} - {metric}"
                mapping[key] = row[metric]
        return mapping

    if len(df) == 1:
        row = df.iloc[0]
        for metric, value in row.items():
            mapping[metric] = value
        return mapping

    for idx, row in df.iterrows():
        for metric, value in row.items():
            key = f"{idx} - {metric}"
            mapping[key] = value

    return mapping


def get_existing_metrics(ws):
    col = ws.col_values(1)
    metric_rows = {}

    for i, val in enumerate(col):
        if i == 0:
            continue
        metric_rows[val] = i + 1

    return metric_rows


def get_existing_dates(ws):
    row = ws.row_values(1)
    date_cols = {}

    for i, val in enumerate(row):
        if i == 0:
            continue
        date_cols[val] = i + 1

    return date_cols


def generate_column_header(query_type, frequency):
    today = datetime.now()
    
    if query_type == "no_date":
        if frequency.lower() == "daily":
            return today.strftime("%Y-%m-%d")
        
        elif frequency.lower() == "weekly":
            week_num = today.isocalendar()[1]
            month_name = today.strftime("%b")
            return f"{month_name} Week {week_num}"
        
        elif frequency.lower() == "monthly":
            return today.strftime("%B")
    
    elif query_type == "with_date":
        if frequency.lower() == "daily":
            return today.strftime("%Y-%m-%d")
        
        elif frequency.lower() == "weekly":
            start_date = (today - timedelta(days=7)).strftime("%d")
            end_date = today.strftime("%d")
            month_name = today.strftime("%b")
            return f"{month_name} {start_date}-{end_date}"
        
        elif frequency.lower() == "monthly":
            return today.strftime("%B")
    
    return today.strftime("%Y-%m-%d")

init_db()
def automate_report(sheet_url, result_df, sql_query, refresh_frequency, query_type="no_date"):
    init_db()
    layout_mapping = generate_layout_mapping(result_df)
    automation_id = store_automation(
        sheet_url,
        sql_query,
        refresh_frequency,
        layout_mapping,
        query_type
    )
    if isinstance(service_account_info, str):
        service_account_info = json.loads(service_account_info)
    creds = Credentials.from_service_account_info(
        service_account_info,
        scopes=scopes
    )
    client = gspread.authorize(creds)
    sheet = client.open_by_url(sheet_url)

    try:
        ws = sheet.sheet1
    except Exception:
        ws = sheet.add_worksheet(title="Report", rows=1000, cols=200)

    column_header = generate_column_header(query_type, refresh_frequency)

    if ws.cell(1, 1).value is None:
        ws.update_cell(1, 1, "KPIs")
        ws.format("A1", {"textFormat": {"bold": True}})

    existing_dates = get_existing_dates(ws)
    if column_header in existing_dates:
        date_col = existing_dates[column_header]
    else:
        date_col = len(existing_dates) + 2
        ws.update_cell(1, date_col, column_header)
        ws.format(gspread.utils.rowcol_to_a1(1, date_col), {"textFormat": {"bold": True}})

    existing_metrics = get_existing_metrics(ws)

    for metric, value in layout_mapping.items():
        if metric in existing_metrics:
            row = existing_metrics[metric]
        else:
            row = len(existing_metrics) + 2
            ws.update_cell(row, 1, metric)
            time.sleep(0.1)
            existing_metrics[metric] = row
        ws.update_cell(row, date_col, value)
        time.sleep(0.1)
    
    ws.format(f"A1:A{len(existing_metrics)+1}", {"textFormat": {"bold": True}})
    
    return {
        "automation_id": automation_id,
        "sheet_url": sheet_url,
        "refresh_frequency": refresh_frequency,
        "query_type": query_type,
        "status": "success"
    }
