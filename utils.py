'''import os
def get_secret(key):
    try:
        import streamlit as st
        return st.secrets[key]
    except:
        return os.getenv(key)
'''
import os
import json

def get_secret(key):
    try:
        import streamlit as st
        val = st.secrets[key]
    except:
        val = os.getenv(key)

    # Parse JSON if it's a string
    if isinstance(val, str):
        try:
            val = json.loads(val)
        except json.JSONDecodeError:
            pass

    # Fix private_key formatting for Google service account
    if isinstance(val, dict) and "private_key" in val:
        key = val["private_key"]
        key = key.strip().strip('"').strip("'")  # remove extra quotes/whitespace
        key = key.replace("\\n", "\n")           # convert literal \n → real newlines
        val["private_key"] = key

    return val
