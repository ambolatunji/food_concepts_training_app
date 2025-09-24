import streamlit as st
from datetime import datetime

st.set_page_config(
    page_title="Food Concepts Training App",
    page_icon="✅",
    layout="wide"
)

st.logo = "assets/logo.png"  # optional, won’t break if missing

st.markdown("""
# Food Concepts Training App

Welcome! Use the sidebar to navigate:
- **📥 Record Training** – staff submit training attendance and upload evidence.
- **📊 Dashboard** – view training summaries and download reports.
- **🛠️ Admin** – upload/clean employee master, dedupe, configure, and manage records (login required).
- **📑 Reporting** – generate PowerPoint/PDF reports (login required).
""")

st.caption(f"Local time: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
