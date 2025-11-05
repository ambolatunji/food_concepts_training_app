import pandas as pd
from pathlib import Path

TEMPLATES_DIR = Path("data")
TEMPLATES_DIR.mkdir(parents=True, exist_ok=True)

def write_employee_template()->Path:
    df = pd.DataFrame([{
        "Employee Name": "",
        "Email": "",
        "Employee Code": "",
        "Department": "",
        "Store": "",
        "Position": "",
        "Region": "",
        "Start Date (YYYY-MM-DD)": ""
    }])
    p = TEMPLATES_DIR / "Employee_Upload_Template.xlsx"
    df.to_excel(p, index=False)
    return p

def write_training_template()->Path:
    df = pd.DataFrame([{
        "Employee Name": "",
        "Email": "",
        "Department": "",
        "Store": "",
        "Region": "",
        "Training Title": "",
        "Training Venue": "",
        "Training Date (YYYY-MM-DD)": "",
        "Evidence File Name (optional)": ""
    }])
    p = TEMPLATES_DIR / "Training_Upload_Template.xlsx"
    df.to_excel(p, index=False)
    return p
