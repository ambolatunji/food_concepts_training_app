# Food Concepts Training App (Streamlit)

## Features

- Staff submit training records with evidence
- Auto-compute next due date (+365 days, push to weekday)
- Searchable dropdowns for Employee Name, Department, Store, Position, Region
- Admin login, employee master upload with dedupe
- Dashboard: filters, KPIs, charts, CSV export
- Email confirmations + manual reminder sender

## Quickstart

1. `python -m venv .venv && . .venv/Scripts/activate` (Windows) or `source .venv/bin/activate` (macOS/Linux)
2. `pip install -r requirements.txt`
3. Create `.streamlit/secrets.toml` (see example)
4. `streamlit run app.py`

## Notes

- SQLite DB at `data/training.db`
- Evidence files saved in `data/evidence/`
- Templates downloadable from **Admin** page
- Update lookup lists anytime by seed or uploads
