import streamlit as st
from pathlib import Path
from core.logic import compute_next_due, file_safe_name
from core.emailer import send_confirmation
from core.db import (
    get_conn, migrate, employees_by_name, insert_training, distinct_training_values,
    get_employee_training_history
)


migrate()
conn = get_conn()

st.title("📥 Record Training")
st.info("Pick your name, we'll auto-fill your details. Then choose date, title, venue, and upload evidence.")

# 1) Employee Name (searchable)
emp_names = conn.execute("""
    SELECT DISTINCT name FROM employees
    WHERE name IS NOT NULL AND TRIM(name)!=''
    ORDER BY name COLLATE NOCASE
""").fetchall()
employee_names = [r[0] for r in emp_names]
name = st.selectbox("Employee Name", options=employee_names, index=None, placeholder="Search your name...")

# 2) Auto-populate (disambiguate duplicate names by Store -> Department)
email = department = store = position = region = None
emp_id = None

if name:
    matches = employees_by_name(conn, name)
    if len(matches) == 1:
        emp_id, _, email, department, store, position, region = matches[0]
    elif len(matches) > 1:
        st.warning("Multiple employees share this name. Please select your Store (and Department if needed).")
        stores = sorted({m[4] for m in matches if m[4]})
        sel_store = st.selectbox("Your Store", options=stores, index=None, placeholder="Search store...")
        if sel_store:
            subset = [m for m in matches if m[4] == sel_store]
            if len(subset) == 1:
                emp_id, _, email, department, store, position, region = subset[0]
            else:
                depts = sorted({m[3] for m in subset if m[3]})
                sel_dept = st.selectbox("Your Department", options=depts, index=None, placeholder="Search department...")
                if sel_dept:
                    subset2 = [m for m in subset if m[3] == sel_dept]
                    if subset2:
                        emp_id, _, email, department, store, position, region = subset2[0]

# 3) Show auto-filled fields (read-only)
colA, colB = st.columns(2)
with colA:
    st.text_input("Department", value=department or "", disabled=True)
    st.text_input("Position", value=position or "", disabled=True)
    st.text_input("Work Email", value=email or "", disabled=True)
with colB:
    st.text_input("Store", value=store or "", disabled=True)
    st.text_input("Region", value=region or "", disabled=True)

# Display Training History
if emp_id:
    st.markdown("---")
    st.subheader("📚 Your Training History")

    training_history = get_employee_training_history(conn, emp_id)

    if training_history:
        import pandas as pd
        from datetime import date

        # Convert to DataFrame for better display
        history_df = pd.DataFrame(training_history, columns=[
            'Training Title', 'Training Date', 'Next Due Date', 'Days Until Due', 'Training Venue', 'Status'
        ])

        # Convert dates to more readable format
        history_df['Training Date'] = pd.to_datetime(history_df['Training Date']).dt.strftime('%Y-%m-%d')
        history_df['Next Due Date'] = pd.to_datetime(history_df['Next Due Date']).dt.strftime('%Y-%m-%d')

        # Style the dataframe with colors based on status
        def color_status(val):
            if val == 'Overdue':
                return 'background-color: #ffcccc; color: #cc0000; font-weight: bold'
            elif val == 'Due Soon (≤30 days)':
                return 'background-color: #fff4cc; color: #cc8800; font-weight: bold'
            elif val == 'Due Today':
                return 'background-color: #ffe6cc; color: #cc4400; font-weight: bold'
            else:
                return 'background-color: #ccffcc; color: #006600'

        styled_df = history_df.style.applymap(color_status, subset=['Status'])

        # Display with metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            total_trainings = len(history_df)
            st.metric("Total Trainings Completed", total_trainings)
        with col2:
            overdue_count = len(history_df[history_df['Status'] == 'Overdue'])
            st.metric("Overdue Trainings", overdue_count, delta=None if overdue_count == 0 else -overdue_count, delta_color="inverse")
        with col3:
            due_soon_count = len(history_df[history_df['Status'] == 'Due Soon (≤30 days)'])
            st.metric("Due Soon", due_soon_count, delta=None if due_soon_count == 0 else -due_soon_count, delta_color="inverse")

        st.dataframe(styled_df, use_container_width=True, height=min(400, (len(history_df) + 1) * 35 + 38))

        # Show eligibility message
        if overdue_count > 0:
            st.warning(f"⚠️ You have {overdue_count} overdue training(s). Please schedule a refresher training soon.")
        elif due_soon_count > 0:
            st.info(f"ℹ️ You have {due_soon_count} training(s) due within 30 days. Plan ahead for your next training.")
        else:
            st.success("✅ All your trainings are up to date! Great job!")
    else:
        st.info("No training records found. This will be your first training entry.")

    st.markdown("---")

with st.expander("Request a correction to your employee details"):
    new_name = st.text_input("Corrected Name", value=name or "")
    new_email = st.text_input("Corrected Work Email", value=email or "")
    new_dept  = st.text_input("Corrected Department", value=department or "")
    new_store = st.text_input("Corrected Store", value=store or "")
    new_pos   = st.text_input("Corrected Position", value=position or "")
    new_reg   = st.text_input("Corrected Region", value=region or "")

    if st.button("Submit Correction Request", use_container_width=True):
        if not emp_id:
            st.error("Select your name first so we can link the request.")
        else:
            payload = {}
            # only include fields that actually changed
            if new_name and new_name != (name or ""): payload["name"] = new_name
            if new_email != (email or ""): payload["email"] = new_email
            if new_dept  != (department or ""): payload["department"] = new_dept
            if new_store != (store or ""): payload["store"] = new_store
            if new_pos   != (position or ""): payload["position"] = new_pos
            if new_reg   != (region or ""): payload["region"] = new_reg

            if not payload:
                st.info("No changes detected.")
            else:
                from core.db import create_change_request
                create_change_request(conn, emp_id, payload)
                st.success("Correction submitted for admin approval.")


# 4) Training Title & Venue: suggest from history, allow "Add new"
existing_titles = distinct_training_values(conn, "training_title")
existing_venues = distinct_training_values(conn, "training_venue")

title_choice = st.selectbox("Training Title", options=["<Add new>"] + existing_titles, index=0)
if title_choice == "<Add new>":
    training_title = st.text_input("Enter New Training Title")
else:
    training_title = title_choice

venue_choice = st.selectbox("Training Venue", options=["<Add new>"] + existing_venues, index=0)
if venue_choice == "<Add new>":
    training_venue = st.text_input("Enter New Training Venue")
else:
    training_venue = venue_choice

# 5) Date + Evidence
training_date = st.date_input("Training Date", value=None, format="YYYY-MM-DD")
evidence = st.file_uploader("Upload Evidence (JPG/PNG/PDF, ≤10 MB)", type=["jpg","jpeg","png","pdf"])

# 6) Submit
if st.button("Submit Training", type="primary", use_container_width=True):
    # validations
    if not name:
        st.error("Please select your name.")
        st.stop()
    if not emp_id:
        st.error("We couldn't uniquely identify your record (Store/Department needed).")
        st.stop()
    if not training_date:
        st.error("Please select a training date.")
        st.stop()
    if not training_title:
        st.error("Please provide a Training Title.")
        st.stop()
    if not training_venue:
        st.error("Please provide a Training Venue.")
        st.stop()

    tdate = training_date.isoformat()
    next_due = compute_next_due(tdate)

    # Save evidence
    ev_path = None; ev_mime=None; ev_size=None
    if evidence is not None:
        if evidence.size > 10*1024*1024:
            st.error("Evidence exceeds 10 MB.")
            st.stop()
        evidence_dir = Path("data") / "evidence"
        evidence_dir.mkdir(parents=True, exist_ok=True)
        safe_name = file_safe_name(f"{emp_id}_{tdate}_{evidence.name}")
        ev_path = str(evidence_dir / safe_name)
        with open(ev_path, "wb") as f:
            f.write(evidence.read())
        ev_mime = evidence.type
        ev_size = evidence.size

    insert_training(
        conn,
        employee_id=emp_id,
        training_date=tdate,
        next_due_date=next_due,
        evidence_path=ev_path,
        evidence_mime=ev_mime,
        evidence_size=ev_size,
        training_title=training_title,
        training_venue=training_venue
    )



    # Email (if we have one)
    subject = "Training Confirmation – Food Concepts"
    body = f"""Dear {name},

Your training dated {tdate} has been recorded.

Training Title: {training_title}
Training Venue: {training_venue}
Next training due date: {next_due}.

Department: {department}
Store: {store}
Position: {position}
Region: {region}

Thank you.
"""
    if email:
        ok, msg = send_confirmation(email, subject, body)
        if ok:
            st.success(f"Training submitted. Confirmation email sent to {email}. Next due: {next_due}")
        else:
            st.warning(f"Training submitted, but email not sent: {msg}")
    else:
        st.success(f"Training submitted. (No email on file.) Next due: {next_due}")
    st.info("If you need to update your details (Store/Department/Email), please contact HR/Admin.")