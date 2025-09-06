import smtplib
from email.message import EmailMessage
import ssl
import streamlit as st

def send_confirmation(to_email:str, subject:str, body:str):
    cfg = st.secrets.get("smtp", {})
    host = cfg.get("host")
    port = int(cfg.get("port", 587))
    user = cfg.get("user")
    pwd = cfg.get("password")
    sender = cfg.get("from", user)

    if not (host and port and user and pwd and sender):
        return False, "SMTP settings missing. Configure in .streamlit/secrets.toml"

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = to_email
    msg.set_content(body)

    context = ssl.create_default_context()
    with smtplib.SMTP(host, port) as server:
        server.starttls(context=context)
        server.login(user, pwd)
        server.send_message(msg)
    return True, "sent"
