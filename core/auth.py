import streamlit as st
import streamlit_authenticator as stauth
from datetime import datetime

# Uses st.secrets["auth"] mapping in secrets.toml
def build_authenticator():
    users = st.secrets.get("auth", {}).get("users", [])
    creds = {"usernames": {}}
    for u in users:
        creds["usernames"][u["username"]] = {
            "email": u.get("email", ""),
            "name": u.get("name", u["username"]),
            "password": u["password_hash"]
        }
    cookie_name = st.secrets.get("auth", {}).get("cookie_name", "fc_training_cookie")
    cookie_key = st.secrets.get("auth", {}).get("cookie_key", "supersecret")
    authenticator = stauth.Authenticate(
        credentials=creds,
        cookie_name=cookie_name,
        key=cookie_key,
        cookie_expiry_days=1
    )
    return authenticator
