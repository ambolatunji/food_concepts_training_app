from datetime import datetime, timedelta
import re
from datetime import datetime, date
from dateutil import parser


def normalize_str(x) -> str:
    # Treat None/NaN as empty
    if x is None:
        return ""
    try:
        import pandas as pd
        if pd.isna(x):
            return ""
    except Exception:
        pass

    # Coerce everything to string
    s = str(x)

    # Clean non-breaking spaces and collapse whitespace
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s.strip())
    return s


def unique_key(name, email, store, department):
    name = normalize_str(name).lower()
    email = normalize_str(email).lower()
    store = normalize_str(store).lower()
    department = normalize_str(department).lower()
    return f"{name}|{email}|{store}|{department}" if email else f"{name}|{store}|{department}"

def to_date_str(dt):
    # None / blank
    if dt is None:
        return None

    # Already a datetime/date
    if isinstance(dt, datetime):
        return dt.date().isoformat()
    if isinstance(dt, date):
        return dt.isoformat()

    # Try pandas-aware paths (covers Excel serials, strings with time)
    try:
        import pandas as pd
        # Excel serial numbers (float/int): origin 1899-12-30 works for Excel
        if isinstance(dt, (int, float)):
            ts = pd.to_datetime(dt, unit='d', origin='1899-12-30', errors='coerce')
            if pd.notna(ts):
                return ts.date().isoformat()
        # Strings or other types: use pandas then dateutil as fallback
        s = str(dt).strip()
        if s:
            ts = pd.to_datetime(s, errors='coerce', dayfirst=False)
            if pd.notna(ts):
                return ts.date().isoformat()
            # Fallback to dateutil (US then dayfirst)
            try:
                d = parser.parse(s, dayfirst=False); return d.date().isoformat()
            except Exception:
                d = parser.parse(s, dayfirst=True);  return d.date().isoformat()
    except Exception:
        try:
            d = parser.parse(str(dt).strip()); return d.date().isoformat()
        except Exception:
            return None
    return None

def compute_next_due(training_date_str:str) -> str:
    # add 365 days, then roll forward to Monday if weekend
    dt = datetime.fromisoformat(training_date_str)
    nd = dt + timedelta(days=365)
    while nd.weekday() >= 5:  # 5=Sat,6=Sun
        nd += timedelta(days=1)
    return nd.date().isoformat()

# synonym maps for basic normalization (extend as needed)
REGION_SYNONYMS = {
    "edo-delta": "Edo-Delta",
    "edo/delta": "Edo-Delta",
    "lagos mainland 1": "Lagos Mainland 1",
    "lagos mainland": "Lagos mainland",
    "south west": "South west",
    "south-south": "South south",
    "south-east": "South-East",
}

def canonicalize(value:str, synonyms:dict) -> str:
    if not value: return value
    key = normalize_str(value).lower()
    return synonyms.get(key, value)

def file_safe_name(name:str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", name or "")
