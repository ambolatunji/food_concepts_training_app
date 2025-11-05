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


def normalize_name(name: str) -> str:
    """
    Normalize person names to Title Case for consistent storage/display.
    Examples:
      - "OLATUNJI" -> "Olatunji"
      - "olatunji dare" -> "Olatunji Dare"
      - "OlAtUnJI" -> "Olatunji"
    """
    if not name:
        return ""

    # First normalize the string (trim, clean)
    n = normalize_str(name)

    # Apply title case
    return n.title()


def normalize_field(field: str) -> str:
    """
    Normalize general text fields (departments, stores, positions) to Title Case.
    Examples:
      - "OPERATIONS" -> "Operations"
      - "lagos store 1" -> "Lagos Store 1"
      - "FOOD SAFETY" -> "Food Safety"
    """
    if not field:
        return ""

    # First normalize the string (trim, clean)
    f = normalize_str(field)

    # Apply title case
    return f.title()


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

# Canonical training title mappings
TRAINING_TITLE_CANONICAL = {
    "food safety training": "Food Safety Training",
    "food handlers training": "Food Safety Training",
    "food handler training": "Food Safety Training",
    "fire safety training": "Fire Safety Training",
    "fire fighting training": "Fire Safety Training",
    "first aid training": "First Aid Training",
    "pest control training": "Pest Control Training",
    "occupational health and safety training": "Occupational Health and Safety Training",
    "occupational health & safety training": "Occupational Health and Safety Training",
    "ohs training": "Occupational Health and Safety Training",
    "6s training": "6S Training",
    "water treatment plant training": "Water Treatment Plant Training",
    "water treatment training": "Water Treatment Plant Training",
}

def canonicalize(value:str, synonyms:dict) -> str:
    if not value: return value
    key = normalize_str(value).lower()
    return synonyms.get(key, value)

def normalize_training_title(title:str) -> str:
    """
    Normalize training titles to canonical forms:
    - Lowercase and trim
    - Remove special characters (& becomes 'and')
    - Map to canonical title if recognized
    """
    if not title:
        return ""

    # Normalize basic string (trim, collapse whitespace)
    t = normalize_str(title)

    # Replace & with 'and'
    t = t.replace("&", "and")

    # Remove extra punctuation but keep spaces
    t = re.sub(r"[^\w\s]", "", t)

    # Collapse multiple spaces
    t = re.sub(r"\s+", " ", t.strip())

    # Try to match to canonical form
    key = t.lower()
    canonical = TRAINING_TITLE_CANONICAL.get(key, None)

    if canonical:
        return canonical

    # If not found in map, return title case version
    return t.title()

def file_safe_name(name:str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", name or "")
