#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
List Generator 5000 / 5001
Production-ready version using environment variables for deployment,
with full UI (filters, previews, logs) and time-budgeted fetch.
"""

import os
import json
import time
import random
import math
import requests
import pandas as pd

from requests.exceptions import ReadTimeout, ConnectTimeout, ConnectionError
from dash_auth_external import DashAuthExternal
from dash import Dash, html, dcc, dash_table, Input, Output, State, no_update

# ──────────────────────────────────────────────────────────────────────────────
# ENV VARS (must match Posit secrets)
# ──────────────────────────────────────────────────────────────────────────────

SITE = os.environ.get("SITE", "https://apps.csipacific.ca")
APP_URL = os.environ.get("APP_URL", "http://127.0.0.1:8050")

CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")

AUTH_URL = os.environ.get("AUTH_URL", f"{SITE}/o/authorize")
TOKEN_URL = os.environ.get("TOKEN_URL", f"{SITE}/o/token/")
PROFILES_URL = os.environ.get("PROFILES_URL", f"{SITE}/api/registration/profile/")

CITY_MAP_PATH = os.environ.get("CITY_MAP_PATH", "Cities_Extended_Mapped.csv")

# How long a single fetch click is allowed to run in total (seconds)
MAX_FETCH_SECONDS = float(os.environ.get("MAX_FETCH_SECONDS", "25"))

missing_env = [
    name for name, val in [
        ("CLIENT_ID", CLIENT_ID),
        ("CLIENT_SECRET", CLIENT_SECRET),
    ]
    if not val
]
if missing_env:
    print("⚠️ Missing required environment variables:", ", ".join(missing_env))

# ──────────────────────────────────────────────────────────────────────────────
# Networking & retry tuning
# ──────────────────────────────────────────────────────────────────────────────
PAGE_LIMIT = 50            # per-page size for API
MAX_RETRIES = 5
BACKOFF_SEC = 1.5
# Keep read timeout WELL under gunicorn worker timeout
REQUEST_TIMEOUT = (5, 15)  # (connect, read) seconds
RETRYABLE_STATUSES = (502, 503, 504, 524)

# ──────────────────────────────────────────────────────────────────────────────
# Campus & role options
# ──────────────────────────────────────────────────────────────────────────────
CAMPUS_OPTS = [
    {"label": "CSI Pacific - Victoria",           "value": 1},
    {"label": "CSI Pacific - Vancouver",          "value": 2},
    {"label": "CSI Pacific - Whistler",           "value": 3},
    {"label": "Engage Sport North",               "value": 4},
    {"label": "Pacific Sport - Columbia Basin",   "value": 5},
    {"label": "Pacific Sport - Fraser Valley",    "value": 6},
    {"label": "Pacific Sport - Interior",         "value": 7},
    {"label": "Pacific Sport - Okanagan",         "value": 8},
    {"label": "Pacific Sport - Vancouver Island", "value": 9},
    {"label": "Other",                            "value": 10},
    {"label": "Unsure",                           "value": 11},
    {"label": "Not Applicable",                   "value": 12},
    {"label": "All Campuses",                     "value": "all"},
]
CAMPUS_LABEL_MAP = {
    opt["value"]: opt["label"]
    for opt in CAMPUS_OPTS
    if isinstance(opt["value"], int)
}

ROLE_OPTS = [
    {"label": "(all roles)", "value": ""},
    {"label": "Athlete", "value": "athlete"},
    {"label": "Coach", "value": "coach"},
    {"label": "Staff", "value": "staff"},
]
ROLE_ID_MAP = {"athlete": 1, "coach": 2, "staff": 4}

# ──────────────────────────────────────────────────────────────────────────────
# Carding map
# ──────────────────────────────────────────────────────────────────────────────
CARDING_MAP = {
    1: "SR",
    2: "SRI",
    3: "SR1",
    4: "SR2",
    5: "D",
    6: "DI",
    7: "C1",
    8: "Prov Dev 1",
    9: "Prov Dev 2",
    10: "Prov Dev 3",
    11: "NSO Affiliated (Uncarded)",
    12: "C1I",
    13: "GamePlan Retired",
    14: "PSO Affiliated (Uncarded)",
    15: "GamePlan Retired",
    16: "PSO Affiliated (Uncarded)",
}

# ──────────────────────────────────────────────────────────────────────────────
# Auth & app init
# ──────────────────────────────────────────────────────────────────────────────
auth = DashAuthExternal(
    AUTH_URL,
    TOKEN_URL,
    app_url=APP_URL,
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
)
server = auth.server
app = Dash(__name__, server=server)

# ──────────────────────────────────────────────────────────────────────────────
# City → Campus mapping
# ──────────────────────────────────────────────────────────────────────────────
try:
    _city_df = pd.read_csv(CITY_MAP_PATH)
    _city_df = _city_df.dropna(subset=["Location Name", "Location Mapped Centre"])
    # Normalise keys to avoid case/space issues
    _city_df["key_norm"] = (
        _city_df["Location Name"].astype(str).str.strip().str.lower()
    )
    CITY_TO_CAMPUS = (
        _city_df
        .drop_duplicates(subset=["key_norm"])
        .set_index("key_norm")["Location Mapped Centre"]
        .to_dict()
    )
    MAPPED_CAMPUS_NAMES = sorted(set(CITY_TO_CAMPUS.values()))
    CITY_MAP_STATUS = f"Loaded {len(CITY_TO_CAMPUS)} city→campus mappings."
except Exception as e:
    CITY_TO_CAMPUS = {}
    MAPPED_CAMPUS_NAMES = []
    CITY_MAP_STATUS = f"ERROR loading city map: {e}"
    print("⚠️", CITY_MAP_STATUS)

# ──────────────────────────────────────────────────────────────────────────────
# Flatten helpers
# ──────────────────────────────────────────────────────────────────────────────
def safe_str(v):
    if v is None:
        return ""
    if isinstance(v, (str, int, float, bool)):
        return str(v)
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return str(v)


def flatten_json(data, prefix=""):
    out = {}
    if isinstance(data, dict):
        for k, v in data.items():
            new_key = f"{prefix}{k}" if prefix == "" else f"{prefix}.{k}"
            out.update(flatten_json(v, new_key))
    elif isinstance(data, list):
        out[prefix] = "; ".join([safe_str(i) for i in data])
    else:
        out[prefix] = safe_str(data)
    return out


def _coerce_int(val):
    if val is None:
        return None
    try:
        s = str(val).strip()
        return int(s) if s.isdigit() else None
    except Exception:
        return None


def derive_carding_columns(flat: dict) -> dict:
    candidate_keys = [
        "carding_level", "carding_level_id", "carding.id",
        "carding_level.pk", "carding_pk", "athlete.carding_level_id",
        "profile.carding_level_id",
    ]
    cand_val = None
    for k in candidate_keys:
        if k in flat and flat[k] not in ("", None):
            cand_val = _coerce_int(flat[k])
            if cand_val is not None:
                break

    if cand_val is not None:
        flat["carding_level_id"] = str(cand_val)
        flat["carding_level_mapped"] = CARDING_MAP.get(
            cand_val, f"Unknown ({cand_val})"
        )
    else:
        flat.setdefault("carding_level_id", "")
        flat.setdefault("carding_level_mapped", "")

    return flat


def flatten_profile(p, campus_id):
    flat = flatten_json(p)

    flat["campus_id"] = str(campus_id)
    flat["campus_label"] = CAMPUS_LABEL_MAP.get(
        campus_id, f"Unknown ({campus_id})"
    )

    birth_city = (
        flat.get("birth_city.name")
        or flat.get("person.birth_city.name")
        or flat.get("profile.birth_city.name")
        or ""
    )
    res_city = (
        flat.get("residence_city.name")
        or flat.get("person.residence_city.name")
        or flat.get("profile.residence_city.name")
        or ""
    )

    birth_key = birth_city.strip().lower() if birth_city else ""
    res_key = res_city.strip().lower() if res_city else ""

    flat["campus_by_birth"] = (
        CITY_TO_CAMPUS.get(birth_key, "") if birth_key else ""
    )
    flat["current_campus"] = (
        CITY_TO_CAMPUS.get(res_key, "") if res_key else ""
    )

    flat = derive_carding_columns(flat)
    return flat

# ──────────────────────────────────────────────────────────────────────────────
# Paginated fetch with time budget
# ──────────────────────────────────────────────────────────────────────────────
def fetch_paginated(url, headers, log, deadline=None):
    """
    Fetch paginated DRF endpoint with:
      - retries on 502/503/504/524 and timeouts
      - total time budget via `deadline` (unix timestamp)
    """
    rows, page = [], 0
    session = requests.Session()

    while url:
        if deadline and time.time() >= deadline:
            log.append(f"[page {page}] Stopping fetch: hit time budget.")
            return rows

        if "limit=" not in url:
            url += ("&" if "?" in url else "?") + f"limit={PAGE_LIMIT}"

        page += 1
        retries, wait = 0, BACKOFF_SEC

        while True:
            if deadline and time.time() >= deadline:
                log.append(
                    f"[page {page}] Aborting mid-page: time budget exceeded."
                )
                return rows

            try:
                resp = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
                status = resp.status_code

                if status in RETRYABLE_STATUSES and retries < MAX_RETRIES:
                    log.append(
                        f"[page {page}] {status} → retry {retries+1}/{MAX_RETRIES}"
                    )
                    time.sleep(wait + random.uniform(0, 0.5))
                    retries += 1
                    wait *= 2
                    continue

                if status != 200:
                    log.append(
                        f"[page {page}] non-200 status {status}\n"
                        f"{resp.text[:300]}"
                    )
                    return rows

                data = resp.json()
                rows.extend(data.get("results", []))
                url = data.get("next")
                log.append(f"[page {page}] OK, rows so far: {len(rows)}")
                break

            except (ReadTimeout, ConnectTimeout, ConnectionError) as e:
                if retries < MAX_RETRIES:
                    log.append(
                        f"[page {page}] timeout/conn {type(e).__name__} → "
                        f"retry {retries+1}/{MAX_RETRIES}"
                    )
                    time.sleep(wait + random.uniform(0, 0.5))
                    retries += 1
                    wait *= 2
                    continue
                log.append(
                    f"[page {page}] giving up after {MAX_RETRIES} retries: {e}"
                )
                return rows

            except Exception as e:
                log.append(
                    f"[page {page}] unexpected error: {type(e).__name__}: {e}"
                )
                return rows

    return rows

# ──────────────────────────────────────────────────────────────────────────────
# Global cache
# ──────────────────────────────────────────────────────────────────────────────
cached_df, cached_name = pd.DataFrame(), ""

# ──────────────────────────────────────────────────────────────────────────────
# Export column specs
# ──────────────────────────────────────────────────────────────────────────────
EXPORT_COLUMNS = [
    ("role_slug",                          "role"),
    ("person.first_name",                  "First Name"),
    ("person.last_name",                   "Last Name"),
    ("person.email",                       "Email"),
    ("person.dob",                         "Birth Date"),
    ("person.guardian.first_name",         "Guardian First Name"),
    ("person.guardian.last_name",          "Guardian Last Name"),
    ("person.guardian.relationship",       "Guardian Relationship"),
    ("person.guardian.email",              "Guardian Email"),
    ("sport.name",                         "Sport"),
    ("current_enrollment.admin_status",    "Enrollment Status"),
    ("current_nomination.organization.name","Nomination Organization"),
    ("current_nomination.fiscal_year",     "Nomination Fiscal Year"),
    ("current_nomination.end_date",        "Nomination End Date"),
    ("current_nomination.sport.name",      "Nomination Sport Name"),
    ("current_nomination.redeemed",        "Nomination Claimed"),
    ("current_nomination.carding_level",   "Nomination Carding Level"),
    ("residence_city.name",                "Current Residence"),
    ("birth_city.name",                    "Birth City"),
    ("discipline",                         "Discipline"),
    ("sex_of_competition",                 "Sex of Competition"),
    ("gender",                             "Gender"),
    ("ethnicity",                          "Ethnicity"),
    ("ethnicity_other",                    "Ethnicity Other"),
    ("pronouns",                           "Pronouns"),
    ("pronouns_other",                     "Pronouns Other"),
    ("disability",                         "Disability"),
    ("birth_country",                      "Birth Country"),
    ("residence_country",                  "Residence Country"),
    ("education_attending",                "Attending Education"),
    ("education_level",                    "Education Level"),
    ("education_institution",              "Education Institution"),
    ("education_css",                      "CSS"),
    ("campus_label",                       "Campus Preferred"),
    ("campus_by_birth",                    "Campus by Birth"),
    ("current_campus",                     "Current Campus"),
    ("carding_level_mapped",               "Carding Level Mapped"),
    ("current_nomination.nccp_number",     "Nccp Number"),
    ("current_nomination.coach_role",      "Coach Role"),
    ("current_nomination.coach_level",     "Coach Level"),
    ("major_games",                        "Major Games"),
    ("level_category",                     "Level Category"),
]

FILTER_COLUMNS = [field for field, _ in EXPORT_COLUMNS]
FIELD_TO_LABEL = {field: label for field, label in EXPORT_COLUMNS}
LABEL_TO_FIELD = {label: field for field, label in EXPORT_COLUMNS}

# ──────────────────────────────────────────────────────────────────────────────
# Test sports filter
# ──────────────────────────────────────────────────────────────────────────────
TEST_SPORTS = {
    "Cinderball (TEST)",
    "Skimboarding Cross (TEST)",
}
TEST_SPORTS_NORMALIZED = {s.strip().lower() for s in TEST_SPORTS}

def remove_test_sports(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    cols = [
        c for c in [
            "sport.name",
            "current_nomination.sport.name",
            "Sport",
            "Nomination Sport Name",
        ]
        if c in df.columns
    ]
    if not cols:
        return df

    mask = pd.Series(True, index=df.index)

    for col in cols:
        s = df[col].fillna("").astype(str).str.strip().str.lower()
        explicit = s.isin(TEST_SPORTS_NORMALIZED)
        contains_test = s.str.contains("(test", case=False, regex=False)
        mask &= ~(explicit | contains_test)

    return df[mask]

# ──────────────────────────────────────────────────────────────────────────────
# Carding merge + Level Category
# ──────────────────────────────────────────────────────────────────────────────
def merge_carding_columns(df: pd.DataFrame) -> pd.DataFrame:
    nom_col = "current_nomination.carding_level"
    map_col = "carding_level_mapped"

    if nom_col not in df.columns or map_col not in df.columns:
        return df

    nom = df[nom_col].astype(str).str.strip()
    mapped = df[map_col].astype(str).str.strip()

    nom_clean = nom.replace("nan", "")
    is_missing = nom_clean.eq("")
    has_mapped = mapped.ne("")

    fill_mask = is_missing & has_mapped
    df.loc[fill_mask, nom_col] = mapped[fill_mask]

    return df


def add_level_category(df: pd.DataFrame) -> pd.DataFrame:
    """
    Level Category from nomination card level (after merge_carding_columns):
      - Prov Dev 1/2/3 → Provincial Development
      - NSO Affiliated (Uncarded) → Canadian Development
      - SR, SR1, C, C1, D → Canadian Elite
      - 'PSO Affiliated' (string match) → PSO Affiliated (Non Carded)
    """
    col = "current_nomination.carding_level"
    if col not in df.columns:
        df["level_category"] = ""
        return df

    vals = df[col].astype(str).str.strip()

    def categorize(x: str) -> str:
        if x in {"Prov Dev 1", "Prov Dev 2", "Prov Dev 3"}:
            return "Provincial Development"
        if x == "NSO Affiliated (Uncarded)":
            return "Canadian Development"
        if x in {"SR", "SR1", "C", "C1", "D"}:
            return "Canadian Elite"
        if "PSO Affiliated" in x:
            return "PSO Affiliated (Non Carded)"
        return ""

    df["level_category"] = vals.map(categorize)
    return df

# ──────────────────────────────────────────────────────────────────────────────
# Campus filter
# ──────────────────────────────────────────────────────────────────────────────
def apply_campus_filters(df, campus_val, birth_campus_val, current_campus_val):
    out = df

    if isinstance(campus_val, int) and "campus_label" in out.columns:
        label = CAMPUS_LABEL_MAP.get(campus_val)
        if label:
            out = out[out["campus_label"] == label]

    if birth_campus_val and "campus_by_birth" in out.columns:
        out = out[out["campus_by_birth"] == birth_campus_val]

    if current_campus_val and "current_campus" in out.columns:
        out = out[out["current_campus"] == current_campus_val]

    return out

# ──────────────────────────────────────────────────────────────────────────────
# Layout – same visual structure as your full version
# ──────────────────────────────────────────────────────────────────────────────
app.layout = html.Div(
    style={"fontFamily": "Arial", "margin": "2rem"},
    children=[
        html.H1(
            "List Generator 5000",
            style={
                "marginBottom": "0.1rem",
                "fontSize": "2rem",
                "color": "#003366",
            },
        ),
        html.Div(
            "Campus filter options",
            style={
                "marginBottom": "1rem",
                "fontSize": "0.95rem",
                "color": "#555555",
            },
        ),

        html.Div(
            [
                dcc.Dropdown(
                    id="campus-dd",
                    options=CAMPUS_OPTS,
                    value="all",
                    placeholder="Filter: API campus (campus_label)",
                    style={"width": "260px"},
                ),
                dcc.Dropdown(
                    id="birth-campus-dd",
                    options=[{"label": "(all birth campuses)", "value": ""}]
                    + [
                        {"label": name, "value": name}
                        for name in MAPPED_CAMPUS_NAMES
                    ],
                    value="",
                    placeholder="Filter: campus by birth",
                    style={"width": "260px", "marginLeft": "0.6rem"},
                ),
                dcc.Dropdown(
                    id="current-campus-dd",
                    options=[{"label": "(all current campuses)", "value": ""}]
                    + [
                        {"label": name, "value": name}
                        for name in MAPPED_CAMPUS_NAMES
                    ],
                    value="",
                    placeholder="Filter: current campus",
                    style={"width": "260px", "marginLeft": "0.6rem"},
                ),
                dcc.Dropdown(
                    id="role-dd",
                    options=ROLE_OPTS,
                    value="",
                    placeholder="Filter: role",
                    style={"width": "160px", "marginLeft": "0.6rem"},
                ),
                html.Button(
                    "Fetch",
                    id="btn-fetch",
                    style={
                        "marginLeft": "0.8rem",
                        "padding": "0.45rem 1.2rem",
                        "height": "40px",
                    },
                ),
            ],
            style={
                "display": "flex",
                "alignItems": "center",
                "flexWrap": "wrap",
                "marginBottom": "1rem",
            },
        ),

        dcc.Loading(
            id="loading-spinner",
            type="circle",
            color="#0072B2",
            fullscreen=True,
            children=[
                html.Div(
                    id="loading-message",
                    style={
                        "textAlign": "center",
                        "fontWeight": "bold",
                        "color": "#0072B2",
                        "marginTop": "0.5rem",
                        "fontSize": "1rem",
                    },
                ),
                dash_table.DataTable(
                    id="preview",
                    page_size=10,
                    style_table={
                        "overflowX": "auto",
                        "marginTop": "0.8rem",
                        "width": "100%",
                    },
                    style_header={
                        "backgroundColor": "#0072B2",
                        "color": "white",
                        "fontWeight": "bold",
                    },
                    style_cell={
                        "textAlign": "left",
                        "fontSize": "0.8rem",
                        "padding": "5px",
                    },
                    style_data_conditional=[
                        {
                            "if": {"row_index": "odd"},
                            "backgroundColor": "#f9f9f9",
                        }
                    ],
                ),
            ],
        ),

        html.Hr(style={"marginTop": "1.8rem", "marginBottom": "1rem"}),
        html.H3(
            "Download options",
            style={
                "marginBottom": "0.4rem",
                "fontSize": "1.2rem",
                "color": "#003366",
            },
        ),

        html.Div(
            "Filtered CSV preview (first 10 rows)",
            style={
                "marginBottom": "0.4rem",
                "fontSize": "0.9rem",
                "color": "#555555",
            },
        ),
        dash_table.DataTable(
            id="filtered-preview",
            page_size=10,
            style_table={
                "overflowX": "auto",
                "marginBottom": "0.8rem",
                "width": "100%",
            },
            style_header={
                "backgroundColor": "#444444",
                "color": "white",
                "fontWeight": "bold",
            },
            style_cell={
                "textAlign": "left",
                "fontSize": "0.8rem",
                "padding": "5px",
            },
            style_data_conditional=[
                {
                    "if": {"row_index": "odd"},
                    "backgroundColor": "#f9f9f9",
                }
            ],
        ),

        html.Div(
            [
                html.Button(
                    "Download CSV (full)",
                    id="btn-dl",
                    n_clicks=0,
                    disabled=True,
                    style={
                        "padding": "0.45rem 1.2rem",
                        "marginRight": "0.8rem",
                    },
                ),
                html.Button(
                    "Download Filtered CSV",
                    id="btn-dl-filter",
                    n_clicks=0,
                    disabled=True,
                    style={
                        "padding": "0.45rem 1.2rem",
                        "backgroundColor": "#e0e0e0",
                    },
                ),
            ],
            style={
                "display": "flex",
                "alignItems": "center",
                "flexWrap": "wrap",
                "marginBottom": "0.7rem",
            },
        ),

        html.Div(
            [
                dcc.Dropdown(
                    id="column-select",
                    options=[
                        {"label": label, "value": field}
                        for field, label in EXPORT_COLUMNS
                    ],
                    value=[field for field, _ in EXPORT_COLUMNS],
                    multi=True,
                    placeholder="Select columns for filtered CSV",
                    style={"width": "100%"},
                )
            ],
            style={"marginBottom": "0.6rem"},
        ),

        html.Div(
            [
                dcc.Dropdown(
                    id="enrollment-status-dd",
                    options=[],
                    value=[],
                    multi=True,
                    placeholder="Filter by enrollment status for filtered CSV (optional)",
                    style={"width": "100%"},
                )
            ],
            style={"marginBottom": "0.6rem"},
        ),

        html.Div(
            [
                dcc.Dropdown(
                    id="nomination-claimed-dd",
                    options=[],
                    value=[],
                    multi=True,
                    placeholder="Filter by nomination claimed for filtered CSV (optional)",
                    style={"width": "100%"},
                )
            ],
            style={"marginBottom": "0.8rem"},
        ),

        html.Details(
            [
                html.Summary(
                    "Technical request log (advanced)",
                    style={
                        "cursor": "pointer",
                        "fontSize": "0.9rem",
                        "color": "#555",
                        "fontWeight": "bold",
                    },
                ),
                html.Pre(
                    id="log",
                    style={
                        "whiteSpace": "pre-wrap",
                        "background": "#f7f7f7",
                        "height": "25vh",
                        "overflow": "auto",
                        "padding": "0.7rem",
                        "fontSize": "0.8rem",
                        "border": "1px solid #ddd",
                        "marginTop": "0.8rem",
                    },
                ),
            ],
            open=False,
            style={"marginTop": "1.0rem"},
        ),

        dcc.Download(id="csv-file"),
        dcc.Download(id="csv-file-filtered"),
    ],
)

# ──────────────────────────────────────────────────────────────────────────────
# Callbacks
# ──────────────────────────────────────────────────────────────────────────────
@app.callback(
    Output("preview", "data"),
    Output("preview", "columns"),
    Output("btn-dl", "disabled"),
    Output("btn-dl-filter", "disabled"),
    Output("log", "children"),
    Output("loading-message", "children"),
    Output("enrollment-status-dd", "options"),
    Output("enrollment-status-dd", "value"),
    Output("nomination-claimed-dd", "options"),
    Output("nomination-claimed-dd", "value"),
    Input("btn-fetch", "n_clicks"),
    State("campus-dd", "value"),
    State("birth-campus-dd", "value"),
    State("current-campus-dd", "value"),
    State("role-dd", "value"),
    prevent_initial_call=True,
)
def fetch_profiles(_, campus_val, birth_campus_val, current_campus_val, role_val):
    token = auth.get_token()
    if not token:
        return (
            no_update,
            no_update,
            True,
            True,
            "No OAuth token – log in.",
            "",
            [],
            [],
            [],
            [],
        )

    headers = {"Authorization": f"Bearer {token}"}

    campus_ids = [o["value"] for o in CAMPUS_OPTS if isinstance(o["value"], int)]

    rows_total, flattened, log_lines = [], [], []

    log_lines.append(CITY_MAP_STATUS)
    start = time.time()
    deadline = start + MAX_FETCH_SECONDS

    for cid in campus_ids:
        if time.time() >= deadline:
            log_lines.append(
                f"Global time budget hit after campus {cid-1}, "
                f"stopping further campuses."
            )
            break

        role_id = ROLE_ID_MAP.get(role_val, "")
        url = f"{PROFILES_URL}?campus_id={cid}"
        if role_id:
            url += f"&role_id={role_id}"

        log_lines.append(f"\nCampus {cid}, role='{role_val or 'all'}'")
        batch = fetch_paginated(url, headers, log_lines, deadline=deadline)
        for r in batch:
            flattened.append(flatten_profile(r, cid))
            rows_total.append(r)
        log_lines.append(f"  added {len(batch)} rows (running total {len(rows_total)})")

    if not flattened:
        return (
            [],
            [],
            True,
            True,
            "\n".join(log_lines),
            "No profiles found (or time budget hit before any page completed).",
            [],
            [],
            [],
            [],
        )

    df = pd.DataFrame(flattened)

    # Merge carding & add level category ONCE globally
    df = merge_carding_columns(df)
    df = add_level_category(df)

    global cached_df, cached_name
    cached_df = df.copy()
    campus_tag = "all"
    cached_name = f"profiles_{campus_tag}{'_'+role_val if role_val else ''}.csv"

    df_view = apply_campus_filters(df, campus_val, birth_campus_val, current_campus_val)

    columns = [{"name": c, "id": c} for c in df_view.columns]
    loading_msg = (
        f"Fetched {len(df_view)} profiles (from {len(df)} total) "
        f"in {time.time() - start:.1f}s."
    )

    enrollment_options = []
    if "current_enrollment.admin_status" in df.columns:
        vals = (
            df["current_enrollment.admin_status"]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )
        vals = sorted(vals)
        enrollment_options = [{"label": v, "value": v} for v in vals]

    nomination_options = []
    if "current_nomination.redeemed" in df.columns:
        nvals = (
            df["current_nomination.redeemed"]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )
        nvals = sorted(nvals)
        nomination_options = [{"label": v, "value": v} for v in nvals]

    return (
        df_view.to_dict("records"),
        columns,
        False,
        False,
        "\n".join(log_lines),
        loading_msg,
        enrollment_options,
        [],
        nomination_options,
        [],
    )

@app.callback(
    Output("csv-file", "data"),
    Input("btn-dl", "n_clicks"),
    State("campus-dd", "value"),
    State("birth-campus-dd", "value"),
    State("current-campus-dd", "value"),
    prevent_initial_call=True,
)
def download_csv(_, campus_val, birth_campus_val, current_campus_val):
    if cached_df.empty:
        return no_update
    df_out = apply_campus_filters(
        cached_df, campus_val, birth_campus_val, current_campus_val
    )
    return dcc.send_data_frame(df_out.to_csv, cached_name, index=False)

@app.callback(
    Output("csv-file-filtered", "data"),
    Input("btn-dl-filter", "n_clicks"),
    State("campus-dd", "value"),
    State("birth-campus-dd", "value"),
    State("current-campus-dd", "value"),
    State("column-select", "value"),
    State("enrollment-status-dd", "value"),
    State("nomination-claimed-dd", "value"),
    prevent_initial_call=True,
)
def download_filtered_csv(
    _, campus_val, birth_campus_val, current_campus_val,
    selected_fields, enrollment_status_vals, nomination_claimed_vals
):
    if cached_df.empty:
        return no_update

    df_out = apply_campus_filters(
        cached_df, campus_val, birth_campus_val, current_campus_val
    )

    if enrollment_status_vals and "current_enrollment.admin_status" in df_out.columns:
        df_out = df_out[
            df_out["current_enrollment.admin_status"].astype(str).isin(enrollment_status_vals)
        ]

    if nomination_claimed_vals and "current_nomination.redeemed" in df_out.columns:
        df_out = df_out[
            df_out["current_nomination.redeemed"].astype(str).isin(nomination_claimed_vals)
        ]

    df_out = remove_test_sports(df_out)
    df_out = merge_carding_columns(df_out)
    df_out = add_level_category(df_out)

    if not selected_fields:
        selected_fields = FILTER_COLUMNS

    fields = [f for f in selected_fields if f in df_out.columns]
    if not fields:
        return no_update

    df_filtered = df_out[fields].copy()
    rename_map = {field: FIELD_TO_LABEL.get(field, field) for field in fields}
    df_filtered.rename(columns=rename_map, inplace=True)

    filename = cached_name.replace(".csv", "_filtered.csv")
    return dcc.send_data_frame(df_filtered.to_csv, filename, index=False)

@app.callback(
    Output("filtered-preview", "data"),
    Output("filtered-preview", "columns"),
    Input("campus-dd", "value"),
    Input("birth-campus-dd", "value"),
    Input("current-campus-dd", "value"),
    Input("column-select", "value"),
    Input("enrollment-status-dd", "value"),
    Input("nomination-claimed-dd", "value"),
)
def update_filtered_preview(
    campus_val,
    birth_campus_val,
    current_campus_val,
    selected_fields,
    enrollment_status_vals,
    nomination_claimed_vals,
):
    if cached_df.empty:
        return [], []

    df_out = apply_campus_filters(
        cached_df, campus_val, birth_campus_val, current_campus_val
    )

    if enrollment_status_vals and "current_enrollment.admin_status" in df_out.columns:
        df_out = df_out[
            df_out["current_enrollment.admin_status"].astype(str).isin(enrollment_status_vals)
        ]

    if nomination_claimed_vals and "current_nomination.redeemed" in df_out.columns:
        df_out = df_out[
            df_out["current_nomination.redeemed"].astype(str).isin(nomination_claimed_vals)
        ]

    df_out = remove_test_sports(df_out)
    df_out = merge_carding_columns(df_out)
    df_out = add_level_category(df_out)

    if df_out.empty:
        return [], []

    if not selected_fields:
        selected_fields = FILTER_COLUMNS

    fields = [f for f in selected_fields if f in df_out.columns]
    if not fields:
        return [], []

    df_filtered = df_out[fields].copy()
    rename_map = {field: FIELD_TO_LABEL.get(field, field) for field in fields}
    df_filtered.rename(columns=rename_map, inplace=True)

    df_preview = df_filtered.head(10)
    columns = [{"name": c, "id": c} for c in df_preview.columns]
    data = df_preview.to_dict("records")

    return data, columns

# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(debug=True, port=8050)
