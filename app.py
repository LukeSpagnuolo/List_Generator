#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
List Generator 5001
Timed-chunk fetch loop + original OAuth constants
"""

# ------------------------------------------------------------------------
# OAuth + API Configuration (RESTORED CONSTANTS)
# ------------------------------------------------------------------------
CLIENT_ID = "gqncUonQuByTgSZzgCHFSh9tn1F6jPygOnq2PEV5"
CLIENT_SECRET = "MGsn5aY0y2wPoqcK2kc21f7ISm1B2X12PNuMZ8yOiV0IZby5SVSUwPqeS3ypceI7ButFUTXdCcuxZVkVoAIQnm1akFj42VkVEm7PFYSbUTN0d9EWVjxRr6BSbX3ncrbn"

SITE = "https://apps.csipacific.ca"
APP_URL = "http://127.0.0.1:8050"  # for Posit, change this to your Posit URL

AUTH_URL = f"{SITE}/o/authorize"
TOKEN_URL = f"{SITE}/o/token/"
PROFILES_URL = f"{SITE}/api/registration/profile/"

CITY_MAP_PATH = "Cities_Extended_Mapped.csv"  # put this file next to app.py

# -------------------------------------------------------------------------
# Networking / Retry Config
# -------------------------------------------------------------------------
PAGE_LIMIT = 50
MAX_RETRIES = 5
BACKOFF_SEC = 1.5
REQUEST_TIMEOUT = (10, 20)  # shorter read timeout so a single request doesn't hang too long
RETRYABLE_STATUSES = (502, 503, 504, 524)

# Time budget per fetch click (must be < gunicorn worker timeout)
TIME_BUDGET_SECONDS = 25.0

# -------------------------------------------------------------------------
# Campus Configuration
# -------------------------------------------------------------------------
CAMPUS_OPTS = [
    {"label": "CSI Pacific - Victoria", "value": 1},
    {"label": "CSI Pacific - Vancouver", "value": 2},
    {"label": "CSI Pacific - Whistler", "value": 3},
    {"label": "Engage Sport North", "value": 4},
    {"label": "Pacific Sport - Columbia Basin", "value": 5},
    {"label": "Pacific Sport - Fraser Valley", "value": 6},
    {"label": "Pacific Sport - Interior", "value": 7},
    {"label": "Pacific Sport - Okanagan", "value": 8},
    {"label": "Pacific Sport - Vancouver Island", "value": 9},
    {"label": "Other", "value": 10},
    {"label": "Unsure", "value": 11},
    {"label": "Not Applicable", "value": 12},
    {"label": "All Campuses", "value": "all"},
]
CAMPUS_LABEL_MAP = {
    opt["value"]: opt["label"] for opt in CAMPUS_OPTS if isinstance(opt["value"], int)
}

# -------------------------------------------------------------------------
# Role Filter Options (kept for future expansion if needed)
# -------------------------------------------------------------------------
ROLE_OPTS = [
    {"label": "(all roles)", "value": ""},
    {"label": "Athlete", "value": "athlete"},
    {"label": "Coach", "value": "coach"},
    {"label": "Staff", "value": "staff"},
]
ROLE_ID_MAP = {"athlete": 1, "coach": 2, "staff": 4}

# -------------------------------------------------------------------------
# Carding Mapping
# -------------------------------------------------------------------------
CARDING_MAP = {
    1: "SR", 2: "SRI", 3: "SR1", 4: "SR2", 5: "D", 6: "DI", 7: "C1",
    8: "Prov Dev 1", 9: "Prov Dev 2", 10: "Prov Dev 3",
    11: "NSO Affiliated (Uncarded)", 12: "C1I",
    13: "GamePlan Retired", 14: "PSO Affiliated (Uncarded)",
    15: "GamePlan Retired", 16: "PSO Affiliated (Uncarded)"
}

# -------------------------------------------------------------------------
# Imports
# -------------------------------------------------------------------------
import json
import time
import random
import requests
import pandas as pd

from requests.exceptions import ReadTimeout, ConnectTimeout, ConnectionError
from dash_auth_external import DashAuthExternal
from dash import Dash, html, dcc, dash_table, Input, Output, State

# -------------------------------------------------------------------------
# Load City → Campus Mapping
# -------------------------------------------------------------------------
try:
    _city_df = pd.read_csv(CITY_MAP_PATH)
    _city_df = _city_df.dropna(subset=["Location Name", "Location Mapped Centre"])
    CITY_TO_CAMPUS = (
        _city_df.drop_duplicates(subset=["Location Name"])
                .set_index("Location Name")["Location Mapped Centre"]
                .to_dict()
    )
    MAPPED_CAMPUS_NAMES = sorted(set(CITY_TO_CAMPUS.values()))
except Exception:
    CITY_TO_CAMPUS = {}
    MAPPED_CAMPUS_NAMES = []

# -------------------------------------------------------------------------
# Helper functions
# -------------------------------------------------------------------------
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
            new = f"{prefix}{k}" if not prefix else f"{prefix}.{k}"
            out.update(flatten_json(v, new))
    elif isinstance(data, list):
        out[prefix] = "; ".join([safe_str(i) for i in data])
    else:
        out[prefix] = safe_str(data)
    return out


def derive_carding(flat):
    candidate_keys = [
        "carding_level", "carding_level_id", "carding.id",
        "carding_level.pk", "carding_pk", "athlete.carding_level_id",
        "profile.carding_level_id",
    ]

    value = None
    for k in candidate_keys:
        if k in flat and flat[k] not in ("", None, ""):
            try:
                val = int(str(flat[k]).strip())
                value = val
                break
            except Exception:
                pass

    flat["carding_level_mapped"] = CARDING_MAP.get(value, "") if value else ""
    return flat


def flatten_profile(p, campus_id):
    flat = flatten_json(p)
    flat["campus_id"] = str(campus_id)
    flat["campus_label"] = CAMPUS_LABEL_MAP.get(campus_id, "Unknown")

    birth_city = flat.get("birth_city.name", "")
    res_city = flat.get("residence_city.name", "")

    flat["campus_by_birth"] = CITY_TO_CAMPUS.get(birth_city, "")
    flat["current_campus"] = CITY_TO_CAMPUS.get(res_city, "")

    return derive_carding(flat)


def fetch_one_page(url, headers):
    """
    Fetch a SINGLE page from the DRF endpoint with retries.
    Returns (rows, next_url).
    """
    session = requests.Session()
    retries = 0
    wait = BACKOFF_SEC

    while True:
        try:
            resp = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            status = resp.status_code

            if status in RETRYABLE_STATUSES and retries < MAX_RETRIES:
                time.sleep(wait + random.uniform(0, 0.5))
                retries += 1
                wait *= 2
                continue

            if status != 200:
                # Non-retryable error: stop this page
                return [], None

            data = resp.json()
            return data.get("results", []), data.get("next")

        except (ReadTimeout, ConnectTimeout, ConnectionError):
            if retries < MAX_RETRIES:
                time.sleep(wait + random.uniform(0, 0.5))
                retries += 1
                wait *= 2
                continue
            return [], None
        except Exception:
            return [], None


# -------------------------------------------------------------------------
# App Setup
# -------------------------------------------------------------------------
auth = DashAuthExternal(
    AUTH_URL,
    TOKEN_URL,
    app_url=APP_URL,
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET
)

server = auth.server
app = Dash(__name__, server=server)

cached_df = pd.DataFrame()
cached_name = ""

# State for chunked fetch across button clicks
fetch_state = {
    "campus_ids": [],
    "current_campus_index": 0,
    "next_url": None,
    "done": True,
}

# -------------------------------------------------------------------------
# UI Layout (simple)
# -------------------------------------------------------------------------
app.layout = html.Div(
    [
        html.H1("List Generator 5001"),
        html.H3("Campus filters"),

        dcc.Dropdown(
            id="campus-dd",
            options=CAMPUS_OPTS,
            value="all",
            placeholder="Filter preview by campus",
            style={"width": "300px"},
        ),

        html.Button("Fetch (chunked)", id="btn-fetch"),

        html.Div(
            id="status-text",
            style={"marginTop": "0.5rem", "fontSize": "0.9rem", "color": "#555"},
        ),

        dash_table.DataTable(
            id="preview",
            page_size=10,
            style_table={"overflowX": "auto", "marginTop": "0.8rem"},
        ),

        html.Hr(),
        html.H3("Filtered CSV preview (first 10 rows)"),
        dash_table.DataTable(
            id="filtered-preview",
            page_size=10,
            style_table={"overflowX": "auto"},
        ),

        html.Button("Download Filtered CSV", id="btn-dl-filter"),
        dcc.Download(id="csv-file-filtered"),

        dcc.Store(id="dummy-store"),  # just to keep layout happy if expanded later
    ]
)

# -------------------------------------------------------------------------
# Callbacks
# -------------------------------------------------------------------------
@app.callback(
    Output("preview", "data"),
    Output("preview", "columns"),
    Output("status-text", "children"),
    Input("btn-fetch", "n_clicks"),
    State("campus-dd", "value"),
    prevent_initial_call=True,
)
def fetch_profiles(n_clicks, campus_val):
    global cached_df, cached_name, fetch_state

    token = auth.get_token()
    if not token:
        return [], [], "No OAuth token – please log in via the auth screen."

    headers = {"Authorization": f"Bearer {token}"}

    # If this is the first click or a completed run, start a new multi-campus fetch
    if n_clicks == 1 or fetch_state["done"] or not fetch_state["campus_ids"]:
        campus_ids = sorted([v for v in CAMPUS_LABEL_MAP.keys()])
        fetch_state = {
            "campus_ids": campus_ids,
            "current_campus_index": 0,
            "next_url": None,
            "done": False,
        }
        cached_df = pd.DataFrame()
        cached_name = "list_generator.csv"
        status_prefix = "Started new fetch run."
    else:
        status_prefix = "Resuming fetch run."

    start = time.perf_counter()
    total_new_rows = 0

    # Chunked fetch loop: respect TIME_BUDGET_SECONDS
    while (
        not fetch_state["done"]
        and (time.perf_counter() - start) < TIME_BUDGET_SECONDS
    ):
        campus_ids = fetch_state["campus_ids"]
        idx = fetch_state["current_campus_index"]

        if idx >= len(campus_ids):
            fetch_state["done"] = True
            break

        cid = campus_ids[idx]
        url = fetch_state["next_url"] or f"{PROFILES_URL}?campus_id={cid}"

        page_rows, next_url = fetch_one_page(url, headers)

        # If we got no rows and no next_url, consider this campus done and move on
        if not page_rows and not next_url:
            fetch_state["current_campus_index"] += 1
            fetch_state["next_url"] = None
            # If that was the last campus, mark done
            if fetch_state["current_campus_index"] >= len(campus_ids):
                fetch_state["done"] = True
            continue

        # Flatten and append rows
        flattened = [flatten_profile(r, cid) for r in page_rows]
        if flattened:
            df_new = pd.DataFrame(flattened)
            if cached_df.empty:
                cached_df = df_new
            else:
                cached_df = pd.concat([cached_df, df_new], ignore_index=True)
            total_new_rows += len(flattened)

        # Update next_url or move to next campus
        fetch_state["next_url"] = next_url
        if next_url is None:
            fetch_state["current_campus_index"] += 1
            fetch_state["next_url"] = None
            if fetch_state["current_campus_index"] >= len(campus_ids):
                fetch_state["done"] = True

    # Build preview filtered by campus selection
    if cached_df.empty:
        return [], [], f"{status_prefix} No data fetched yet."

    if campus_val == "all":
        df_view = cached_df
    else:
        label = CAMPUS_LABEL_MAP.get(campus_val)
        if label:
            df_view = cached_df[cached_df["campus_label"] == label]
        else:
            df_view = cached_df

    preview_data = df_view.head(10).to_dict("records")
    preview_cols = [{"name": c, "id": c} for c in df_view.columns]

    total_rows = len(cached_df)
    if fetch_state["done"]:
        status = f"{status_prefix} Fetched {total_rows} rows across all campuses. (Run complete.)"
    else:
        status = (
            f"{status_prefix} Added {total_new_rows} rows this pass; "
            f"{total_rows} rows total so far. Click 'Fetch (chunked)' again to continue."
        )

    return preview_data, preview_cols, status


@app.callback(
    Output("filtered-preview", "data"),
    Output("filtered-preview", "columns"),
    Input("preview", "data"),
)
def preview_filtered(_):
    global cached_df
    if cached_df.empty:
        return [], []
    preview = cached_df.head(10)
    return preview.to_dict("records"), [{"name": c, "id": c} for c in preview.columns]


@app.callback(
    Output("csv-file-filtered", "data"),
    Input("btn-dl-filter", "n_clicks"),
    prevent_initial_call=True,
)
def download_filtered(_):
    global cached_df, cached_name
    if cached_df.empty:
        return None
    return dcc.send_data_frame(cached_df.to_csv, cached_name, index=False)


if __name__ == "__main__":
    app.run(debug=True, port=8050)
