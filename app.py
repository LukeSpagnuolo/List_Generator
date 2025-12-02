#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
List Generator 5001
Production-ready version using environment variables for deployment
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
from dash.exceptions import PreventUpdate

# -------------------------------------------------------------------------
# ENVIRONMENT VARIABLES (required for secure deployment)
# -------------------------------------------------------------------------

CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")

SITE = os.environ.get("SITE", "https://apps.csipacific.ca")  # Default = production
APP_URL = os.environ.get("APP_URL", "http://127.0.0.1:8050")  # Default for local dev

AUTH_URL = os.environ.get("AUTH_URL", f"{SITE}/o/authorize")
TOKEN_URL = os.environ.get("TOKEN_URL", f"{SITE}/o/token/")
PROFILES_URL = os.environ.get("PROFILES_URL", f"{SITE}/api/registration/profile/")

CITY_MAP_PATH = os.environ.get("CITY_MAP_PATH", "Cities_Extended_Mapped.csv")  # allow override


# -------------------------------------------------------------------------
# Networking / Retry Config
# -------------------------------------------------------------------------
PAGE_LIMIT = 50
MAX_RETRIES = 5
BACKOFF_SEC = 1.5
REQUEST_TIMEOUT = (10, 90)
RETRYABLE_STATUSES = (502, 503, 504, 524)


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
# Role Filter Options (not used yet, but kept for future)
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
    CITY_MAP_STATUS = f"Loaded {len(CITY_TO_CAMPUS)} city→campus mappings."
except Exception as e:
    CITY_TO_CAMPUS = {}
    MAPPED_CAMPUS_NAMES = []
    CITY_MAP_STATUS = f"ERROR loading city map: {e}"


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
        if k in flat:
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


def fetch_paginated(url, headers, log):
    """
    Old full paginator (not used by the chunked fetch, but kept for local/debug).
    """
    rows, page = [], 0
    session = requests.Session()

    while url:
        if "limit=" not in url:
            url += ("&" if "?" in url else "?") + f"limit={PAGE_LIMIT}"

        page += 1
        retries, wait = 0, BACKOFF_SEC

        while True:
            try:
                resp = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)

                if resp.status_code in RETRYABLE_STATUSES and retries < MAX_RETRIES:
                    time.sleep(wait + random.uniform(0, .5))
                    retries += 1
                    wait *= 2
                    continue

                if resp.status_code != 200:
                    return rows

                data = resp.json()
                rows.extend(data.get("results", []))
                url = data.get("next")
                break

            except Exception:
                if retries < MAX_RETRIES:
                    time.sleep(wait)
                    retries += 1
                    continue
                return rows

    return rows


def fetch_paginated_chunk(url, headers, log, max_seconds=20):
    """
    Time-bounded paginator: fetch from `url` for at most `max_seconds`.
    Returns (rows, next_url).
    """
    if not url:
        return [], None

    rows = []
    page = 0
    session = requests.Session()
    start = time.time()

    while url and (time.time() - start) < max_seconds:
        if "limit=" not in url:
            url += ("&" if "?" in url else "?") + f"limit={PAGE_LIMIT}"

        page += 1
        retries, wait = 0, BACKOFF_SEC

        while True:
            try:
                resp = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
                status = resp.status_code

                if status in RETRYABLE_STATUSES and retries < MAX_RETRIES:
                    log.append(
                        f"[chunk p{page}] {url} • {status} → retry {retries+1}/{MAX_RETRIES} in {wait:.1f}s"
                    )
                    time.sleep(wait + random.uniform(0, 0.5))
                    retries += 1
                    wait *= 2
                    continue

                if status != 200:
                    log.append(f"[chunk p{page}] {url} • {status}")
                    return rows, None

                data = resp.json()
                rows.extend(data.get("results", []))
                url = data.get("next")
                log.append(f"[chunk p{page}] ok, next={url}")
                break

            except (ReadTimeout, ConnectTimeout, ConnectionError) as e:
                if retries < MAX_RETRIES:
                    log.append(
                        f"[chunk p{page}] timeout/conn error {e.__class__.__name__} "
                        f"→ retry {retries+1}/{MAX_RETRIES} in {wait:.1f}s"
                    )
                    time.sleep(wait + random.uniform(0, 0.5))
                    retries += 1
                    wait *= 2
                    continue
                else:
                    log.append(
                        f"[chunk p{page}] giving up after {MAX_RETRIES} retries: "
                        f"{type(e).__name__}: {str(e)[:200]}"
                    )
                    return rows, url

            except Exception as e:
                log.append(
                    f"[chunk p{page}] unexpected error: {type(e).__name__}: {str(e)[:200]}"
                )
                return rows, url

    return rows, url


def filter_by_campus(df, campus_val):
    """
    Simple campus filter for preview.
    """
    if df.empty or campus_val in (None, "all"):
        return df
    label = CAMPUS_LABEL_MAP.get(campus_val)
    if not label:
        return df
    return df[df["campus_label"] == label]


# -------------------------------------------------------------------------
# App Setup
# -------------------------------------------------------------------------
auth = DashAuthExternal(AUTH_URL, TOKEN_URL, APP_URL, CLIENT_ID, CLIENT_SECRET)
server = auth.server
app = Dash(__name__, server=server)

cached_df = pd.DataFrame()
cached_name = ""


# -------------------------------------------------------------------------
# UI Layout
# -------------------------------------------------------------------------
app.layout = html.Div(
    [
        html.H1("List Generator 5000"),
        html.H3("Campus filters"),

        dcc.Dropdown(id="campus-dd", options=CAMPUS_OPTS, value="all"),

        html.Button("Fetch ALL (chunked)", id="btn-fetch"),
        html.Div(id="status-msg", style={"marginTop": "0.5rem"}),

        dash_table.DataTable(id="preview", page_size=10),

        html.Hr(),
        html.H3("Filtered CSV preview"),
        dash_table.DataTable(id="filtered-preview", page_size=10),

        html.Button("Download Filtered CSV", id="btn-dl-filter"),
        dcc.Download(id="csv-file-filtered"),

        # Stores + interval for chunked fetching
        dcc.Store(id="df-store"),
        dcc.Store(id="fetch-state", data={"campus_next": {}, "done": True}),
        dcc.Interval(
            id="fetch-interval",
            interval=3_000,  # 3 seconds between chunks
            n_intervals=0,
            disabled=True,
        ),
    ]
)


# -------------------------------------------------------------------------
# Callbacks
# -------------------------------------------------------------------------

# 1) Start chunked Fetch ALL when button is clicked
@app.callback(
    Output("fetch-state", "data"),
    Output("fetch-interval", "disabled"),
    Output("status-msg", "children"),
    Input("btn-fetch", "n_clicks"),
    prevent_initial_call=True,
)
def start_fetch_all(n_clicks):
    global cached_df, cached_name

    if not n_clicks:
        raise PreventUpdate

    token = auth.get_token()
    if not token:
        return {"campus_next": {}, "done": True}, True, "No OAuth token – log in."

    headers = {"Authorization": f"Bearer {token}"}

    # Seed "next" URLs for every campus
    campus_ids = [v for v in CAMPUS_LABEL_MAP.keys()]
    campus_next = {}
    for cid in campus_ids:
        url = f"{PROFILES_URL}?campus_id={cid}"
        campus_next[str(cid)] = url

    # Reset global cache
    cached_df = pd.DataFrame()
    cached_name = "list_generator.csv"

    state = {
        "campus_next": campus_next,  # str(cid) -> url or None
        "done": False,
    }

    msg = f"{CITY_MAP_STATUS} | Starting chunked fetch..."

    return state, False, msg  # enable interval


# 2) Interval-driven chunked fetch loop
@app.callback(
    Output("preview", "data"),
    Output("preview", "columns"),
    Output("fetch-state", "data"),
    Output("fetch-interval", "disabled"),
    Output("status-msg", "children"),
    Input("fetch-interval", "n_intervals"),
    State("fetch-state", "data"),
    State("campus-dd", "value"),
    State("status-msg", "children"),
    prevent_initial_call=True,
)
def continue_fetch_all(n_intervals, state, campus_val, status_text):
    global cached_df

    if not state or state.get("done", False):
        # Nothing to do; stop interval
        raise PreventUpdate

    token = auth.get_token()
    if not token:
        state["done"] = True
        return [], [], state, True, "Auth error – please log in again."

    headers = {"Authorization": f"Bearer {token}"}
    campus_next = state.get("campus_next", {})
    log = []

    # Find the next campus that still has pages
    campus_ids = [v for v in CAMPUS_LABEL_MAP.keys()]
    target_cid = None
    for cid in campus_ids:
        if campus_next.get(str(cid)):
            target_cid = cid
            break

    # If all campuses done
    if target_cid is None:
        state["done"] = True
        msg = f"Fetch ALL complete. Total profiles loaded: {len(cached_df)}"

        df_view = filter_by_campus(cached_df, campus_val)
        cols = [{"name": c, "id": c} for c in df_view.columns]

        return (
            df_view.to_dict("records"),
            cols,
            state,
            True,  # disable interval
            msg,
        )

    # Pull one time-limited chunk for this campus
    cid_str = str(target_cid)
    url_start = campus_next.get(cid_str)

    log.append(f"Chunk {n_intervals}: campus {target_cid}, starting from {url_start}")
    rows, next_url = fetch_paginated_chunk(url_start, headers, log, max_seconds=20)

    # Flatten and append
    if rows:
        flattened = [flatten_profile(r, target_cid) for r in rows]
        new_df = pd.DataFrame(flattened)

        if cached_df.empty:
            cached_df = new_df
        else:
            cached_df = pd.concat([cached_df, new_df], ignore_index=True)

    # Update next URL for this campus
    campus_next[cid_str] = next_url  # None when done
    state["campus_next"] = campus_next

    # Build preview of what we have so far
    df_view = filter_by_campus(cached_df, campus_val)
    cols = [{"name": c, "id": c} for c in df_view.columns]

    msg = f"Fetching… currently loaded {len(cached_df)} profiles."

    return (
        df_view.to_dict("records"),
        cols,
        state,
        False,  # keep interval running until all campuses done
        msg,
    )


# 3) Filtered preview (first 10 rows of full cached_df, independent of campus selection)
@app.callback(
    Output("filtered-preview", "data"),
    Output("filtered-preview", "columns"),
    Input("preview", "data"),
)
def preview_filtered(_):
    if cached_df.empty:
        return [], []
    preview = cached_df.head(10)
    return preview.to_dict("records"), [{"name": c, "id": c} for c in preview.columns]


# 4) Download filtered (currently just the full cached_df)
@app.callback(
    Output("csv-file-filtered", "data"),
    Input("btn-dl-filter", "n_clicks"),
    prevent_initial_call=True,
)
def download_filtered(_):
    if cached_df.empty:
        return no_update
    return dcc.send_data_frame(cached_df.to_csv, cached_name, index=False)


if __name__ == "__main__":
    app.run(debug=True, port=8050)
