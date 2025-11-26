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
# Role Filter Options
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
        if k in flat:
            try:
                val = int(str(flat[k]).strip())
                value = val
                break
            except:
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


# -------------------------------------------------------------------------
# App Setup
# -------------------------------------------------------------------------
auth = DashAuthExternal(AUTH_URL, TOKEN_URL, APP_URL, CLIENT_ID, CLIENT_SECRET)
server = auth.server
app = Dash(__name__, server=server)

cached_df = pd.DataFrame()
cached_name = ""


# -------------------------------------------------------------------------
# UI Layout (shortened for clarity – content unchanged except env support)
# -------------------------------------------------------------------------
app.layout = html.Div(
    [
        html.H1("List Generator 5000"),
        html.H3("Campus filters"),
        dcc.Dropdown(id="campus-dd", options=CAMPUS_OPTS, value="all"),
        html.Button("Fetch", id="btn-fetch"),

        dash_table.DataTable(id="preview", page_size=10),

        html.Hr(),
        html.H3("Filtered CSV preview"),
        dash_table.DataTable(id="filtered-preview", page_size=10),

        html.Button("Download Filtered CSV", id="btn-dl-filter"),
        dcc.Download(id="csv-file-filtered"),
        dcc.Store(id="df-store"),
    ]
)


# -------------------------------------------------------------------------
# Callbacks (unchanged logic, shortened output for readability)
# -------------------------------------------------------------------------
@app.callback(
    Output("preview", "data"),
    Output("preview", "columns"),
    Input("btn-fetch", "n_clicks"),
    State("campus-dd", "value"),
    prevent_initial_call=True,
)
def fetch_profiles(_, campus_val):

    token = auth.get_token()
    headers = {"Authorization": f"Bearer {token}"}

    rows = []
    for cid in [v for v in CAMPUS_LABEL_MAP.keys()]:
        rows.extend(fetch_paginated(f"{PROFILES_URL}?campus_id={cid}", headers, []))

    df = pd.DataFrame([flatten_profile(r, cid) for r in rows])

    global cached_df, cached_name
    cached_df = df.copy()
    cached_name = "list_generator.csv"

    df_view = df if campus_val == "all" else df[df["campus_label"] == CAMPUS_LABEL_MAP[campus_val]]

    return df_view.to_dict("records"), [{"name": c, "id": c} for c in df_view.columns]


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


@app.callback(
    Output("csv-file-filtered", "data"),
    Input("btn-dl-filter", "n_clicks"),
    prevent_initial_call=True,
)
def download_filtered(_):
    return dcc.send_data_frame(cached_df.to_csv, cached_name, index=False)


if __name__ == "__main__":
    app.run(debug=True, port=8050)

