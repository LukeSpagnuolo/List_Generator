#!/usr/bin/env python3
"""List Generator Lite.

Uses Registration API v2 (graph authorization); see REGISTRATION_API_BASE to
fall back to the unversioned v1 API.

Fetch authenticated registration profiles, preview a deliberately limited set of
fields, filter the results, and export either a curated or complete CSV.
"""

from __future__ import annotations

import base64
import json
import os
import random
import re
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import pandas as pd
import requests
from dash import ALL, Dash, Input, Output, State, callback_context, dash_table, dcc, html, no_update
from dash_auth_external import DashAuthExternal
import dash_auth_external.auth as _dash_auth_external_auth
from dotenv import load_dotenv
from requests.exceptions import ConnectionError, ConnectTimeout, ReadTimeout
from prebuilt_reports_7000 import (
    REPORT_GUIDE,
    REPORT_OPTIONS,
    available_fiscal_years,
    build_report,
    default_sports,
    previous_fiscal_year,
    sport_values,
)
from national_inventory_workflow import (
    OUTPUT_COLUMNS as NATIONAL_INVENTORY_COLUMNS,
    SERVICES as NATIONAL_INVENTORY_SERVICES,
    build_service_report,
    lou_sports_in,
    registration_cohort,
    uploaded_cohort,
)


APP_DIR = Path(__file__).resolve().parent
# Override stale values retained by Spyder when the script is run repeatedly.
load_dotenv(APP_DIR / ".env", override=True)

CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
SITE = os.environ.get("SITE", "https://apps.csipacific.ca")
APP_URL = os.environ.get("APP_URL", "http://127.0.0.1:8060")
AUTH_URL = os.environ.get("AUTH_URL", f"{SITE}/o/authorize")
TOKEN_URL = os.environ.get("TOKEN_URL", f"{SITE}/o/token/")
# Registration API v2 (graph authorization). Set REGISTRATION_API_BASE to
# {SITE}/api/registration/ in .env to fall back to the unversioned v1 API.
REGISTRATION_API_BASE = (
    os.environ.get("REGISTRATION_API_BASE", f"{SITE}/api/registration/v2/").rstrip("/") + "/"
)
LEGACY_REGISTRATION_API_BASE = f"{SITE}/api/registration/"
PROFILES_URL = os.environ.get("PROFILES_URL", f"{REGISTRATION_API_BASE}report-rows/")
REPORT_COLUMNS_URL = os.environ.get("REPORT_COLUMNS_URL", f"{REGISTRATION_API_BASE}report-columns/")
ACTOR_URL = os.environ.get("ACTOR_URL", f"{SITE}/api/csiauth/v2/me/")
SCHEMA_URL = os.environ.get("SCHEMA_URL", f"{REGISTRATION_API_BASE}schema/")
API_VERSION = "v2" if "/v2/" in PROFILES_URL else "v1"

CITY_MAP_PATH = Path(os.environ.get("CITY_MAP_PATH", APP_DIR / "Cities_Extended_Mapped.csv"))
CACHE_DIR = Path(
    os.environ.get(
        "LIST_GENERATOR_CACHE_DIR",
        Path(tempfile.gettempdir()) / "list_generator_lite_cache",
    )
)
CACHE_TTL_SECONDS = int(os.environ.get("LIST_GENERATOR_CACHE_TTL_SECONDS", "7200"))

PAGE_LIMIT = 100
MAX_RETRIES = 5
BACKOFF_SECONDS = 1.5
REQUEST_TIMEOUT = (10, 90)
RETRYABLE_STATUSES = {429, 502, 503, 504, 524}
# Posit Connect (and its proxy) drops a request that runs too long, so "Fetch" runs as a
# chain of short callbacks: each one fetches pages until FETCH_STEP_SECONDS have passed and
# keeps the progress in a job file in CACHE_DIR, then the browser asks for the next step.
FETCH_STEP_SECONDS = float(os.environ.get("LIST_GENERATOR_FETCH_STEP_SECONDS", "15"))
STEP_REQUEST_TIMEOUT = (10, float(os.environ.get("LIST_GENERATOR_PAGE_TIMEOUT_SECONDS", "40")))

CAMPUS_OPTIONS = [
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
    {"label": "All campuses", "value": "all"},
]
CAMPUS_LABELS = {
    option["value"]: option["label"]
    for option in CAMPUS_OPTIONS
    if isinstance(option["value"], int)
}
ROLE_OPTIONS = [
    {"label": "All roles", "value": ""},
    {"label": "Athlete", "value": "athlete"},
    {"label": "Coach", "value": "coach"},
    {"label": "Staff", "value": "staff"},
]
ROLE_IDS = {"athlete": 1, "coach": 2, "staff": 4}
FETCH_SOURCE_OPTIONS = [
    {"label": "Profiles + Nominations", "value": "all"},
    {"label": "Profiles", "value": "profile"},
    {"label": "Nominations", "value": "nomination"},
]
FETCH_SOURCE_NOUNS = {
    "profile": "profiles",
    "nomination": "nominations",
    "all": "profiles and nominations",
}

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

EXPORT_COLUMNS = [
    ("role", "Role", False),
    ("first_name", "First Name", False),
    ("last_name", "Last Name", False),
    ("email", "Email", False),
    ("sport", "Sport", False),
    ("enrollment_status", "Enrollment Status", False),
    ("nomination_organization", "Nomination Organization", False),
    ("nomination_fiscal_year", "Nomination Fiscal Year", False),
    ("nomination_end_date", "Nomination End Date", False),
    ("nomination_sport_name", "Nomination Sport", False),
    ("nomination_claimed", "Nomination Claimed", False),
    ("nomination_approved", "Nomination Approved", False),
    ("nomination_status", "Nomination Status", False),
    ("athlete_carding", "Carding Level", False),
    ("level_category", "Level Category", False),
    ("birth_city_campus", "Campus by Birth", False),
    ("residence_city_campus", "Current Campus", False),
    ("current_residence", "Current Residence", False),
    ("birth_city", "Birth City", False),
    ("attending_education", "Attending Education", False),
    ("nccp_number", "NCCP Number", False),
    ("coach_role", "Coach Role", False),
    ("coach_level", "Coach Level", False),
    ("dob", "Birth Date — sensitive", True),
    ("age", "Age — sensitive", True),
    ("guardian_first_name", "Guardian First Name — sensitive", True),
    ("guardian_last_name", "Guardian Last Name — sensitive", True),
    ("guardian_relationship", "Guardian Relationship — sensitive", True),
    ("guardian_email", "Guardian Email — sensitive", True),
    ("athlete_ethnicity", "Ethnicity — sensitive", True),
    ("role_slug", "Role", False),
    ("person.first_name", "First Name", False),
    ("person.last_name", "Last Name", False),
    ("person.email", "Email", False),
    ("sport.name", "Sport", False),
    ("current_enrollment.admin_status", "Enrollment Status", False),
    ("current_nomination.organization.name", "Nomination Organization", False),
    ("current_nomination.fiscal_year", "Nomination Fiscal Year", False),
    ("current_nomination.end_date", "Nomination End Date", False),
    ("current_nomination.sport.name", "Nomination Sport", False),
    ("current_nomination.redeemed", "Nomination Claimed", False),
    ("carding_level_mapped", "Carding Level", False),
    ("discipline", "Discipline", False),
    ("campus_label", "Preferred Campus", False),
    ("campus_by_birth", "Campus by Birth", False),
    ("current_campus", "Current Campus", False),
    ("residence_city.name", "Current Residence", False),
    ("birth_city.name", "Birth City", False),
    ("education_attending", "Attending Education", False),
    ("education_level", "Education Level", False),
    ("education_institution", "Education Institution", False),
    ("current_nomination.nccp_number", "NCCP Number", False),
    ("current_nomination.coach_role", "Coach Role", False),
    ("current_nomination.coach_level", "Coach Level", False),
    ("major_games", "Major Games", False),
    ("person.dob", "Birth Date — sensitive", True),
    ("person.guardian.first_name", "Guardian First Name — sensitive", True),
    ("person.guardian.last_name", "Guardian Last Name — sensitive", True),
    ("person.guardian.relationship", "Guardian Relationship — sensitive", True),
    ("person.guardian.email", "Guardian Email — sensitive", True),
    ("sex_of_competition", "Sex of Competition — sensitive", True),
    ("gender", "Gender — sensitive", True),
    ("ethnicity", "Ethnicity — sensitive", True),
    ("ethnicity_other", "Ethnicity Other — sensitive", True),
    ("pronouns", "Pronouns — sensitive", True),
    ("pronouns_other", "Pronouns Other — sensitive", True),
    ("disability", "Disability — sensitive", True),
    ("birth_country", "Birth Country — sensitive", True),
]
RAW_EXPORT_COLUMNS = EXPORT_COLUMNS
EXPORT_FIELD_ALIASES: dict[str, list[str]] = {}
_unique_export_columns: list[tuple[str, str, bool]] = []
_canonical_by_label: dict[str, str] = {}
for _field, _label, _sensitive in RAW_EXPORT_COLUMNS:
    _canonical = _canonical_by_label.get(_label)
    if _canonical is None:
        _canonical = _field
        _canonical_by_label[_label] = _canonical
        _unique_export_columns.append((_canonical, _label, _sensitive))
        EXPORT_FIELD_ALIASES[_canonical] = []
    EXPORT_FIELD_ALIASES[_canonical].append(_field)
EXPORT_COLUMNS = _unique_export_columns
FIELD_LABELS = {field: label for field, label, _ in EXPORT_COLUMNS}
DEFAULT_FIELDS = [field for field, _, sensitive in EXPORT_COLUMNS if not sensitive]
PREVIEW_FIELDS = [
    "first_name",
    "last_name",
    "email",
    "role",
    "sport",
    "enrollment_status",
    "residence_city_campus",
    "athlete_carding",
]

# Extra multi-select filters on the Custom export (step 3), as (canonical field, label).
# Options come from the fetched data; each filter also matches the field's API aliases.
EXPORT_LIST_FILTERS = [
    ("role", "Role"),
    ("sport", "Sport"),
    ("nomination_fiscal_year", "Nomination fiscal year"),
    ("athlete_carding", "Carding level"),
    ("nomination_status", "Nomination status"),
    ("nomination_organization", "Nomination organization"),
]
EXPORT_LIST_FILTER_LABELS = dict(EXPORT_LIST_FILTERS)


def export_filter_id(field: str) -> dict[str, str]:
    return {"type": "export-filter", "field": field}


EXPORT_FILTER_ALL = {"type": "export-filter", "field": ALL}

REPORT_DESCRIPTIONS = {
    "pending_unmatched": ("Finds pending profiles that may match unlinked nominations.", "CSV", "Pending profiles; compatible name and sport matching"),
    "duplicate_profiles": ("Finds people entered under the same name and sport with more than one email address.", "CSV", "Exact name and sport match; ignores fiscal year"),
    "guardian_email_match": ("Finds profiles where the guardian email is the same as the athlete's own email.", "CSV", "Non-blank, case-insensitive email match; ignores fiscal year"),
    "unclaimed_nominations": ("Lists approved nominations that have not been claimed.", "CSV", "Approved and unclaimed; one row per person"),
    "mail_merge": ("Creates recipient-ready contact data for unclaimed nominations.", "CSV or ZIP", "Guardian for athletes under 19; otherwise athlete"),
    "active_athletes": ("Exports claimed athlete nominations with level category.", "CSV", "Claimed nominations; internal identifiers removed"),
    "viasport_lou": ("Produces the standard ViaSport LOU list.", "CSV", "Selected fiscal year and LOU sports"),
    "enhanced_excellence": ("Produces the standard Enhanced Excellence list.", "CSV", "Selected fiscal year and Enhanced Excellence sports"),
    "sc_carded": ("Produces ViaSport’s SC Carded List (2A).", "CSV", "Approved Canadian Elite athletes"),
    "high_performance": ("Produces ViaSport’s High Performance Athletes report (2B).", "CSV", "Approved athletes in the selected fiscal year"),
    "coaches": ("Produces ViaSport’s Coaches report (4B).", "CSV", "Approved coaches in the selected fiscal year"),
    "conversion": ("Summarizes high-performance athletes by sport.", "CSV", "Counts and average age by sport"),
    "viasport_all": ("Packages the three principal ViaSport reports together.", "ZIP of CSV files", "SC Carded, High Performance Athletes, and Coaches"),
}
REPORT_OUTPUT_COLUMNS = {
    "pending_unmatched": "Role, First Name, Last Name, Guardian Email, Sport, Enrollment Status, Email",
    "duplicate_profiles": "Role, First Name, Last Name, Sport, Email, Enrollment Status, Nomination Claimed",
    "guardian_email_match": "Role, First Name, Last Name, Email, Guardian First Name, Guardian Last Name, Guardian Email, Age, Sport, Enrollment Status",
    "unclaimed_nominations": "Nomination contact fields in the standard API order",
    "mail_merge": "First Name, Last Name, To",
    "active_athletes": "Standard ordered fields plus Level Category; internal IDs removed",
    "viasport_lou": "Standard ordered ViaSport export fields",
    "enhanced_excellence": "Standard ordered Enhanced Excellence export fields",
    "sc_carded": "Sport, Name, Gender, Card level, Province, Nomination Claimed",
    "high_performance": "Nomination ID, Sport, Card level, competition, demographics, campus, and claim status",
    "coaches": "Nomination ID, Sport, Gender, campus, Ethnicity, claim status, NCCP Number",
    "conversion": "Sport, Total Athletes, Average Age, Sex of Competition (F/M/X)",
    "viasport_all": "The fixed columns from reports 2A, 2B, and 4B",
}

TEST_SPORTS = {"cinderball (test)", "skimboarding cross (test)"}



def load_city_mapping(path: Path) -> tuple[dict[str, str], list[str], str | None]:
    try:
        city_df = pd.read_csv(path).dropna(
            subset=["Location Name", "Location Mapped Centre"]
        )
        mapping = (
            city_df.drop_duplicates(subset=["Location Name"])
            .set_index("Location Name")["Location Mapped Centre"]
            .to_dict()
        )
        return mapping, sorted(set(mapping.values())), None
    except Exception as exc:
        return {}, [], f"City mapping unavailable: {type(exc).__name__}: {exc}"


CITY_TO_CAMPUS, MAPPED_CAMPUS_NAMES, CITY_MAPPING_WARNING = load_city_mapping(
    CITY_MAP_PATH
)


def safe_string(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    except (TypeError, ValueError):
        return str(value)


def flatten_json(data: Any, prefix: str = "") -> dict[str, str]:
    flattened: dict[str, str] = {}
    if isinstance(data, dict):
        for key, value in data.items():
            child_key = f"{prefix}.{key}" if prefix else str(key)
            flattened.update(flatten_json(value, child_key))
    elif isinstance(data, list):
        flattened[prefix] = "; ".join(safe_string(item) for item in data)
    else:
        flattened[prefix] = safe_string(data)
    return flattened


def first_present(record: dict[str, str], *keys: str) -> str:
    for key in keys:
        if record.get(key):
            return record[key]
    return ""


def derive_carding(record: dict[str, str]) -> dict[str, str]:
    keys = (
        "carding_level",
        "carding_level_id",
        "carding.id",
        "carding_level.pk",
        "carding_pk",
        "athlete.carding_level_id",
        "profile.carding_level_id",
        "current_nomination.carding_level",
    )
    carding_id = None
    for key in keys:
        try:
            carding_id = int(str(record.get(key, "")).strip())
            break
        except (TypeError, ValueError):
            continue
    record["carding_level_id"] = str(carding_id) if carding_id is not None else ""
    record["carding_level_mapped"] = (
        CARDING_MAP.get(carding_id, f"Unknown ({carding_id})")
        if carding_id is not None
        else ""
    )
    return record


def flatten_profile(profile: dict[str, Any], campus_id: int) -> dict[str, str]:
    record = flatten_json(profile)
    record["campus_id"] = str(campus_id)
    record["campus_label"] = CAMPUS_LABELS.get(campus_id, f"Unknown ({campus_id})")
    birth_city = first_present(
        record, "birth_city.name", "person.birth_city.name", "profile.birth_city.name"
    )
    residence_city = first_present(
        record,
        "residence_city.name",
        "person.residence_city.name",
        "profile.residence_city.name",
    )
    record["campus_by_birth"] = CITY_TO_CAMPUS.get(birth_city, "")
    record["current_campus"] = CITY_TO_CAMPUS.get(residence_city, "")
    return derive_carding(record)


def with_query(url: str, **parameters: Any) -> str:
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query.update({key: str(value) for key, value in parameters.items()})
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


@dataclass
class FetchResult:
    rows: list[dict[str, Any]]
    complete: bool
    pages: int
    error: str = ""
    columns: list[dict[str, Any]] = field(default_factory=list)


def api_error_message(response: requests.Response, *, page: int | None = None) -> str:
    location = f" on page {page}" if page is not None else ""
    if response.status_code == 401:
        return f"Authentication expired{location}. Please sign out and sign in again."
    if response.status_code == 403:
        return (
            f"Access denied by the registration reporting API{location}. "
            + (
                "Registration API v2 uses graph permissions: your account needs an active acting "
                "profile at this site and Registration report permissions. Contact an Apps administrator."
                if API_VERSION == "v2"
                else "Your account needs permission to view registration reports; contact an Apps administrator."
            )
        )
    if response.status_code == 404 and API_VERSION == "v2":
        return (
            f"The registration API returned Not Found{location}. In v2 this can mean the data is "
            "outside your acting profile's permissions."
        )
    return f"API returned HTTP {response.status_code}{location}."


def check_acting_profile(token: str, *, session: requests.Session | None = None) -> str:
    """Return an error message when v2 has no active acting profile for this user, else ''.

    Problems reaching the actor endpoint itself are not treated as blocking; the
    data request reports its own errors.
    """
    if API_VERSION != "v2":
        return ""
    client = session or requests
    try:
        response = client.get(
            ACTOR_URL, headers={"Authorization": f"Bearer {token}"}, timeout=REQUEST_TIMEOUT
        )
    except requests.RequestException:
        return ""
    if response.status_code in {401, 403}:
        return (
            "Registration API v2 did not accept your account: it needs an active acting profile "
            "for this site. Sign out and in again, or ask an Apps administrator to check your profile."
        )
    if response.status_code != 200:
        return ""
    try:
        payload = response.json()
    except ValueError:
        return ""
    current = payload.get("current_profile") if isinstance(payload, dict) else None
    if not (isinstance(current, dict) and current.get("id")):
        return (
            "Your account has no active acting profile for this site, which Registration API v2 "
            "requires. Ask an Apps administrator to check your profile and permissions."
        )
    return ""


# Fields that come back blank when a v2 graph permission is missing, with the
# rows they apply to and the permission an administrator would need to grant.
PERMISSION_FIELD_CHECKS = [
    ("Email", ("email", "person.email"), "all", "registration.person:view"),
    ("Date of birth", ("dob", "person.dob"), "all", "registration.person:view"),
    ("Enrollment status", ("enrollment_status", "current_enrollment.admin_status"), "profile", "registration.enrollment:list"),
    ("Nomination fiscal year", ("nomination_fiscal_year", "current_nomination.fiscal_year"), "all", "registration.nomination:list"),
]


def blank_field_warnings(frame: pd.DataFrame) -> list[str]:
    """Key fields that are missing or entirely blank, which in v2 usually means a missing permission."""
    if frame.empty:
        return []
    kinds = (
        frame["row_kind"].fillna("").astype(str).str.strip().str.lower()
        if "row_kind" in frame
        else pd.Series("profile", index=frame.index)
    )
    warnings = []
    for label, names, applies_to, permission in PERMISSION_FIELD_CHECKS:
        rows = frame if applies_to == "all" else frame[kinds.isin({"profile", "profiles"})]
        if rows.empty:
            continue
        name = next((name for name in names if name in rows), "")
        filled = (
            ~rows[name].fillna("").astype(str).str.strip().isin({"", "nan", "None", "NaT"})
        ).sum() if name else 0
        if not filled:
            warnings.append(f"{label} is blank for every row (check the {permission} permission).")
    return warnings


def fetch_paginated(
    url: str,
    headers: dict[str, str],
    *,
    session: requests.Session | None = None,
    sleep=time.sleep,
) -> FetchResult:
    client = session or requests.Session()
    rows: list[dict[str, Any]] = []
    report_columns: list[dict[str, Any]] = []
    pages = 0
    next_url: str | None = with_query(url, limit=PAGE_LIMIT)

    while next_url:
        pages += 1
        wait = BACKOFF_SECONDS
        for attempt in range(MAX_RETRIES + 1):
            try:
                response = client.get(next_url, headers=headers, timeout=REQUEST_TIMEOUT)
                if response.status_code in RETRYABLE_STATUSES and attempt < MAX_RETRIES:
                    retry_after = response.headers.get("Retry-After")
                    delay = float(retry_after) if retry_after else wait + random.uniform(0, 0.5)
                    sleep(delay)
                    wait *= 2
                    continue
                if response.status_code != 200:
                    return FetchResult(
                        rows,
                        False,
                        pages,
                        api_error_message(response, page=pages),
                        report_columns,
                    )
                try:
                    payload = response.json()
                except ValueError:
                    return FetchResult(rows, False, pages, f"Invalid JSON on page {pages}.", report_columns)
                if isinstance(payload.get("columns"), list):
                    report_columns = payload["columns"]
                report_key = payload.get("report")
                results = payload.get(report_key) if report_key else payload.get("results")
                if not isinstance(results, list):
                    results = payload.get("report_rows")
                if not isinstance(results, list):
                    return FetchResult(
                        rows, False, pages, f"Unexpected API response on page {pages}.", report_columns
                    )
                rows.extend(results)
                next_url = payload.get("next")
                break
            except (ReadTimeout, ConnectTimeout, ConnectionError) as exc:
                if attempt >= MAX_RETRIES:
                    return FetchResult(
                        rows,
                        False,
                        pages,
                        f"{type(exc).__name__} after {MAX_RETRIES + 1} attempts on page {pages}.",
                        report_columns,
                    )
                sleep(wait + random.uniform(0, 0.5))
                wait *= 2
            except requests.RequestException as exc:
                return FetchResult(rows, False, pages, f"Request failed: {exc}", report_columns)

    return FetchResult(rows, True, pages, columns=report_columns)


def remove_test_sports(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    keep = pd.Series(True, index=frame.index)
    for column in ("sport.name", "current_nomination.sport.name"):
        if column not in frame:
            continue
        sports = frame[column].fillna("").astype(str).str.strip().str.lower()
        keep &= ~(sports.isin(TEST_SPORTS) | sports.str.contains("(test", regex=False))
    return frame.loc[keep]


def apply_filters(
    frame: pd.DataFrame,
    birth_campus: str = "",
    current_campus: str = "",
    enrollment_statuses: list[str] | None = None,
    nomination_claimed: list[str] | None = None,
    list_filters: dict[str, list[str]] | None = None,
) -> pd.DataFrame:
    """``list_filters`` maps a canonical export field (see EXPORT_LIST_FILTERS) to the values to keep."""
    result = frame
    filters = (
        (("birth_city_campus", "campus_by_birth"), birth_campus),
        (("residence_city_campus", "current_campus"), current_campus),
    )
    for candidates, value in filters:
        column_name = next((name for name in candidates if name in result), "")
        if value and column_name:
            result = result[result[column_name] == value]
    status_filters = (
        (("enrollment_status", "current_enrollment.admin_status"), enrollment_statuses),
        (("nomination_claimed", "current_nomination.redeemed"), nomination_claimed),
    )
    for candidates, values in status_filters:
        column_name = next((name for name in candidates if name in result), "")
        if values and column_name:
            result = result[result[column_name].astype(str).isin(values)]
    for field_name, values in (list_filters or {}).items():
        column_name = next((name for name in EXPORT_FIELD_ALIASES.get(field_name, [field_name]) if name in result), "")
        if values and column_name:
            result = result[result[column_name].fillna("").astype(str).str.strip().isin(values)]
    return remove_test_sports(result)


def export_filter_options(frame: pd.DataFrame, field_name: str) -> list[dict[str, str]]:
    return options_for_any(frame, tuple(EXPORT_FIELD_ALIASES.get(field_name, [field_name])))


def selected_list_filters(values: list[Any] | None, ids: list[dict[str, str]] | None) -> dict[str, list[str]]:
    """Pair the pattern-matched filter dropdowns' values with their fields, keeping only active filters."""
    return {item["field"]: value for item, value in zip(ids or [], values or []) if value}


def build_export(frame: pd.DataFrame, fields: list[str] | None) -> pd.DataFrame:
    requested = DEFAULT_FIELDS if fields is None else fields
    sources: list[str] = []
    labels: list[str] = []
    for field in requested:
        source = next(
            (
                candidate
                for candidate in EXPORT_FIELD_ALIASES.get(field, [field])
                if candidate in frame.columns
            ),
            "",
        )
        if source and source not in sources:
            sources.append(source)
            labels.append(FIELD_LABELS.get(field, field))
    exported = frame.loc[:, sources].copy()
    exported.columns = labels
    return exported


def options_for(frame: pd.DataFrame, column: str) -> list[dict[str, str]]:
    if column not in frame:
        return []
    values = sorted(
        value
        for value in frame[column].dropna().astype(str).str.strip().unique()
        if value
    )
    return [{"label": value, "value": value} for value in values]


def options_for_any(
    frame: pd.DataFrame, candidates: tuple[str, ...]
) -> list[dict[str, str]]:
    column_name = next((name for name in candidates if name in frame), "")
    return options_for(frame, column_name) if column_name else []


def select_fetch_source(frame: pd.DataFrame, source: str) -> pd.DataFrame:
    """Keep only the requested row type when the API returns mixed records."""
    kind_column = next(
        (name for name in ("row_kind", "kind", "row_type", "type") if name in frame),
        "",
    )
    if not kind_column:
        return frame if source == "profile" else frame.iloc[0:0].copy()
    if source == "all":
        return frame.copy()
    kinds = frame[kind_column].fillna("").astype(str).str.strip().str.lower()
    if source == "nomination":
        return frame[kinds.isin({"nomination", "nominations"})].copy()
    return frame[kinds.isin({"profile", "profiles", "person", "people"})].copy()


def normalize_report_value(value: Any, definition: dict[str, Any]) -> str:
    if value is None:
        return ""
    option_labels = {
        str(option.get("value")): option.get("label", safe_string(option.get("value")))
        for option in definition.get("options") or []
    }
    if definition.get("is_multiselect"):
        values = value if isinstance(value, list) else [value]
        return "; ".join(option_labels.get(str(item), safe_string(item)) for item in values)
    if option_labels:
        return option_labels.get(str(value), safe_string(value))
    return safe_string(value)


def flatten_report_row(
    row: dict[str, Any], definitions: list[dict[str, Any]]
) -> dict[str, str]:
    cells = row.get("cells") if isinstance(row.get("cells"), dict) else {}
    meta = row.get("meta") if isinstance(row.get("meta"), dict) else {}
    record: dict[str, str] = {}
    for definition in definitions:
        key = definition.get("key")
        if not key:
            continue
        target = definition.get("target")
        source = cells if target == "cells" else meta if target == "meta" else row
        value = source.get(key)
        if value is None:
            value = row.get(key, cells.get(key, meta.get(key)))
        record[key] = normalize_report_value(value, definition)
    for source in (row, cells, meta):
        for key, value in source.items():
            if key not in {"cells", "meta"} and key not in record:
                record[key] = safe_string(value)
    return record


def fetch_report_columns(token: str, url: str | None = None) -> list[dict[str, Any]]:
    response = requests.get(
        url or REPORT_COLUMNS_URL,
        headers={"Authorization": f"Bearer {token}"},
        timeout=REQUEST_TIMEOUT,
    )
    if response.status_code != 200:
        raise PermissionError(api_error_message(response))
    payload = response.json()
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        definitions = payload.get("results", payload.get("columns", []))
        return definitions if isinstance(definitions, list) else []
    return []


def cache_path(dataset_id: str) -> Path:
    try:
        canonical = str(uuid.UUID(dataset_id))
    except (ValueError, TypeError, AttributeError) as exc:
        raise ValueError("Invalid dataset ID.") from exc
    return CACHE_DIR / f"{canonical}.pkl"


def cleanup_cache(now: float | None = None) -> None:
    CACHE_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    cutoff = (now or time.time()) - CACHE_TTL_SECONDS
    for path in CACHE_DIR.glob("*.pkl"):
        try:
            if path.stat().st_mtime < cutoff:
                path.unlink()
        except FileNotFoundError:
            continue


def save_dataset(frame: pd.DataFrame) -> str:
    cleanup_cache()
    dataset_id = str(uuid.uuid4())
    path = cache_path(dataset_id)
    frame.to_pickle(path)
    path.chmod(0o600)
    return dataset_id


def load_dataset(dataset_id: str | None) -> pd.DataFrame:
    if not dataset_id:
        raise FileNotFoundError("No dataset has been fetched.")
    cleanup_cache()
    path = cache_path(dataset_id)
    if not path.exists():
        raise FileNotFoundError("This dataset expired. Fetch profiles again.")
    return pd.read_pickle(path)


def fetch_report_frame(
    token: str, rows_url: str | None = None, columns_url: str | None = None
) -> tuple[pd.DataFrame, list[str], int]:
    """Fetch every report row (profiles and nominations) as a flat frame."""
    rows_url = rows_url or PROFILES_URL
    headers = {"Authorization": f"Bearer {token}"}
    errors: list[str] = []
    try:
        definitions = fetch_report_columns(token, columns_url)
    except (requests.RequestException, PermissionError) as exc:
        definitions = []
        errors.append(f"Column dictionary unavailable: {exc}")
    selected_keys = [definition.get("key") for definition in definitions if definition.get("key")]
    url = with_query(rows_url, columns=",".join(selected_keys)) if selected_keys else rows_url
    result = fetch_paginated(url, headers)
    if not result.complete:
        errors.append(result.error)
    definitions = result.columns or definitions
    return rows_to_frame(result.rows, definitions), errors, result.pages



def filter_selection(
    frame: pd.DataFrame, campus_value: int | str, role_value: str, source: str = "all"
) -> pd.DataFrame:
    frame = select_fetch_source(frame, source)
    if isinstance(campus_value, int):
        campus_column = next(
            (name for name in ("campus_label", "preferred_campus", "campus") if name in frame),
            "",
        )
        if campus_column:
            frame = frame[frame[campus_column] == CAMPUS_LABELS[campus_value]]
    role_column = next((name for name in ("role", "role_slug") if name in frame), "")
    if role_value and role_column:
        frame = frame[
            frame[role_column].fillna("").astype(str).str.lower() == role_value.lower()
        ]
    return frame


def rows_to_frame(rows: list[dict[str, Any]], definitions: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            flatten_report_row(row, definitions)
            if isinstance(row.get("cells"), dict) or isinstance(row.get("meta"), dict)
            else flatten_json(row)
            for row in rows
        ]
    )


# ---- Segmented fetch (one short request per step; see FETCH_STEP_SECONDS) ----


def job_path(job_id: str) -> Path:
    try:
        canonical = str(uuid.UUID(job_id))
    except (ValueError, TypeError, AttributeError) as exc:
        raise ValueError("Invalid fetch job ID.") from exc
    return CACHE_DIR / f"{canonical}.job.pkl"  # *.pkl, so cleanup_cache expires it too


def save_job(job: dict[str, Any]) -> None:
    CACHE_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    path = job_path(job["id"])
    temporary = path.with_suffix(".tmp")
    pd.to_pickle(job, temporary)
    temporary.chmod(0o600)
    temporary.replace(path)


def load_job(job_id: str | None) -> dict[str, Any]:
    if not job_id:
        raise FileNotFoundError("No fetch is running.")
    path = job_path(job_id)
    if not path.exists():
        raise FileNotFoundError("This fetch expired. Fetch again.")
    return pd.read_pickle(path)


def start_fetch_job(
    token: str,
    campus_value: int | str,
    role_value: str,
    source: str,
    rows_url: str | None = None,
    columns_url: str | None = None,
) -> dict[str, Any]:
    """Create the job (no rows yet). The token is never written to disk."""
    errors: list[str] = []
    try:
        definitions = fetch_report_columns(token, columns_url)
    except (requests.RequestException, PermissionError, ValueError) as exc:
        definitions = []
        errors.append(f"Column dictionary unavailable: {exc}")
    selected_keys = [definition.get("key") for definition in definitions if definition.get("key")]
    url = rows_url or PROFILES_URL
    if selected_keys:
        url = with_query(url, columns=",".join(selected_keys))
    job = {
        "id": str(uuid.uuid4()),
        "status": "running",  # running -> done | failed
        "next_url": with_query(url, limit=PAGE_LIMIT),
        "pages": 0,
        "rows": [],
        "definitions": definitions,
        "errors": errors,
        "attempt": 0,
        "wait": BACKOFF_SECONDS,
        "retry_at": 0.0,
        "selection": {"campus": campus_value, "role": role_value, "source": source},
        "result": None,
    }
    save_job(job)
    return job


def _retry_or_fail(job: dict[str, Any], reason: str, retry_after: str | None, now: float) -> None:
    job["attempt"] += 1
    if job["attempt"] > MAX_RETRIES:
        job["status"] = "failed"
        job["errors"].append(f"{reason} after {MAX_RETRIES + 1} attempts on page {job['pages'] + 1}.")
        return
    try:
        delay = float(retry_after) if retry_after else job["wait"] + random.uniform(0, 0.5)
    except ValueError:
        delay = job["wait"]
    job["retry_at"] = now + delay
    job["wait"] *= 2


def advance_fetch_job(
    job: dict[str, Any],
    headers: dict[str, str],
    *,
    session: requests.Session | None = None,
    budget: float = FETCH_STEP_SECONDS,
    clock=time.time,
) -> None:
    """Fetch pages until the job finishes or ``budget`` seconds pass; updates ``job`` in place.

    Each page gets one attempt per step. A retryable failure schedules the retry for a later
    step (with backoff), so no single request waits through every retry.
    """
    client = session or requests.Session()
    started = clock()
    while job["status"] == "running":
        if not job["next_url"]:
            job["status"] = "done"
            break
        if clock() < job["retry_at"]:
            break
        page = job["pages"] + 1
        try:
            response = client.get(job["next_url"], headers=headers, timeout=STEP_REQUEST_TIMEOUT)
        except (ReadTimeout, ConnectTimeout, ConnectionError) as exc:
            _retry_or_fail(job, type(exc).__name__, None, clock())
            break
        except requests.RequestException as exc:
            job["status"] = "failed"
            job["errors"].append(f"Request failed: {exc}")
            break
        if response.status_code in RETRYABLE_STATUSES:
            _retry_or_fail(job, f"HTTP {response.status_code}", response.headers.get("Retry-After"), clock())
            break
        if response.status_code != 200:
            job["status"] = "failed"
            job["errors"].append(api_error_message(response, page=page))
            break
        try:
            payload = response.json()
        except ValueError:
            job["status"] = "failed"
            job["errors"].append(f"Invalid JSON on page {page}.")
            break
        if isinstance(payload.get("columns"), list):
            job["definitions"] = payload["columns"]
        report_key = payload.get("report")
        results = payload.get(report_key) if report_key else payload.get("results")
        if not isinstance(results, list):
            results = payload.get("report_rows")
        if not isinstance(results, list):
            job["status"] = "failed"
            job["errors"].append(f"Unexpected API response on page {page}.")
            break
        job["rows"].extend(results)
        job["pages"] = page
        job["next_url"] = payload.get("next")
        job["attempt"], job["wait"], job["retry_at"] = 0, BACKOFF_SECONDS, 0.0
        if clock() - started >= budget:
            break
    if job["status"] == "running" and not job["next_url"]:
        job["status"] = "done"


def finish_fetch_job(job: dict[str, Any]) -> pd.DataFrame:
    selection = job["selection"]
    frame = rows_to_frame(job["rows"], job["definitions"])
    return filter_selection(frame, selection["campus"], selection["role"], selection["source"])

def card(title: str, value_id: str) -> html.Div:
    return html.Div(
        [html.Div(title, className="metric-title"), html.Div("—", id=value_id, className="metric-value")],
        className="metric-card",
    )


def join_app_url(base: str, suffix: str) -> str:
    """Append a route to APP_URL, keeping its path.

    dash-auth-external builds the OAuth redirect URI with urljoin(APP_URL, "/redirect"),
    which drops any path. Posit Connect serves apps under a path (…/content/<id>/), so
    the redirect would point at the server root. Locally (no path) the result is the same.
    """
    return base.rstrip("/") + "/" + suffix.lstrip("/")


_dash_auth_external_auth.urljoin = join_app_url
# Where to send people after sign-in; includes APP_URL's path when hosted under one (Posit Connect).
APP_PATH = urlsplit(APP_URL).path.rstrip("/")

auth = DashAuthExternal(
    AUTH_URL,
    TOKEN_URL,
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    app_url=APP_URL,
    home_suffix=f"{APP_PATH}/home",
    _secret_key=os.environ.get("FLASK_SECRET_KEY"),
)
server = auth.server




app = Dash(__name__, server=server, title="List Generator Lite")

app.index_string = """
<!DOCTYPE html>
<html>
  <head>
    {%metas%}<title>{%title%}</title>{%favicon%}{%css%}
    <style>
      :root { --navy:#12304a; --blue:#1676a3; --sky:#eaf5fa; --ink:#17232d;
              --muted:#62727e; --border:#d8e1e7; --danger:#9b2c2c; }
      * { box-sizing:border-box; }
      body { margin:0; background:#f5f7f8; color:var(--ink);
             font-family:Inter, ui-sans-serif, system-ui, -apple-system, sans-serif; }
      .shell { max-width:1500px; margin:auto; padding:30px; }
      .hero { background:linear-gradient(120deg,var(--navy),#195a76); color:white;
              border-radius:16px; padding:28px 32px; box-shadow:0 8px 30px #12304a24; }
      h1 { margin:0; font-size:32px; } h2 { margin:0 0 16px; font-size:20px; }
      .subtitle { opacity:.82; margin-top:6px; }
      .panel { background:white; border:1px solid var(--border); border-radius:14px;
               padding:22px; margin-top:18px; box-shadow:0 3px 15px #17232d0a; }
      .filter-grid { display:grid; grid-template-columns:repeat(4,minmax(190px,1fr)); gap:14px; }
    .ee-sport-year-controls { display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:14px; margin-top:14px; }
      .label { color:var(--muted); font-size:13px; font-weight:700; margin-bottom:6px; }
      .actions { display:flex; gap:10px; align-items:center; flex-wrap:wrap; margin-top:18px; }
      button { border:0; border-radius:8px; padding:11px 18px; font-weight:700;
               cursor:pointer; background:var(--blue); color:white; }
      button.secondary { background:white; color:var(--navy); border:1px solid var(--border); }
      button:disabled { opacity:.45; cursor:not-allowed; }
      .metrics { display:grid; grid-template-columns:repeat(3,1fr); gap:14px; margin-top:18px; }
      .metrics.six { grid-template-columns:repeat(6,1fr); }
      @media(max-width:1100px) { .metrics.six { grid-template-columns:repeat(3,1fr); } }
      .metric-card { background:white; border:1px solid var(--border); border-radius:12px; padding:18px; }
      .metric-title { color:var(--muted); font-size:13px; font-weight:700; }
      .metric-value { color:var(--navy); font-size:25px; font-weight:800; margin-top:3px; }
      .status { border-radius:9px; padding:12px 14px; margin-top:14px; background:var(--sky); }
      .status.error { background:#fff0f0; color:var(--danger); }
      .hint { color:var(--muted); font-size:13px; line-height:1.5; }
      .workflow-choice label { display:inline-flex; align-items:center; gap:7px; margin-right:24px;
                               font-weight:700; cursor:pointer; }
      .control-row { display:flex; gap:8px; align-items:center; flex-wrap:wrap; margin:10px 0 14px; }
      .control-row button { padding:7px 11px; font-size:12px; background:white; color:var(--navy);
                            border:1px solid var(--border); }
      .empty-state { border:1px dashed var(--border); border-radius:10px; padding:28px;
                     text-align:center; color:var(--muted); margin-top:14px; }
      .filter-summary { display:flex; align-items:center; justify-content:space-between; gap:12px;
                        background:var(--sky); border-radius:9px; padding:10px 13px; margin:14px 0; }
      .report-summary { border-left:4px solid var(--blue); background:var(--sky); padding:12px 14px;
                        margin-top:14px; border-radius:0 9px 9px 0; }
      .warning { background:#fff8e6; color:#674d00; border:1px solid #ead79a; border-radius:9px;
                 padding:12px 14px; margin-top:12px; }
      .table-note { color:var(--muted); font-size:12px; margin:8px 0; }
      details { margin-top:14px; } summary { cursor:pointer; font-weight:700; }
      @media(max-width:900px) { .filter-grid { grid-template-columns:1fr 1fr; } }
      @media(max-width:560px) { .shell { padding:14px; } .filter-grid,.metrics { grid-template-columns:1fr; } }
    </style>
  </head>
  <body>{%app_entry%}<footer>{%config%}{%scripts%}{%renderer%}</footer></body>
</html>
"""


def dropdown_block(label: str, component: Any) -> html.Div:
    return html.Div([html.Div(label, className="label"), component])


app.layout = html.Div(
    className="shell",
    children=[
        dcc.Store(id="dataset-id", storage_type="session"),
        dcc.Store(id="fetch-job", storage_type="memory"),
        # Drives the segmented fetch: each step raises max_intervals by one, so the next
        # step starts only after the previous one has returned (no overlapping requests).
        dcc.Interval(id="fetch-interval", interval=300, n_intervals=0, max_intervals=0, disabled=True),
        dcc.Store(id="ni-cohort", storage_type="memory"),
        html.Div(
            className="hero",
            children=[
                html.H1("List Generator Lite"),
                html.Div(
                    "Build accurate, reviewable profile lists before exporting.",
                    className="subtitle",
                ),
            ],
        ),
        html.Div(
            className="panel",
            children=[
                html.H2("1. Choose profiles to fetch"),
                dropdown_block(
                    "API data type",
                    dcc.Tabs(
                        id="fetch-source",
                        children=[
                            dcc.Tab(label=option["label"], value=option["value"])
                            for option in FETCH_SOURCE_OPTIONS
                        ],
                        value="all",
                    ),
                ),
                html.Div(
                    className="filter-grid",
                    children=[
                        dropdown_block(
                            "Preferred campus",
                            dcc.Dropdown(id="campus-dd", options=CAMPUS_OPTIONS, value="all", clearable=False),
                        ),
                        dropdown_block(
                            "Role",
                            dcc.Dropdown(id="role-dd", options=ROLE_OPTIONS, value="", clearable=False),
                        ),
                    ],
                ),
                html.Div(
                    [html.Button("Fetch selected data", id="fetch-button"), html.Span(id="fetch-scope", className="hint")],
                    className="actions",
                ),
                html.Div(id="fetch-status"),
            ],
        ),
        html.Div(
            className="metrics",
            children=[
                card("Fetched profiles", "fetched-count"),
                card("Matching export filters", "filtered-count"),
                card("Last refreshed", "last-refreshed"),
            ],
        ),
        html.Div(
            className="panel",
            children=[
                html.H2("2. Choose an output"),
                html.P("Create a flexible CSV or use a report with fixed, documented rules.", className="hint"),
                dcc.Tabs(
                    id="workflow-mode",
                    children=[
                        dcc.Tab(label="Custom export", value="custom"),
                        dcc.Tab(label="Prebuilt reports", value="prebuilt"),
                        dcc.Tab(label="National Inventory", value="national_inventory"),
                    ],
                    value="custom",
                ),
            ],
        ),
        html.Div(
            id="custom-workflow",
            children=[
        html.Div(
            className="panel",
            children=[
                html.H2("3. Refine the custom export"),
                html.Div(
                    className="filter-grid",
                    children=[
                        dropdown_block(
                            "Campus by birth",
                            dcc.Dropdown(
                                id="birth-campus-dd",
                                options=[{"label": "All", "value": ""}]
                                + [{"label": item, "value": item} for item in MAPPED_CAMPUS_NAMES],
                                value="",
                                clearable=False,
                                disabled=True,
                            ),
                        ),
                        dropdown_block(
                            "Current campus",
                            dcc.Dropdown(
                                id="current-campus-dd",
                                options=[{"label": "All", "value": ""}]
                                + [{"label": item, "value": item} for item in MAPPED_CAMPUS_NAMES],
                                value="",
                                clearable=False,
                                disabled=True,
                            ),
                        ),
                        dropdown_block(
                            "Enrollment status",
                            dcc.Dropdown(id="enrollment-dd", options=[], value=[], multi=True, disabled=True),
                        ),
                        dropdown_block(
                            "Nomination claimed",
                            dcc.Dropdown(id="nomination-dd", options=[], value=[], multi=True, disabled=True),
                        ),
                        *[
                            dropdown_block(
                                label,
                                dcc.Dropdown(
                                    id=export_filter_id(field_name),
                                    options=[],
                                    value=[],
                                    multi=True,
                                    placeholder="All",
                                    disabled=True,
                                ),
                            )
                            for field_name, label in EXPORT_LIST_FILTERS
                        ],
                    ],
                ),
                html.Details(
                    [
                        html.Summary("Choose export columns"),
                        html.P(
                            "Sensitive fields are excluded by default and clearly marked. "
                            "Select them only when required for the intended purpose.",
                            className="hint",
                        ),
                        dcc.Dropdown(
                            id="column-select",
                            options=[
                                {"label": label, "value": field}
                                for field, label, _ in EXPORT_COLUMNS
                            ],
                            value=DEFAULT_FIELDS,
                            multi=True,
                            disabled=True,
                        ),
                        html.Div(
                            [
                                html.Button("Select all", id="export-select-all", type="button", disabled=True),
                                html.Button("Clear", id="export-clear", type="button", disabled=True),
                                html.Button("Reset defaults", id="export-reset", type="button", disabled=True),
                                html.Span(id="export-column-count", className="hint"),
                            ],
                            className="control-row",
                        ),
                    ]
                ),
            ],
        ),
        html.Div(
            className="panel",
            children=[
                html.H2("4. Review"),
                html.P(
                    "The preview follows your export columns unless you choose to customize it.",
                    className="hint",
                ),
                dcc.Checklist(
                    id="customize-preview",
                    options=[{"label": "Customize preview columns", "value": "custom", "disabled": True}],
                    value=[],
                    inputStyle={"marginRight": "7px"},
                ),
                dcc.Dropdown(
                    id="preview-column-select",
                    options=[
                        {"label": label, "value": field}
                        for field, label, _ in EXPORT_COLUMNS
                    ],
                    value=PREVIEW_FIELDS,
                    multi=True,
                    placeholder="Choose preview columns",
                    disabled=True,
                ),
                html.Div(
                    [
                        html.Button("Select all", id="preview-select-all", type="button"),
                        html.Button("Clear", id="preview-clear", type="button"),
                        html.Button("Reset defaults", id="preview-reset", type="button"),
                        html.Span(id="preview-column-count", className="hint"),
                    ],
                    id="preview-controls",
                    className="control-row",
                    style={"display": "none"},
                ),
                html.Div(
                    [
                        html.Span(id="active-filter-text"),
                        html.Button("Clear all filters", id="clear-filters", type="button", className="secondary", disabled=True),
                    ],
                    id="active-filter-summary",
                    className="filter-summary",
                ),
                html.Div(id="preview-empty", className="empty-state"),
                html.Div(id="preview-table-note", className="table-note"),
                dash_table.DataTable(
                    id="preview-table",
                    page_size=10,
                    sort_action="native",
                    filter_action="native",
                    fixed_rows={"headers": True},
                    style_table={"overflowX": "auto", "maxHeight": "560px", "overflowY": "auto"},
                    style_header={"backgroundColor": "#12304a", "color": "white", "fontWeight": "700"},
                    style_cell={"textAlign": "left", "fontSize": "13px", "padding": "8px", "maxWidth": "260px"},
                    style_data_conditional=[{"if": {"row_index": "odd"}, "backgroundColor": "#f5f7f8"}],
                ),
            ],
        ),
        html.Div(
            className="panel",
            children=[
                html.H2("5. Download custom export"),
                html.P(id="download-summary", className="hint"),
                html.Div(
                    className="actions",
                    children=[
                        html.Button("Download curated CSV", id="download-curated", disabled=True),
                        html.Button(
                            "Download complete raw data",
                            id="download-full",
                            disabled=True,
                            className="secondary",
                        ),
                    ],
                ),
                html.Div(
                    "Complete raw data includes every field returned by the API and ignores the custom filters and column selection.",
                    className="warning",
                ),
                html.Div(id="download-error"),
                dcc.Download(id="curated-file"),
                dcc.Download(id="full-file"),
            ],
        ),
            ],
        ),
        html.Div(
            id="prebuilt-workflow",
            className="panel",
            style={"display": "none"},
            children=[
                html.H2("3. Build a prebuilt report"),
                html.P(
                    "Choose one of the established Prebuilt reports (documentation below). "
                    "Each preset applies its documented filters and fixed output schema.",
                    className="hint",
                ),
                html.Div(id="selected-report-summary"),
                html.Div(
                    className="filter-grid",
                    children=[
                        dropdown_block(
                            "Prebuilt report",
                            dcc.Dropdown(
                                id="prebuilt-report",
                                options=REPORT_OPTIONS,
                                value="pending_unmatched",
                                clearable=False,
                            ),
                        ),
                        html.Div(
                            dropdown_block(
                                "Fiscal year",
                                dcc.Dropdown(
                                    id="fiscal-year-dd",
                                    options=[{"label": "All fiscal years", "value": ""}],
                                    value="",
                                    clearable=False,
                                    disabled=True,
                                ),
                            ),
                            id="fiscal-year-block",
                        ),
                        html.Div(
                            [
                                html.Div("ViaSport LOU sports", className="label"),
                                dcc.Dropdown(
                                    id="lou-sports-dd",
                                    options=[],
                                    value=[],
                                    multi=True,
                                ),
                            ],
                            id="lou-sports-block",
                            style={"display": "none"},
                        ),
                        html.Div(
                            [
                                html.Div("Enhanced Excellence sports", className="label"),
                                dcc.Dropdown(
                                    id="enhanced-sports-dd",
                                    options=[],
                                    value=[],
                                    multi=True,
                                ),
                            ],
                            id="enhanced-sports-block",
                            style={"display": "none"},
                        ),
                    ],
                ),
                html.Div(
                    [
                        html.Button(
                            "Download selected prebuilt report",
                            id="download-prebuilt",
                            disabled=True,
                        )
                    ],
                    className="actions",
                ),
                html.Div(id="prebuilt-status"),
                dcc.Download(id="prebuilt-file"),
                html.Details(
                    [
                        html.Summary("Prebuilt report dictionary"),
                        dcc.Markdown(REPORT_GUIDE, className="report-guide"),
                    ],
                    open=False,
                ),
            ],
        ),
        html.Div(
            id="national-inventory-workflow",
            style={"display": "none"},
            children=[
                html.Div(
                    className="panel",
                    children=[
                        html.H2("3. Build the National Inventory athlete list"),
                        html.P(
                            "Every source is reduced to one row per athlete and sport. Only Name and Sport carry forward.",
                            className="hint",
                        ),
                        dropdown_block(
                            "Athlete-list source",
                            dcc.RadioItems(
                                id="ni-source",
                                options=[
                                    {"label": "Registration data", "value": "registration"},
                                    {"label": "AAP monthly lists", "value": "aap"},
                                    {"label": "COPSIN National Inventory", "value": "copsin"},
                                ],
                                value="registration",
                                inline=True,
                                className="workflow-choice",
                            ),
                        ),
                        dropdown_block(
                            "Fiscal year",
                            dcc.Dropdown(
                                id="ni-fiscal-year",
                                options=[{"label": "Choose a fiscal year", "value": ""}],
                                value="",
                                clearable=False,
                            ),
                        ),
                        html.Div(
                            html.P(
                                "Uses fetched registration data to select approved SC-carded athletes.",
                                className="hint",
                            ),
                            id="ni-registration-source",
                        ),
                        html.Div(
                            dcc.Upload(
                                id="ni-aap-upload",
                                children=html.Div(["Drag monthly AAP files here or ", html.A("choose files")]),
                                multiple=True,
                                accept=".csv,.xlsx,.xls,.xlsm",
                            ),
                            id="ni-aap-source",
                            style={"display": "none"},
                            className="upload-box",
                        ),
                        html.Div(
                            dcc.Upload(
                                id="ni-copsin-upload",
                                children=html.Div(["Drag a COPSIN inventory here or ", html.A("choose a file")]),
                                multiple=False,
                                accept=".csv,.xlsx,.xls,.xlsm",
                            ),
                            id="ni-copsin-source",
                            style={"display": "none"},
                            className="upload-box",
                        ),
                        html.Div([html.Button("Build athlete list", id="ni-build")], className="actions"),
                        html.Div(id="ni-build-status"),
                    ],
                ),
                html.Div(
                    className="panel",
                    children=[
                        html.H2("4. Select the LOU sports"),
                        html.P(
                            "Choose which LOU sports from the athlete list should be assigned services.",
                            className="hint",
                        ),
                        dcc.Dropdown(
                            id="ni-lou-sports",
                            options=[],
                            value=[],
                            multi=True,
                            placeholder="Select LOU sports",
                        ),
                    ],
                ),
                html.Div(
                    className="panel",
                    children=[
                        html.H2("5. Select services provided to each LOU sport"),
                        html.P(
                            "A selected service writes ‘Partial Claim’ for every athlete in that sport.",
                            className="hint",
                        ),
                        html.Div(id="ni-service-matrix"),
                    ],
                ),
                html.Div(
                    className="panel",
                    children=[
                        html.H2("6. Review and download"),
                        html.P(
                            "Registration is ‘Partial Claim’ when the athlete has an active enrollment and a claimed nomination for the selected fiscal year.",
                            className="hint",
                        ),
                        html.Div(id="ni-preview-status"),
                        dash_table.DataTable(
                            id="ni-preview",
                            columns=[{"name": name, "id": name} for name in NATIONAL_INVENTORY_COLUMNS],
                            data=[],
                            page_size=15,
                            sort_action="native",
                            filter_action="native",
                            style_table={"overflowX": "auto"},
                            style_header={"backgroundColor": "#12304a", "color": "white", "fontWeight": "700"},
                            style_cell={"textAlign": "left", "fontSize": "13px", "padding": "8px"},
                        ),
                        html.Div([html.Button("Download National Inventory CSV", id="ni-download", disabled=True)], className="actions"),
                        dcc.Download(id="ni-file"),
                    ],
                ),
            ],
        ),
        html.Div(
            CITY_MAPPING_WARNING or "",
            className="status error" if CITY_MAPPING_WARNING else "",
        ),
    ],
)


@app.callback(
    Output("custom-workflow", "style"),
    Output("prebuilt-workflow", "style"),
    Output("national-inventory-workflow", "style"),
    Input("workflow-mode", "value"),
)
def show_workflow(mode: str):
    hidden = {"display": "none"}
    visible = {"display": "block"}
    return (
        visible if mode == "custom" else hidden,
        visible if mode == "prebuilt" else hidden,
        visible if mode == "national_inventory" else hidden,
    )


@app.callback(
    Output("ni-registration-source", "style"),
    Output("ni-aap-source", "style"),
    Output("ni-copsin-source", "style"),
    Input("ni-source", "value"),
)
def show_national_inventory_source(source: str):
    visible = {"display": "block"}
    hidden = {"display": "none"}
    return (
        visible if source == "registration" else hidden,
        visible if source == "aap" else hidden,
        visible if source == "copsin" else hidden,
    )


@app.callback(
    Output("ni-fiscal-year", "options"),
    Output("ni-fiscal-year", "value"),
    Input("dataset-id", "data"),
)
def update_national_inventory_years(dataset_id: str | None):
    try:
        frame = load_dataset(dataset_id)
    except (FileNotFoundError, ValueError):
        return [{"label": "Choose a fiscal year", "value": ""}], ""
    year_column = next(
        (
            name
            for name in ("nomination_fiscal_year", "current_nomination.fiscal_year", "Fiscal Year")
            if name in frame
        ),
        "",
    )
    years = (
        sorted(value for value in frame[year_column].dropna().astype(str).str.strip().unique() if value)
        if year_column
        else []
    )
    options = [{"label": year, "value": year} for year in years]
    if not options:
        options = [{"label": "Choose a fiscal year", "value": ""}]
    return options, years[-1] if years else ""


@app.callback(
    Output("ni-cohort", "data"),
    Output("ni-build-status", "children"),
    Input("ni-build", "n_clicks"),
    State("ni-source", "value"),
    State("ni-fiscal-year", "value"),
    State("dataset-id", "data"),
    State("ni-aap-upload", "contents"),
    State("ni-aap-upload", "filename"),
    State("ni-copsin-upload", "contents"),
    State("ni-copsin-upload", "filename"),
    prevent_initial_call=True,
)
def build_national_inventory_cohort(
    _: int,
    source: str,
    fiscal_year: str,
    dataset_id: str | None,
    aap_contents: list[str] | None,
    aap_filenames: list[str] | None,
    copsin_contents: str | None,
    copsin_filename: str | None,
):
    try:
        if source == "registration":
            if not fiscal_year:
                raise ValueError("Choose a fiscal year.")
            cohort = registration_cohort(load_dataset(dataset_id), fiscal_year)
            source_label = "registration data"
        elif source == "aap":
            cohort = uploaded_cohort(aap_contents or [], aap_filenames or [])
            source_label = f"{len(aap_contents or [])} AAP file(s)"
        elif source == "copsin":
            if not copsin_contents:
                raise ValueError("Upload a COPSIN CSV or Excel file.")
            cohort = uploaded_cohort([copsin_contents], [copsin_filename or "COPSIN upload"])
            source_label = copsin_filename or "COPSIN upload"
        else:
            raise ValueError("Choose an athlete-list source.")
    except (FileNotFoundError, ValueError) as exc:
        return no_update, html.Div(str(exc), className="status error")
    if cohort.empty:
        return no_update, html.Div("No valid Name and Sport rows were found.", className="status error")
    return cohort.to_dict("records"), html.Div(
        f"Built {len(cohort):,} unique athlete–sport rows from {source_label}.",
        className="status success",
    )


@app.callback(
    Output("ni-lou-sports", "options"),
    Output("ni-lou-sports", "value"),
    Input("ni-cohort", "data"),
)
def update_national_inventory_lou_sports(records: list[dict] | None):
    if not records:
        return [], []
    cohort = pd.DataFrame(records)
    sports = lou_sports_in(cohort)
    return [{"label": sport, "value": sport} for sport in sports], sports


@app.callback(
    Output("ni-service-matrix", "children"),
    Input("ni-lou-sports", "value"),
)
def render_national_inventory_services(sports: list[str] | None):
    if not sports:
        return html.Div("Select one or more LOU sports before assigning services.", className="empty-state")
    rows = []
    for sport in sports:
        rows.append(
            html.Div(
                [
                    html.Div(sport, className="label"),
                    dcc.Checklist(
                        id={"type": "ni-services", "sport": sport},
                        options=[{"label": service, "value": service} for service in NATIONAL_INVENTORY_SERVICES],
                        value=[],
                        inline=True,
                        inputStyle={"marginLeft": "12px", "marginRight": "5px"},
                    ),
                ],
                className="report-summary",
            )
        )
    return rows


@app.callback(
    Output("ni-preview", "data"),
    Output("ni-preview-status", "children"),
    Output("ni-download", "disabled"),
    Input("ni-cohort", "data"),
    Input("ni-lou-sports", "value"),
    Input({"type": "ni-services", "sport": ALL}, "value"),
    State({"type": "ni-services", "sport": ALL}, "id"),
    Input("dataset-id", "data"),
    Input("ni-fiscal-year", "value"),
)
def preview_national_inventory(
    records: list[dict] | None,
    selected_lou_sports: list[str] | None,
    service_values: list[list[str]],
    service_ids: list[dict],
    dataset_id: str | None,
    fiscal_year: str,
):
    if not records:
        return [], "", True
    services_by_sport = {
        item["sport"]: values for item, values in zip(service_ids or [], service_values or [])
        if item["sport"] in (selected_lou_sports or [])
    }
    try:
        registration = load_dataset(dataset_id)
        registration_note = "Registration matches use the fetched registration data."
    except (FileNotFoundError, ValueError):
        registration = pd.DataFrame()
        registration_note = "No registration data has been fetched; Registration is blank."
    report = build_service_report(
        pd.DataFrame(records), services_by_sport, registration, fiscal_year or ""
    )
    return report.to_dict("records"), html.Div(
        f"{len(report):,} athlete–sport rows. {registration_note}",
        className="status success" if not registration.empty else "status warning",
    ), report.empty


@app.callback(
    Output("ni-file", "data"),
    Input("ni-download", "n_clicks"),
    State("ni-preview", "data"),
    State("ni-fiscal-year", "value"),
    prevent_initial_call=True,
)
def download_national_inventory_service_report(
    _: int, records: list[dict] | None, fiscal_year: str
):
    if not records:
        return no_update
    report = pd.DataFrame(records).reindex(columns=NATIONAL_INVENTORY_COLUMNS, fill_value="")
    year_label = re.sub(r"[^A-Za-z0-9_-]+", "_", fiscal_year or "all_years").strip("_")
    return dcc.send_data_frame(
        report.to_csv,
        f"national_inventory_{year_label}.csv",
        index=False,
    )


@app.callback(
    Output("birth-campus-dd", "disabled"),
    Output("current-campus-dd", "disabled"),
    Output("enrollment-dd", "disabled"),
    Output("nomination-dd", "disabled"),
    Output("column-select", "disabled"),
    Output("customize-preview", "options"),
    Output("fiscal-year-dd", "disabled"),
    Output("clear-filters", "disabled"),
    Output("export-select-all", "disabled"),
    Output("export-clear", "disabled"),
    Output("export-reset", "disabled"),
    Output(EXPORT_FILTER_ALL, "disabled"),
    Input("dataset-id", "data"),
)
def set_controls_ready(dataset_id: str | None):
    disabled = not bool(dataset_id)
    preview_option = [{"label": "Customize preview columns", "value": "custom", "disabled": disabled}]
    return (
        disabled, disabled, disabled, disabled, disabled, preview_option,
        disabled, disabled, disabled, disabled, disabled,
        [disabled] * len(EXPORT_LIST_FILTERS),
    )


@app.callback(
    Output(EXPORT_FILTER_ALL, "options"),
    Output(EXPORT_FILTER_ALL, "value"),
    Input("dataset-id", "data"),
)
def update_export_filter_options(dataset_id: str | None):
    """Fill the Role, Sport, fiscal year, ... filters from the fetched data (and clear them on a new fetch)."""
    try:
        frame = load_dataset(dataset_id)
    except (FileNotFoundError, ValueError):
        frame = pd.DataFrame()
    return (
        [export_filter_options(frame, field_name) for field_name, _ in EXPORT_LIST_FILTERS],
        [[] for _ in EXPORT_LIST_FILTERS],
    )


@app.callback(
    Output("column-select", "value"),
    Input("export-select-all", "n_clicks"),
    Input("export-clear", "n_clicks"),
    Input("export-reset", "n_clicks"),
    State("column-select", "value"),
    prevent_initial_call=True,
)
def update_export_columns(_: int, __: int, ___: int, current: list[str]):
    triggered = callback_context.triggered_id
    if triggered == "export-select-all":
        return [field for field, _, _ in EXPORT_COLUMNS]
    if triggered == "export-clear":
        return []
    if triggered == "export-reset":
        return DEFAULT_FIELDS
    return current


@app.callback(
    Output("preview-column-select", "value"),
    Input("preview-select-all", "n_clicks"),
    Input("preview-clear", "n_clicks"),
    Input("preview-reset", "n_clicks"),
    State("preview-column-select", "value"),
    prevent_initial_call=True,
)
def update_preview_columns(_: int, __: int, ___: int, current: list[str]):
    triggered = callback_context.triggered_id
    if triggered == "preview-select-all":
        return [field for field, _, _ in EXPORT_COLUMNS]
    if triggered == "preview-clear":
        return []
    if triggered == "preview-reset":
        return PREVIEW_FIELDS
    return current


@app.callback(
    Output("preview-column-select", "disabled"),
    Output("preview-controls", "style"),
    Input("customize-preview", "value"),
    Input("dataset-id", "data"),
)
def configure_preview_columns(customize: list[str], dataset_id: str | None):
    enabled = bool(dataset_id) and "custom" in (customize or [])
    return not enabled, ({"display": "flex"} if enabled else {"display": "none"})


@app.callback(
    Output("birth-campus-dd", "value", allow_duplicate=True),
    Output("current-campus-dd", "value", allow_duplicate=True),
    Output("enrollment-dd", "value", allow_duplicate=True),
    Output("nomination-dd", "value", allow_duplicate=True),
    Output(EXPORT_FILTER_ALL, "value", allow_duplicate=True),
    Input("clear-filters", "n_clicks"),
    prevent_initial_call=True,
)
def clear_export_filters(_: int):
    return "", "", [], [], [[] for _ in EXPORT_LIST_FILTERS]


@app.callback(
    Output("selected-report-summary", "children"),
    Input("prebuilt-report", "value"),
)
def describe_prebuilt_report(report_key: str):
    purpose, output_format, rules = REPORT_DESCRIPTIONS.get(
        report_key, ("Uses a fixed report definition.", "Download", "See the report dictionary")
    )
    return html.Div(
        [
            html.Div(purpose),
            html.Div([html.Strong("Output: "), output_format]),
            html.Div([html.Strong("Applied rules: "), rules]),
            html.Div([html.Strong("Columns: "), REPORT_OUTPUT_COLUMNS.get(report_key, "Fixed report schema")]),
        ],
        className="report-summary",
    )


def fetch_progress_message(job: dict[str, Any]) -> str:
    label = FETCH_SOURCE_NOUNS.get(job["selection"]["source"], "rows")
    note = " (API busy, retrying…)" if job["attempt"] else ""
    return (
        f"Fetching {label}… {len(job['rows']):,} rows from {job['pages']} API pages so far{note}. "
        "Large fetches run in steps; keep this tab open."
    )


@app.callback(
    Output("fetch-job", "data"),
    Output("fetch-status", "children", allow_duplicate=True),
    Output("fetch-status", "className", allow_duplicate=True),
    Output("fetch-interval", "n_intervals"),
    Output("fetch-interval", "max_intervals"),
    Output("fetch-interval", "disabled"),
    Output("fetch-button", "disabled"),
    Input("fetch-button", "n_clicks"),
    State("campus-dd", "value"),
    State("role-dd", "value"),
    State("fetch-source", "value"),
    prevent_initial_call=True,
)
def handle_fetch(_: int, campus_value: int | str, role_value: str, source: str):
    """Check sign-in and start a segmented fetch; poll_fetch fetches the pages."""
    def stop(message: str):
        return no_update, message, "status error", no_update, no_update, True, False

    try:
        token = auth.get_token()
    except Exception as exc:
        return stop(f"Authentication failed: {type(exc).__name__}. Please sign in again.")
    if not token:
        return stop("No access token. Please sign in again.")
    actor_error = check_acting_profile(token)
    if actor_error:
        return stop(actor_error)
    job = start_fetch_job(token, campus_value, role_value, source)
    return job["id"], fetch_progress_message(job), "status", 0, 1, False, True


@app.callback(
    Output("dataset-id", "data"),
    Output("fetch-status", "children"),
    Output("fetch-status", "className"),
    Output("fetched-count", "children"),
    Output("last-refreshed", "children"),
    Output("enrollment-dd", "options"),
    Output("enrollment-dd", "value"),
    Output("nomination-dd", "options"),
    Output("nomination-dd", "value"),
    Output("fetch-interval", "max_intervals", allow_duplicate=True),
    Output("fetch-interval", "disabled", allow_duplicate=True),
    Output("fetch-button", "disabled", allow_duplicate=True),
    Input("fetch-interval", "n_intervals"),
    State("fetch-job", "data"),
    prevent_initial_call=True,
)
def poll_fetch(n_intervals: int, job_id: str | None):
    """One fetch step: fetch pages for up to FETCH_STEP_SECONDS, then report progress or the result."""
    done = (True, False)  # interval disabled, button enabled
    keep = (no_update,) * 7

    def stop(message: str, count: str = "—"):
        return (no_update, message, "status error", count, "—", [], [], [], [], no_update, *done)

    try:
        job = load_job(job_id)
    except (FileNotFoundError, ValueError) as exc:
        return stop(str(exc))

    if job["status"] == "running":
        try:
            token = auth.get_token()
        except Exception as exc:
            job["status"] = "failed"
            job["errors"].append(f"Authentication failed: {type(exc).__name__}. Please sign in again.")
            token = None
        if job["status"] == "running" and not token:
            job["status"] = "failed"
            job["errors"].append("No access token. Please sign in again.")
        if job["status"] == "running":
            advance_fetch_job(job, {"Authorization": f"Bearer {token}"})
        save_job(job)
        if job["status"] == "running":
            return (no_update, fetch_progress_message(job), "status", *keep[:6], n_intervals + 1, False, True)

    # Finished (done or failed). Saving the result in the job makes this step safe to repeat.
    if job["result"] is None:
        frame = finish_fetch_job(job)
        job["result"] = {
            "dataset_id": save_dataset(frame) if not frame.empty else None,
            "rows": len(frame),
            "refreshed": datetime.now(timezone.utc).astimezone().strftime("%b %d, %Y %H:%M"),
            "warnings": blank_field_warnings(frame) if not frame.empty else [],
            "enrollment": options_for_any(frame, ("enrollment_status", "current_enrollment.admin_status")),
            "nomination": options_for_any(frame, ("nomination_claimed", "current_nomination.redeemed")),
        }
        job["rows"] = []  # the dataset file holds them now
        save_job(job)

    result, errors, pages = job["result"], job["errors"], job["pages"]
    source = job["selection"]["source"]
    if not result["dataset_id"]:
        label = FETCH_SOURCE_NOUNS.get(source, "profiles")
        message = " ".join(errors) if errors else f"No {label} matched the selected filters."
        if not errors and API_VERSION == "v2":
            message += (
                " In Registration API v2 an empty result can also mean your acting profile "
                "lacks permissions for this data."
            )
        return stop(message, "0")

    noun = FETCH_SOURCE_NOUNS.get(source, "rows")
    if errors:
        message = (
            f"Partial result: saved {result['rows']:,} {noun} from {pages} API pages. "
            + " ".join(errors)
            + " Export only if the missing data is acceptable."
        )
        status_class = "status error"
    else:
        message = (
            f"Fetched {result['rows']:,} {noun} successfully across "
            f"{pages} API pages (Registration API {API_VERSION})."
        )
        status_class = "status"
    if result["warnings"]:
        message = [
            html.Div(message),
            html.Div(
                "Some fields came back empty, which usually means a missing permission rather than "
                "missing data: " + " ".join(result["warnings"]),
                className="warning",
            ),
        ]
    return (
        result["dataset_id"],
        message,
        status_class,
        f"{result['rows']:,}",
        result["refreshed"],
        result["enrollment"],
        [],
        result["nomination"],
        [],
        no_update,
        *done,
    )


@app.callback(
    Output("fetch-scope", "children"),
    Input("campus-dd", "value"),
    Input("role-dd", "value"),
    Input("fetch-source", "value"),
)
def describe_scope(campus_value: int | str, role_value: str, source: str) -> str:
    campus = CAMPUS_LABELS.get(campus_value, "all campuses")
    role = role_value or "all roles"
    label = FETCH_SOURCE_NOUNS.get(source, "profiles")
    return f"Will request {label} for {role} from {campus}."


@app.callback(
    Output("preview-table", "data"),
    Output("preview-table", "columns"),
    Output("filtered-count", "children"),
    Output("download-summary", "children"),
    Output("download-curated", "disabled"),
    Output("download-full", "disabled"),
    Output("active-filter-text", "children"),
    Output("preview-empty", "children"),
    Output("preview-empty", "style"),
    Output("preview-table", "style_table"),
    Output("preview-table-note", "children"),
    Output("export-column-count", "children"),
    Output("preview-column-count", "children"),
    Input("dataset-id", "data"),
    Input("birth-campus-dd", "value"),
    Input("current-campus-dd", "value"),
    Input("enrollment-dd", "value"),
    Input("nomination-dd", "value"),
    Input("column-select", "value"),
    Input("preview-column-select", "value"),
    Input("customize-preview", "value"),
    Input(EXPORT_FILTER_ALL, "value"),
    State(EXPORT_FILTER_ALL, "id"),
)
def update_review(
    dataset_id: str | None,
    birth_campus: str,
    current_campus: str,
    enrollment_statuses: list[str],
    nomination_claimed: list[str],
    selected_fields: list[str],
    selected_preview_fields: list[str],
    customize_preview: list[str],
    list_filter_values: list[Any] | None = None,
    list_filter_ids: list[dict[str, str]] | None = None,
):
    list_filters = selected_list_filters(list_filter_values, list_filter_ids)
    try:
        frame = load_dataset(dataset_id)
    except (FileNotFoundError, ValueError):
        return (
            [], [], "—", "Fetch profiles to enable downloads.", True, True,
            "No data fetched", "Fetch profiles to enable filtering, preview, and downloads.",
            {"display": "block"}, {"display": "none"}, "",
            f"{len(selected_fields or [])} columns selected", "",
        )
    filtered = apply_filters(
        frame,
        birth_campus,
        current_campus,
        enrollment_statuses,
        nomination_claimed,
        list_filters,
    )
    preview_fields = (
        selected_preview_fields
        if "custom" in (customize_preview or [])
        else selected_fields
    )
    preview = build_export(filtered.head(100), preview_fields)
    selected_count = len(build_export(frame.head(0), selected_fields).columns)
    preview_count = len(preview.columns)
    active_filters = []
    if birth_campus:
        active_filters.append(f"Birth campus: {birth_campus}")
    if current_campus:
        active_filters.append(f"Current campus: {current_campus}")
    if enrollment_statuses:
        active_filters.append(f"Enrollment: {', '.join(enrollment_statuses)}")
    if nomination_claimed:
        active_filters.append(f"Nomination claimed: {', '.join(nomination_claimed)}")
    for field_name, values in list_filters.items():
        active_filters.append(f"{EXPORT_LIST_FILTER_LABELS.get(field_name, field_name)}: {', '.join(values)}")
    filter_text = f"{len(filtered):,} of {len(frame):,} profiles"
    if active_filters:
        filter_text += " · " + " · ".join(active_filters)
    has_preview = not filtered.empty and preview_count > 0
    empty_message = (
        "No profiles match the current filters. Clear or change a filter to continue."
        if filtered.empty
        else "Choose at least one preview column to display the data."
    )
    table_style = (
        {"overflowX": "auto", "maxHeight": "560px", "overflowY": "auto"}
        if has_preview else {"display": "none"}
    )
    summary = (
        f"The curated CSV will contain {len(filtered):,} rows and {selected_count} selected columns. "
        "The complete download contains every raw field returned for the fetched scope."
    )
    return (
        preview.to_dict("records"),
        [{"name": column, "id": column} for column in preview.columns],
        f"{len(filtered):,}",
        summary,
        filtered.empty or selected_count == 0,
        frame.empty,
        filter_text,
        empty_message,
        {"display": "none"} if has_preview else {"display": "block"},
        table_style,
        f"Previewing the first {min(len(filtered), 100):,} matching records. Scroll horizontally to see additional columns." if has_preview else "",
        f"{selected_count} available columns selected",
        f"{preview_count} available columns shown",
    )


@app.callback(
    Output("curated-file", "data"),
    Output("full-file", "data"),
    Output("download-error", "children"),
    Input("download-curated", "n_clicks"),
    Input("download-full", "n_clicks"),
    State("dataset-id", "data"),
    State("birth-campus-dd", "value"),
    State("current-campus-dd", "value"),
    State("enrollment-dd", "value"),
    State("nomination-dd", "value"),
    State("column-select", "value"),
    State(EXPORT_FILTER_ALL, "value"),
    State(EXPORT_FILTER_ALL, "id"),
    prevent_initial_call=True,
)
def download_data(
    _: int,
    __: int,
    dataset_id: str | None,
    birth_campus: str,
    current_campus: str,
    enrollment_statuses: list[str],
    nomination_claimed: list[str],
    selected_fields: list[str],
    list_filter_values: list[Any] | None = None,
    list_filter_ids: list[dict[str, str]] | None = None,
):
    try:
        frame = load_dataset(dataset_id)
    except (FileNotFoundError, ValueError) as exc:
        return no_update, no_update, html.Div(str(exc), className="status error")

    stamp = datetime.now().strftime("%Y%m%d_%H%M")
    triggered = callback_context.triggered_id
    if triggered == "download-full":
        return (
            no_update,
            dcc.send_data_frame(
                frame.to_csv, f"list_generator_7000_all_fields_{stamp}.csv", index=False
            ),
            "",
        )

    filtered = apply_filters(
        frame,
        birth_campus,
        current_campus,
        enrollment_statuses,
        nomination_claimed,
        selected_list_filters(list_filter_values, list_filter_ids),
    )
    exported = build_export(filtered, selected_fields)
    if exported.empty or not len(exported.columns):
        return no_update, no_update, html.Div("No rows or columns to export.", className="status error")
    return (
        dcc.send_data_frame(
            exported.to_csv, f"list_generator_7000_curated_{stamp}.csv", index=False
        ),
        no_update,
        "",
    )


@app.callback(
    Output("lou-sports-block", "style"),
    Output("enhanced-sports-block", "style"),
    Input("prebuilt-report", "value"),
)
def show_prebuilt_sport_selector(report_key: str):
    visible = {"display": "block"}
    hidden = {"display": "none"}
    return (
        visible if report_key == "viasport_lou" else hidden,
        visible if report_key == "enhanced_excellence" else hidden,
    )


@app.callback(
    Output("fiscal-year-dd", "options"),
    Output("fiscal-year-dd", "value"),
    Output("lou-sports-dd", "options"),
    Output("lou-sports-dd", "value"),
    Output("enhanced-sports-dd", "options"),
    Output("enhanced-sports-dd", "value"),
    Output("download-prebuilt", "disabled"),
    Input("dataset-id", "data"),
)
def update_prebuilt_controls(dataset_id: str | None):
    empty_result = (
        [{"label": "All fiscal years", "value": ""}],
        "",
        [],
        [],
        [],
        [],
        True,
    )
    try:
        frame = load_dataset(dataset_id)
    except (FileNotFoundError, ValueError):
        return empty_result
    fiscal_column = next(
        (
            name
            for name in ("nomination_fiscal_year", "fiscal_year", "Fiscal Year")
            if name in frame
        ),
        "",
    )
    years = (
        sorted(
            value
            for value in frame[fiscal_column].dropna().astype(str).str.strip().unique()
            if value
        )
        if fiscal_column
        else []
    )
    fiscal_options = [{"label": "All fiscal years", "value": ""}] + [
        {"label": year, "value": year} for year in years
    ]
    sports = sport_values(frame)
    sport_options = [{"label": sport, "value": sport} for sport in sports]
    return (
        fiscal_options,
        years[-1] if years else "",
        sport_options,
        default_sports(sports, "lou"),
        sport_options,
        default_sports(sports, "enhanced"),
        frame.empty,
    )


@app.callback(
    Output("prebuilt-file", "data"),
    Output("prebuilt-status", "children"),
    Input("download-prebuilt", "n_clicks"),
    State("dataset-id", "data"),
    State("prebuilt-report", "value"),
    State("fiscal-year-dd", "value"),
    State("lou-sports-dd", "value"),
    State("enhanced-sports-dd", "value"),
    prevent_initial_call=True,
)
def download_prebuilt_report(
    _: int,
    dataset_id: str | None,
    report_key: str,
    fiscal_year_value: str,
    lou_sports: list[str],
    enhanced_sports: list[str],
):
    try:
        frame = load_dataset(dataset_id)
        artifact = build_report(
            frame,
            report_key,
            fiscal_year_value,
            lou_sports,
            enhanced_sports,
        )
    except (FileNotFoundError, ValueError) as exc:
        return no_update, html.Div(str(exc), className="status error")
    if artifact.row_count == 0:
        return no_update, html.Div(
            "The selected report has no matching rows. Check its dictionary rules "
            "and the fetched data.",
            className="status error",
        )
    return (
        dcc.send_bytes(artifact.content, artifact.filename),
        html.Div(
            f"Prepared {artifact.filename} with {artifact.row_count:,} matching rows.",
            className="status",
        ),
    )




if __name__ == "__main__":
    missing = [
        variable
        for variable, value in (
            ("CLIENT_ID", CLIENT_ID),
            ("CLIENT_SECRET", CLIENT_SECRET),
            ("FLASK_SECRET_KEY", os.environ.get("FLASK_SECRET_KEY")),
        )
        if not value
    ]
    if missing:
        raise SystemExit(f"Missing required environment variables: {', '.join(missing)}")
    local_port = urlsplit(APP_URL).port or 8050
    app.run(
        host=urlsplit(APP_URL).hostname or "127.0.0.1",
        port=local_port,
    )
