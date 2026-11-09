#!/usr/bin/env python3
"""List Generator 7000.

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
from dotenv import load_dotenv
from requests.exceptions import ConnectionError, ConnectTimeout, ReadTimeout
from prebuilt_reports_7000 import (
    EE_FISCAL_MODE_LABELS,
    EE_TEMPLATE_GUIDE,
    REPORT_GUIDE,
    REPORT_OPTIONS,
    CAC_TEMPLATE_COLUMNS,
    available_fiscal_years,
    build_ee_athlete_list,
    build_ee_mail_merge,
    build_ee_sport_templates,
    build_report,
    default_ee_fiscal_mode,
    default_sports,
    ee_cac_matches,
    ee_mail_merge,
    ee_registered_athletes,
    ee_review_rows,
    ee_shared_nccp_coaches,
    ee_workbook_groups,
    parse_cac_report,
    plan_ee_workbooks,
    previous_fiscal_year,
    sport_values,
)
from conversion_update_workflow import (
    ACTION_ADD,
    ACTION_CSS_ONLY,
    ACTION_REVIEW,
    ACTION_UP_TO_DATE,
    ACTION_UPGRADE,
    CONVERSION_GUIDE,
    DECISIONS,
    GitHubSource,
    HOLD,
    REVIEW_COLUMNS,
    apply_update,
    build_update_zip,
    commit_message,
    dataset_summary,
    fetch_conversion_from_github,
    match_nominations,
    nomination_fiscal_years,
    plan_counts,
    prepare_nominations,
    empty_decisions,
    fetch_decisions_from_github,
    is_decisions_csv,
    merge_decisions,
    parse_decisions_csv,
    push_conversion_to_github,
    read_conversion_csv,
    review_rows,
    row_kinds,
    STATUS_SAVED_DIFFERENT,
    STATUS_SAVED_SAME,
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
        Path(tempfile.gettempdir()) / "list_generator_7000_cache",
    )
)
CACHE_TTL_SECONDS = int(os.environ.get("LIST_GENERATOR_CACHE_TTL_SECONDS", "7200"))

PAGE_LIMIT = 100
MAX_RETRIES = 5
BACKOFF_SECONDS = 1.5
REQUEST_TIMEOUT = (10, 90)
RETRYABLE_STATUSES = {429, 502, 503, 504, 524}

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
    {"label": "Profiles", "value": "profile"},
    {"label": "Nominations", "value": "nomination"},
    {"label": "Profiles + Nominations", "value": "all"},
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

# Where the Conversion Update tab loads the conversion dataset from (public repo: no token needed).
CONVERSION_GITHUB = GitHubSource(
    repo=os.environ.get("CONVERSION_GITHUB_REPO", "LukeSpagnuolo/Conversion"),
    branch=os.environ.get("CONVERSION_GITHUB_BRANCH", "main"),
    path=os.environ.get("CONVERSION_GITHUB_PATH", "Conversion_Data_2026_final.csv"),
    token=os.environ.get("GITHUB_TOKEN", ""),
)


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
) -> pd.DataFrame:
    result = frame
    filters = (
        (("birth_city_campus", "campus_by_birth"), birth_campus),
        (("residence_city_campus", "current_campus"), current_campus),
    )
    for candidates, value in filters:
        column_name = next((name for name in candidates if name in result), "")
        if value and column_name:
            result = result[result[column_name] == value]
    list_filters = (
        (("enrollment_status", "current_enrollment.admin_status"), enrollment_statuses),
        (("nomination_claimed", "current_nomination.redeemed"), nomination_claimed),
    )
    for candidates, values in list_filters:
        column_name = next((name for name in candidates if name in result), "")
        if values and column_name:
            result = result[result[column_name].astype(str).isin(values)]
    return remove_test_sports(result)


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
    records = [
        flatten_report_row(row, definitions)
        if isinstance(row.get("cells"), dict) or isinstance(row.get("meta"), dict)
        else flatten_json(row)
        for row in result.rows
    ]
    return pd.DataFrame(records), errors, result.pages


def fetch_profiles_for_selection(
    token: str, campus_value: int | str, role_value: str, source: str = "profile"
) -> tuple[pd.DataFrame, list[str], int]:
    frame, errors, pages = fetch_report_frame(token)
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
    return frame, errors, pages


def card(title: str, value_id: str) -> html.Div:
    return html.Div(
        [html.Div(title, className="metric-title"), html.Div("—", id=value_id, className="metric-value")],
        className="metric-card",
    )


auth = DashAuthExternal(
    AUTH_URL,
    TOKEN_URL,
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    app_url=APP_URL,
    _secret_key=os.environ.get("FLASK_SECRET_KEY"),
)
server = auth.server


@server.route("/api-schema")
def report_rows_schema():
    """Show the Registration API's report-rows and profile parameters (read-only diagnostic)."""
    try:
        token = auth.get_token()
    except Exception as exc:
        return f"Sign in to the app first ({type(exc).__name__}).", 401
    response = requests.get(
        SCHEMA_URL,
        params={"format": "json"},
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        timeout=REQUEST_TIMEOUT,
    )
    if response.status_code in {401, 403}:
        return (
            "The schema was refused. It needs an account with a usable password and the "
            "API Documentation Viewer role at this institution. Data access is not affected.",
            response.status_code,
        )
    if response.status_code != 200:
        return f"Schema request failed: {api_error_message(response)}", 502
    try:
        schema = response.json()
    except ValueError:
        return "The API returned a non-JSON schema.", 502
    paths = {
        path: spec
        for path, spec in (schema.get("paths") or {}).items()
        if "report-" in path or "/profile/" in path
    }
    summary = {"paths": paths, "components": schema.get("components", {}).get("parameters", {})}
    (APP_DIR / "report_rows_schema.json").write_text(json.dumps(summary, indent=2, default=str))
    return server.response_class(json.dumps(summary, indent=2, default=str), mimetype="application/json")


def summarize_report_frame(frame: pd.DataFrame, errors: list[str], pages: int) -> dict[str, Any]:
    """Counts and column fill rates for one API version's report rows. No personal values."""
    def counts(name: str) -> dict[str, int]:
        if name not in frame:
            return {}
        return {str(key): int(value) for key, value in frame[name].fillna("").astype(str).value_counts().items()}

    filled = {
        name: int((~frame[name].fillna("").astype(str).str.strip().isin({"", "nan", "None", "NaT"})).sum())
        for name in frame.columns
    }
    ids = {
        str(value)
        for value in frame.get("row_id", pd.Series(dtype=object)).dropna().astype(str)
        if value.strip()
    }
    return {
        "rows": int(len(frame)),
        "pages": pages,
        "complete": not errors,
        "errors": errors,
        "row_kinds": counts("row_kind"),
        "roles": counts("role"),
        "fiscal_years": counts("nomination_fiscal_year"),
        "columns": sorted(frame.columns),
        "filled": filled,
        "row_ids": ids,
    }


def compare_report_summaries(v1: dict[str, Any], v2: dict[str, Any]) -> dict[str, Any]:
    """Differences between v1 and v2 summaries that matter for acceptance."""
    columns = sorted(set(v1["columns"]) | set(v2["columns"]))
    fill_changes = {
        name: {"v1": v1["filled"].get(name, 0), "v2": v2["filled"].get(name, 0)}
        for name in columns
        if v1["filled"].get(name, 0) != v2["filled"].get(name, 0)
    }
    return {
        "rows": {"v1": v1["rows"], "v2": v2["rows"]},
        "row_kinds": {"v1": v1["row_kinds"], "v2": v2["row_kinds"]},
        "roles": {"v1": v1["roles"], "v2": v2["roles"]},
        "fiscal_years": {"v1": v1["fiscal_years"], "v2": v2["fiscal_years"]},
        "rows_only_in_v1": len(v1["row_ids"] - v2["row_ids"]),
        "rows_only_in_v2": len(v2["row_ids"] - v1["row_ids"]),
        "columns_only_in_v1": sorted(set(v1["columns"]) - set(v2["columns"])),
        "columns_only_in_v2": sorted(set(v2["columns"]) - set(v1["columns"])),
        "fill_changes": fill_changes,
        "errors": {"v1": v1["errors"], "v2": v2["errors"]},
        "matches": (
            v1["rows"] == v2["rows"]
            and v1["row_ids"] == v2["row_ids"]
            and set(v1["columns"]) == set(v2["columns"])
            and not fill_changes
            and not v1["errors"]
            and not v2["errors"]
        ),
    }


SCOPES_URL = os.environ.get("SCOPES_URL", f"{SITE}/api/registration/v2/scopes/")
PROBE_NOMINATION_STATUSES = ("active", "expired", "inactive", "pending", "Active", "Expired", "EXPIRED")
PROBE_COLUMNS = "row_kind,role,nomination_fiscal_year,nomination_status"


def probe_report_rows(
    token: str, params: dict[str, Any], *, session: requests.Session | None = None
) -> dict[str, Any]:
    """Count v2 report rows for one filter and describe the first page (no personal fields)."""
    client = session or requests
    url = with_query(
        f"{SITE}/api/registration/v2/report-rows/", limit=100, columns=PROBE_COLUMNS, **params
    )
    try:
        response = client.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=REQUEST_TIMEOUT)
    except requests.RequestException as exc:
        return {"params": params, "error": type(exc).__name__}
    if response.status_code != 200:
        return {"params": params, "http": response.status_code, "error": api_error_message(response)}
    try:
        payload = response.json()
    except ValueError:
        return {"params": params, "error": "non-JSON response"}
    rows = payload.get("results") if isinstance(payload, dict) else None
    rows = rows if isinstance(rows, list) else []

    def first_page(key: str) -> dict[str, int]:
        values = [
            str((row.get("cells") or {}).get(key, row.get(key, "")) or "(blank)")
            for row in rows
            if isinstance(row, dict)
        ]
        return {value: values.count(value) for value in sorted(set(values))}

    return {
        "params": params,
        "count": payload.get("count") if isinstance(payload, dict) else None,
        "first_page_rows": len(rows),
        "first_page_fiscal_years": first_page("nomination_fiscal_year"),
        "first_page_nomination_statuses": first_page("nomination_status"),
        "first_page_row_kinds": {
            str(kind): sum(1 for row in rows if isinstance(row, dict) and row.get("row_kind") == kind)
            for kind in {row.get("row_kind") for row in rows if isinstance(row, dict)}
        },
    }


def scope_options(payload: Any) -> dict[str, Any]:
    """IDs and display names from the v2 scopes response, dropping any other detail."""
    if not isinstance(payload, dict):
        return {}
    options = {}
    for key, value in payload.items():
        if isinstance(value, list):
            options[key] = [
                {
                    name: item.get(name)
                    for name in ("id", "name", "label", "slug", "kind", "type")
                    if isinstance(item, dict) and name in item
                }
                for item in value
            ]
    return options


def actor_context(payload: Any) -> dict[str, Any]:
    """The acting profile's ID and institution/site fields from csiauth me, without names or emails."""
    if not isinstance(payload, dict):
        return {}
    current = payload.get("current_profile") if isinstance(payload.get("current_profile"), dict) else {}
    keep = {}
    for source_name, source in (("top_level", payload), ("current_profile", current)):
        keep[source_name] = {
            key: value
            for key, value in source.items()
            if key == "id" or any(word in key for word in ("institution", "site", "campus", "role", "group"))
        }
        keep[f"{source_name}_keys"] = sorted(source)
    return keep


V2_CHECK_BREAKDOWN_COLUMNS = (
    "role", "enrollment_status", "residence_city_campus", "sport", "nomination_fiscal_year",
    "nomination_status",
)


def explain_row_differences(v1_frame: pd.DataFrame, v2_frame: pd.DataFrame) -> dict[str, Any]:
    """Describe rows only in v1 and compare field fill on shared rows, using counts only."""
    if "row_id" not in v1_frame or "row_id" not in v2_frame:
        return {}
    v1_ids = v1_frame["row_id"].astype(str)
    v2_ids = v2_frame["row_id"].astype(str)
    only_v1 = v1_frame[~v1_ids.isin(set(v2_ids))]
    breakdown = {
        name: {
            (str(key) or "(blank)"): int(value)
            for key, value in only_v1[name].fillna("").astype(str).value_counts().head(25).items()
        }
        for name in V2_CHECK_BREAKDOWN_COLUMNS
        if name in only_v1
    }
    shared = set(v1_ids) & set(v2_ids)
    left = v1_frame[v1_ids.isin(shared)]
    right = v2_frame[v2_ids.isin(shared)]

    def filled(frame: pd.DataFrame, name: str) -> int:
        if name not in frame:
            return 0
        return int((~frame[name].fillna("").astype(str).str.strip().isin({"", "nan", "None", "NaT"})).sum())

    shared_fill_changes = {
        name: {"v1": filled(left, name), "v2": filled(right, name)}
        for name in sorted(set(left.columns) | set(right.columns))
        if filled(left, name) != filled(right, name)
    }
    return {
        "rows_only_in_v1_by": breakdown,
        "shared_rows": len(shared),
        "shared_row_fill_changes": shared_fill_changes,
    }


@server.route("/api-v2-probe")
def registration_v2_probe():
    """Test v2 report-rows filters and anchors for the signed-in user (counts and IDs only)."""
    try:
        token = auth.get_token()
    except Exception as exc:
        return f"Sign in to the app first ({type(exc).__name__}).", 401
    headers = {"Authorization": f"Bearer {token}"}
    result: dict[str, Any] = {}
    for label, url, describe in (("actor", ACTOR_URL, actor_context), ("scopes", SCOPES_URL, scope_options)):
        try:
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            result[label] = describe(response.json()) if response.status_code == 200 else {"http": response.status_code}
        except (requests.RequestException, ValueError) as exc:
            result[label] = {"error": type(exc).__name__}
    result["unfiltered"] = probe_report_rows(token, {})
    result["nomination_status"] = [
        probe_report_rows(token, {"nomination_status": value}) for value in PROBE_NOMINATION_STATUSES
    ]
    result["nomination_redeemed"] = [
        probe_report_rows(token, {"nomination_redeemed": value}) for value in ("true", "false")
    ]
    institutions = (result.get("scopes") or {}).get("institutions") or []
    result["by_institution"] = [
        {**probe_report_rows(token, {"institution_id": item["id"]}), "name": item.get("name") or item.get("label")}
        for item in institutions
        if isinstance(item, dict) and item.get("id") is not None
    ]
    result["checked_at"] = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    (APP_DIR / "registration_v2_probe.json").write_text(json.dumps(result, indent=2, default=str))
    return server.response_class(json.dumps(result, indent=2, default=str), mimetype="application/json")


def nomination_history_summary(latest: pd.DataFrame, expired: pd.DataFrame) -> dict[str, Any]:
    """Whether the EXPIRED filter exposes earlier nominations for people who have renewed.

    Compares each person's row in the normal fetch (latest nomination) with their row
    in the nomination_status=EXPIRED fetch. Counts only.
    """
    def key_frame(frame: pd.DataFrame) -> pd.DataFrame:
        columns = ["row_id", "row_kind", "role", "sport", "nomination_fiscal_year", "nomination_status", "nomination_id"]
        result = frame.reindex(columns=columns).fillna("").astype(str)
        return result[result["row_id"] != ""]

    latest_rows, expired_rows = key_frame(latest), key_frame(expired)
    joined = expired_rows.merge(
        latest_rows, on="row_id", how="left", suffixes=("_expired", "_latest"), indicator=True
    )
    in_both = joined["_merge"] == "both"
    joined = joined.drop(columns="_merge").fillna("")
    renewed = joined[
        in_both
        & joined["nomination_id_latest"].ne("")
        & joined["nomination_id_latest"].ne(joined["nomination_id_expired"])
    ]

    def counts(frame: pd.DataFrame, column: str, limit: int = 30) -> dict[str, int]:
        return {
            (str(key) or "(blank)"): int(value)
            for key, value in frame[column].value_counts().head(limit).items()
        }

    return {
        "expired_rows": int(len(expired_rows)),
        "expired_by_fiscal_year": counts(expired_rows, "nomination_fiscal_year"),
        "expired_rows_missing_from_latest_fetch": int((~in_both).sum()),
        "missing_from_latest_by_row_kind": counts(joined[~in_both], "row_kind_expired"),
        "missing_from_latest_by_fiscal_year": counts(joined[~in_both], "nomination_fiscal_year_expired"),
        "missing_from_latest_by_role": counts(joined[~in_both], "role_expired"),
        "missing_from_latest_by_sport": counts(joined[~in_both], "sport_expired"),
        "renewed_people_with_earlier_nomination": int(len(renewed)),
        "renewed_by_fiscal_years": {
            f"{expired_year} -> {latest_year}": int(value)
            for (expired_year, latest_year), value in renewed.groupby(
                ["nomination_fiscal_year_expired", "nomination_fiscal_year_latest"]
            ).size().items()
        },
        "renewed_by_latest_status": counts(renewed, "nomination_status_latest"),
        "renewed_by_role": counts(renewed, "role_expired"),
        "renewed_by_sport": counts(renewed, "sport_expired"),
    }


@server.route("/api-v2-history-check")
def registration_v2_history_check():
    """Test whether nomination_status=EXPIRED recovers previous-year nominations (counts only)."""
    try:
        token = auth.get_token()
    except Exception as exc:
        return f"Sign in to the app first ({type(exc).__name__}).", 401
    base = f"{SITE}/api/registration/v2/"
    latest, latest_errors, _ = fetch_report_frame(token, f"{base}report-rows/", f"{base}report-columns/")
    expired, expired_errors, _ = fetch_report_frame(
        token, with_query(f"{base}report-rows/", nomination_status="EXPIRED"), f"{base}report-columns/"
    )
    result = nomination_history_summary(latest, expired)
    result["latest_rows"] = int(len(latest))
    result["errors"] = {"latest": latest_errors, "expired": expired_errors}
    result["checked_at"] = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    (APP_DIR / "registration_v2_history_check.json").write_text(json.dumps(result, indent=2, default=str))
    return server.response_class(json.dumps(result, indent=2, default=str), mimetype="application/json")


@server.route("/api-v2-check")
def registration_v2_check():
    """Compare v1 and v2 report rows for the signed-in user (counts and fill rates only)."""
    try:
        token = auth.get_token()
    except Exception as exc:
        return f"Sign in to the app first ({type(exc).__name__}).", 401
    acting_error = check_acting_profile(token) if API_VERSION == "v2" else ""
    summaries = {}
    frames = {}
    for label, base in (("v1", LEGACY_REGISTRATION_API_BASE), ("v2", f"{SITE}/api/registration/v2/")):
        frame, errors, pages = fetch_report_frame(token, f"{base}report-rows/", f"{base}report-columns/")
        frames[label] = frame
        summaries[label] = summarize_report_frame(frame, errors, pages)
    comparison = compare_report_summaries(summaries["v1"], summaries["v2"])
    comparison.update(explain_row_differences(frames["v1"], frames["v2"]))
    comparison["acting_profile_problem"] = acting_error
    comparison["checked_at"] = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    (APP_DIR / "registration_v2_check.json").write_text(json.dumps(comparison, indent=2, default=str))
    return server.response_class(json.dumps(comparison, indent=2, default=str), mimetype="application/json")


app = Dash(__name__, server=server, title="List Generator 7000")

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
        dcc.Store(id="ni-cohort", storage_type="memory"),
        dcc.Store(id="ee-cac", storage_type="memory"),
        dcc.Store(id="cu-conversion", storage_type="memory"),
        dcc.Store(id="cu-plan", storage_type="memory"),
        html.Div(
            className="hero",
            children=[
                html.H1("List Generator 7000"),
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
                        value="profile",
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
                dcc.Loading(html.Div(id="fetch-status"), type="circle", color="#1676a3"),
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
                        dcc.Tab(label="EE Sport Templates", value="ee_templates"),
                        dcc.Tab(label="Conversion Update", value="conversion_update"),
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
            id="ee-workflow",
            style={"display": "none"},
            children=[
                html.Div(
                    className="panel",
                    children=[
                        html.H2("3. Choose the fiscal year and sports"),
                        html.P(
                            "Fetch with API data type “Nominations” so every fiscal year's nominations are "
                            "available. Previous and Both are calculated from this Current fiscal year, "
                            "so check it before building the templates.",
                            className="hint",
                        ),
                        html.Div(
                            className="filter-grid",
                            children=[
                                dropdown_block(
                                    "Current fiscal year",
                                    dcc.Dropdown(
                                        id="ee-current-year",
                                        options=[],
                                        value="",
                                        clearable=False,
                                        placeholder="Fetch data first",
                                    ),
                                ),
                                html.Div(
                                    dropdown_block(
                                        "Enhanced Excellence sports",
                                        dcc.Dropdown(
                                            id="ee-sports",
                                            options=[],
                                            value=[],
                                            multi=True,
                                            placeholder="Select EE sports",
                                        ),
                                    ),
                                    style={"gridColumn": "span 3"},
                                ),
                            ],
                        ),
                        html.Div(id="ee-year-note", className="hint", style={"marginTop": "10px"}),
                    ],
                ),
                html.Div(
                    className="panel",
                    children=[
                        html.H2("4. Set the fiscal years for each workbook"),
                        html.P(
                            "Defaults follow the EE sport schedule. Para sports share their parent "
                            "sport's workbook; Wheelchair Athletics, Rugby, and Tennis share one.",
                            className="hint",
                        ),
                        html.Div(
                            [
                                html.Button("Reset to schedule defaults", id="ee-reset-years", type="button"),
                                html.Span(id="ee-default-note", className="hint"),
                            ],
                            className="control-row",
                        ),
                        html.Div(id="ee-year-matrix", className="ee-sport-year-controls"),
                    ],
                ),
                html.Div(
                    className="panel",
                    children=[
                        html.H2("5. Add the CAC coach report (optional)"),
                        html.P(
                            "Coaches are matched by NCCP number to fill the CAC Locker Status columns: "
                            + "; ".join(
                                f"{header} ← {source}"
                                for header, (source, _) in CAC_TEMPLATE_COLUMNS.items()
                            )
                            + ". Without a file, those columns stay blank.",
                            className="hint",
                        ),
                        dcc.Upload(
                            id="ee-cac-upload",
                            children=html.Div(["Drag the CAC coach report here or ", html.A("choose a file")]),
                            multiple=False,
                            accept=".xlsx,.xlsm,.xls,.csv",
                            className="upload-box",
                            style={
                                "border": "1px dashed var(--border)",
                                "borderRadius": "10px",
                                "padding": "22px",
                                "textAlign": "center",
                                "cursor": "pointer",
                            },
                        ),
                        html.Div(
                            [
                                html.Button(
                                    "Remove CAC file", id="ee-cac-clear", type="button", className="secondary"
                                )
                            ],
                            className="control-row",
                        ),
                        html.Div(id="ee-cac-status"),
                    ],
                ),
                html.Div(
                    className="panel",
                    children=[
                        html.H2("6. Review and download"),
                        html.Div(id="ee-review-status"),
                        dash_table.DataTable(
                            id="ee-review",
                            columns=[
                                {"name": name, "id": name}
                                for name in (
                                    "Workbook", "Sports", "Fiscal years", "Athletes",
                                    "Registered athletes", "Coaches", "Registered coaches",
                                    "CAC matches", "Status",
                                )
                            ],
                            data=[],
                            sort_action="native",
                            style_table={"overflowX": "auto", "marginTop": "14px"},
                            style_header={"backgroundColor": "#12304a", "color": "white", "fontWeight": "700"},
                            style_cell={"textAlign": "left", "fontSize": "13px", "padding": "8px"},
                            style_data_conditional=[
                                {
                                    "if": {"filter_query": '{Status} contains "Omitted"'},
                                    "backgroundColor": "#fff8e6",
                                    "color": "#674d00",
                                },
                            ],
                        ),
                        html.Div(
                            [
                                html.Button("Download EE Sport Templates (ZIP)", id="ee-download", disabled=True),
                                html.Button(
                                    "Download registered athlete list",
                                    id="ee-athletes-download",
                                    disabled=True,
                                    className="secondary",
                                ),
                                html.Button(
                                    "Download mail merge CSVs",
                                    id="ee-mail-merge-download",
                                    disabled=True,
                                    className="secondary",
                                ),
                            ],
                            className="actions",
                        ),
                        html.Div(id="ee-download-status"),
                        dcc.Download(id="ee-file"),
                        dcc.Download(id="ee-athletes-file"),
                        dcc.Download(id="ee-mail-merge-file"),
                        html.Details(
                            [
                                html.Summary("How EE Sport Templates are built"),
                                dcc.Markdown(EE_TEMPLATE_GUIDE),
                            ],
                            open=False,
                        ),
                    ],
                ),
            ],
        ),
        html.Div(
            id="conversion-workflow",
            style={"display": "none"},
            children=[
                html.Div(
                    className="panel",
                    children=[
                        html.H2("3. Load the conversion dataset"),
                        html.P(
                            "Load the latest version from GitHub, or upload a CSV. "
                            "Nothing is changed until you download the update.",
                            className="hint",
                        ),
                        html.Div(
                            [
                                html.Button("Load latest from GitHub", id="cu-github", type="button"),
                                html.Span(CONVERSION_GITHUB.label, className="hint"),
                            ],
                            className="actions",
                            style={"marginTop": "6px", "marginBottom": "14px"},
                        ),
                        dcc.Upload(
                            id="cu-upload",
                            children=html.Div([
                                "Or drag a conversion CSV here (plus conversion_review_decisions.csv from a "
                                "downloaded ZIP, if you have it) / ",
                                html.A("choose files"),
                            ]),
                            multiple=True,
                            accept=".csv",
                            className="upload-box",
                            style={
                                "border": "1px dashed var(--border)",
                                "borderRadius": "10px",
                                "padding": "22px",
                                "textAlign": "center",
                                "cursor": "pointer",
                            },
                        ),
                        dcc.Loading(html.Div(id="cu-upload-status"), type="circle", color="#1676a3"),
                    ],
                ),
                html.Div(
                    className="panel",
                    children=[
                        html.H2("4. Choose nomination years"),
                        html.Div(id="cu-source-note"),
                        dropdown_block(
                            "Nomination fiscal years",
                            dcc.Dropdown(id="cu-years", options=[], value=[], multi=True),
                        ),
                        html.P(
                            "Years already in the dataset are safe to include: matching rows show as "
                            "already up to date, and only missing rows or higher programs change.",
                            className="hint",
                        ),
                    ],
                ),
                html.Div(
                    className="panel",
                    children=[
                        html.H2("5. Review"),
                        dcc.Loading(html.Div(id="cu-plan-status"), type="circle", color="#1676a3"),
                        html.Div(
                            className="metrics six",
                            children=[
                                card("New athletes", "cu-count-new"),
                                card("Returning athletes", "cu-count-returning"),
                                card("Program upgrades", "cu-count-upgrades"),
                                card("Already up to date", "cu-count-current"),
                                card("Needs review", "cu-count-review"),
                                card("Excluded", "cu-count-excluded"),
                            ],
                        ),
                        html.H3("Athletes to check", style={"marginTop": "22px"}),
                        html.P(
                            "Choose a Decision for each. Hold leaves the nomination out of this update "
                            "(it comes back next time); Same person files it under the possible match and "
                            "applies the nomination DOB to that athlete; Different person adds a new athlete. "
                            "Same/Different decisions are saved with the push (and in the ZIP), so you only "
                            "make them once.",
                            className="hint",
                        ),
                        dash_table.DataTable(
                            id="cu-review",
                            columns=[
                                {"name": name, "id": name, "editable": name == "Decision",
                                 **({"presentation": "dropdown"} if name == "Decision" else {})}
                                for name in REVIEW_COLUMNS
                            ],
                            data=[],
                            dropdown={"Decision": {"options": [{"label": d, "value": d} for d in DECISIONS],
                                                   "clearable": False}},
                            sort_action="native",
                            page_size=15,
                            style_table={"overflowX": "auto", "marginTop": "10px"},
                            style_header={"backgroundColor": "#12304a", "color": "white", "fontWeight": "700"},
                            style_cell={"textAlign": "left", "fontSize": "13px", "padding": "8px",
                                        "whiteSpace": "normal", "height": "auto", "maxWidth": "360px"},
                            style_data_conditional=[
                                {"if": {"column_id": "Decision"}, "backgroundColor": "#fff8e6", "minWidth": "150px"},
                            ],
                            css=[{"selector": ".Select-menu-outer", "rule": "display: block !important"}],
                        ),
                        html.Details(
                            [
                                html.Summary("All matched nominations"),
                                html.P("Filter the Action column, e.g. type Add or Upgrade.", className="hint"),
                                dash_table.DataTable(
                                    id="cu-plan-table",
                                    columns=[
                                        {"name": name, "id": name}
                                        for name in ("Action", "Status", "Year", "First Name", "Last Name", "DOB",
                                                     "Sport", "Program", "CSS", "Dataset key", "Detail")
                                    ],
                                    data=[],
                                    sort_action="native",
                                    filter_action="native",
                                    page_size=15,
                                    style_table={"overflowX": "auto"},
                                    style_header={"backgroundColor": "#12304a", "color": "white", "fontWeight": "700"},
                                    style_cell={"textAlign": "left", "fontSize": "13px", "padding": "8px",
                                                "maxWidth": "320px", "overflow": "hidden", "textOverflow": "ellipsis"},
                                ),
                            ]
                        ),
                    ],
                ),
                html.Div(
                    className="panel",
                    children=[
                        html.H2("6. Save the updated dataset"),
                        dcc.Checklist(
                            id="cu-typo-dobs",
                            options=[{
                                "label": "Apply the newest nomination's DOB when it differs from the dataset by a "
                                         "typo (one digit, or day/month swapped)",
                                "value": "apply",
                            }],
                            value=["apply"],
                            inputStyle={"marginRight": "7px"},
                        ),
                        dropdown_block(
                            "Commit message (optional)",
                            dcc.Input(
                                id="cu-commit-message",
                                type="text",
                                placeholder="Conversion update: +N rows, N program upgrades (years)",
                                debounce=True,
                                style={"width": "100%", "padding": "9px", "border": "1px solid var(--border)",
                                       "borderRadius": "8px"},
                            ),
                        ),
                        html.Div(
                            [
                                dcc.ConfirmDialogProvider(
                                    html.Button("Push to GitHub", id="cu-push", disabled=True),
                                    id="cu-push-confirm",
                                    message=(
                                        f"Commit the updated {CONVERSION_GITHUB.path} to {CONVERSION_GITHUB.repo} "
                                        f"({CONVERSION_GITHUB.branch}) with your review decisions? "
                                        "Held nominations stay out."
                                    ),
                                ),
                                html.Button(
                                    "Download updated conversion data (ZIP)",
                                    id="cu-download",
                                    disabled=True,
                                    className="secondary",
                                ),
                            ],
                            className="actions",
                        ),
                        html.Div(id="cu-push-note", className="hint", style={"marginTop": "8px"}),
                        dcc.Loading(html.Div(id="cu-push-status"), type="circle", color="#1676a3"),
                        dcc.Loading(html.Div(id="cu-download-status"), type="circle", color="#1676a3"),
                        dcc.Download(id="cu-file"),
                        html.P(
                            "Push to GitHub commits the updated dataset straight to the Conversion repo. The ZIP holds "
                            "the same dataset plus a list of every change, the nominations held for review, the "
                            "excluded nominations, and your saved review decisions; download it too if you want a record.",
                            className="hint",
                        ),
                        html.Details(
                            [html.Summary("How the conversion update works"), dcc.Markdown(CONVERSION_GUIDE)],
                            open=False,
                        ),
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
    Output("ee-workflow", "style"),
    Output("conversion-workflow", "style"),
    Input("workflow-mode", "value"),
)
def show_workflow(mode: str):
    hidden = {"display": "none"}
    visible = {"display": "block"}
    return (
        visible if mode == "custom" else hidden,
        visible if mode == "prebuilt" else hidden,
        visible if mode == "national_inventory" else hidden,
        visible if mode == "ee_templates" else hidden,
        visible if mode == "conversion_update" else hidden,
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
    Input("dataset-id", "data"),
)
def set_controls_ready(dataset_id: str | None):
    disabled = not bool(dataset_id)
    preview_option = [{"label": "Customize preview columns", "value": "custom", "disabled": disabled}]
    return (
        disabled, disabled, disabled, disabled, disabled, preview_option,
        disabled, disabled, disabled, disabled, disabled,
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
    Input("clear-filters", "n_clicks"),
    prevent_initial_call=True,
)
def clear_export_filters(_: int):
    return "", "", [], []


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
    Input("fetch-button", "n_clicks"),
    State("campus-dd", "value"),
    State("role-dd", "value"),
    State("fetch-source", "value"),
    prevent_initial_call=True,
)
def handle_fetch(_: int, campus_value: int | str, role_value: str, source: str):
    try:
        token = auth.get_token()
    except Exception as exc:
        message = f"Authentication failed: {type(exc).__name__}. Please sign in again."
        return no_update, message, "status error", "—", "—", [], [], [], []
    if not token:
        return no_update, "No access token. Please sign in again.", "status error", "—", "—", [], [], [], []
    actor_error = check_acting_profile(token)
    if actor_error:
        return no_update, actor_error, "status error", "—", "—", [], [], [], []

    frame, errors, pages = fetch_profiles_for_selection(token, campus_value, role_value, source)
    if frame.empty:
        label = FETCH_SOURCE_NOUNS.get(source, "profiles")
        message = " ".join(errors) if errors else f"No {label} matched the selected filters."
        if not errors and API_VERSION == "v2":
            message += (
                " In Registration API v2 an empty result can also mean your acting profile "
                "lacks permissions for this data."
            )
        return no_update, message, "status error", "0", "—", [], [], [], []

    dataset_id = save_dataset(frame)
    refreshed = datetime.now(timezone.utc).astimezone().strftime("%b %d, %Y %H:%M")
    if errors:
        message = (
            f"Partial result: saved {len(frame):,} {FETCH_SOURCE_NOUNS.get(source, 'rows')} from {pages} API pages. "
            + " ".join(errors)
            + " Export only if the missing data is acceptable."
        )
        status_class = "status error"
    else:
        message = (
            f"Fetched {len(frame):,} {FETCH_SOURCE_NOUNS.get(source, 'rows')} successfully across "
            f"{pages} API pages (Registration API {API_VERSION})."
        )
        status_class = "status"
    field_warnings = blank_field_warnings(frame)
    if field_warnings:
        message = [
            html.Div(message),
            html.Div(
                "Some fields came back empty, which usually means a missing permission rather than "
                "missing data: " + " ".join(field_warnings),
                className="warning",
            ),
        ]
    return (
        dataset_id,
        message,
        status_class,
        f"{len(frame):,}",
        refreshed,
        options_for_any(frame, ("enrollment_status", "current_enrollment.admin_status")),
        [],
        options_for_any(frame, ("nomination_claimed", "current_nomination.redeemed")),
        [],
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
):
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


@app.callback(
    Output("ee-current-year", "options"),
    Output("ee-current-year", "value"),
    Output("ee-sports", "options"),
    Output("ee-sports", "value"),
    Input("dataset-id", "data"),
)
def update_ee_controls(dataset_id: str | None):
    try:
        frame = load_dataset(dataset_id)
    except (FileNotFoundError, ValueError):
        return [], "", [], []
    years = available_fiscal_years(frame)
    sports = sport_values(frame)
    return (
        [{"label": year, "value": year} for year in reversed(years)],
        years[-1] if years else "",
        [{"label": sport, "value": sport} for sport in sports],
        default_sports(sports, "enhanced"),
    )


def is_profile_fetch(frame: pd.DataFrame) -> bool:
    """True when the dataset holds profile rows (latest nomination only) rather than nominations."""
    if "row_kind" not in frame or frame.empty:
        return False
    kinds = frame["row_kind"].fillna("").astype(str).str.strip().str.lower()
    return bool(kinds.isin({"profile", "profiles", "person", "people"}).all())


def ee_mode_options(current_year: str, previous_year: str) -> list[dict[str, Any]]:
    return [
        {"label": f"Current ({current_year or '—'})", "value": "current"},
        {
            "label": f"Previous ({previous_year or 'unknown'})",
            "value": "previous",
            "disabled": not previous_year,
        },
        {
            "label": f"Both ({previous_year or 'unknown'} + {current_year or '—'})",
            "value": "both",
            "disabled": not previous_year,
        },
    ]


@app.callback(
    Output("ee-year-matrix", "children"),
    Output("ee-default-note", "children"),
    Output("ee-year-note", "children"),
    Input("ee-sports", "value"),
    Input("ee-current-year", "value"),
    Input("ee-reset-years", "n_clicks"),
    State("dataset-id", "data"),
    State({"type": "ee-year-dd", "workbook": ALL}, "id"),
    State({"type": "ee-year-dd", "workbook": ALL}, "value"),
)
def render_ee_year_matrix(
    sports: list[str] | None,
    current_year: str,
    _: int,
    dataset_id: str | None,
    existing_ids: list[dict[str, str]],
    existing_values: list[str],
):
    try:
        frame = load_dataset(dataset_id)
    except (FileNotFoundError, ValueError):
        return html.Div("Fetch data to configure EE Sport Templates.", className="empty-state"), "", ""
    available = available_fiscal_years(frame)
    previous_year = previous_fiscal_year(current_year, available)
    if not current_year:
        year_note = "No fiscal years were found in the fetched data."
    elif previous_year in available:
        year_note = f"Previous fiscal year: {previous_year}."
    elif previous_year:
        year_note = f"Previous fiscal year: {previous_year} (no nominations for it in the fetched data)."
    else:
        year_note = "The previous fiscal year could not be determined from this value."
    if not sports:
        return html.Div("Select one or more EE sports.", className="empty-state"), "", year_note

    keep_existing = callback_context.triggered_id != "ee-reset-years"
    existing = (
        {item["workbook"]: value for item, value in zip(existing_ids or [], existing_values or [])}
        if keep_existing
        else {}
    )
    options = ee_mode_options(current_year, previous_year)
    allowed = {option["value"] for option in options if not option.get("disabled")}
    rows = []
    unscheduled = []
    for label, members in ee_workbook_groups(frame, sports):
        scheduled = default_ee_fiscal_mode([label, *members])
        if not scheduled:
            unscheduled.append(label)
        value = existing.get(label) or scheduled or "both"
        if value not in allowed:
            value = "current"
        hint = (
            f"Schedule default: {EE_FISCAL_MODE_LABELS[scheduled]}"
            if scheduled
            else "Not on the EE schedule — defaulting to Both"
        )
        if len(members) > 1:
            hint = f"{', '.join(members)} · {hint}"
        rows.append(
            html.Div(
                [
                    html.Div(label, className="label"),
                    dcc.Dropdown(
                        id={"type": "ee-year-dd", "workbook": label},
                        options=options,
                        value=value,
                        clearable=False,
                    ),
                    html.Div(hint, className="table-note"),
                ],
                className="sport-year-row",
            )
        )
    default_note = (
        f"No schedule default for: {', '.join(unscheduled)}." if unscheduled else ""
    )
    return rows, default_note, year_note


def ee_selection(
    workbook_ids: list[dict[str, str]] | None, workbook_values: list[str] | None
) -> dict[str, str]:
    return {item["workbook"]: value for item, value in zip(workbook_ids or [], workbook_values or [])}


@app.callback(
    Output("ee-cac", "data"),
    Output("ee-cac-status", "children"),
    Output("ee-cac-upload", "contents"),
    Input("ee-cac-upload", "contents"),
    Input("ee-cac-clear", "n_clicks"),
    State("ee-cac-upload", "filename"),
    prevent_initial_call=True,
)
def load_ee_cac_report(contents: str | None, _: int, filename: str | None):
    if callback_context.triggered_id == "ee-cac-clear" or not contents:
        return None, html.Div("No CAC file loaded; the CAC columns will be left blank.", className="hint"), None
    try:
        lookup, stats = parse_cac_report(contents, filename or "CAC upload")
    except ValueError as exc:
        return None, html.Div(str(exc), className="status error"), None
    messages = [
        html.Div(
            f"Loaded {filename}: {stats['coaches']:,} coaches from {stats['rows']:,} rows with an NCCP number"
            + (f" ({stats['skipped']:,} rows without one were skipped)." if stats["skipped"] else "."),
            className="status",
        )
    ]
    if stats["missing_columns"]:
        messages.append(
            html.Div(
                f"Columns not found in the file: {', '.join(stats['missing_columns'])}. "
                "Those CAC columns will show their empty value.",
                className="warning",
            )
        )
    return {"filename": filename, "lookup": lookup}, messages, no_update


@app.callback(
    Output("ee-review", "data"),
    Output("ee-review-status", "children"),
    Output("ee-download", "disabled"),
    Output("ee-athletes-download", "disabled"),
    Output("ee-mail-merge-download", "disabled"),
    Input("dataset-id", "data"),
    Input("ee-current-year", "value"),
    Input("ee-sports", "value"),
    Input({"type": "ee-year-dd", "workbook": ALL}, "value"),
    Input("ee-cac", "data"),
    State({"type": "ee-year-dd", "workbook": ALL}, "id"),
)
def review_ee_templates(
    dataset_id: str | None,
    current_year: str,
    sports: list[str] | None,
    workbook_values: list[str],
    cac: dict[str, Any] | None,
    workbook_ids: list[dict[str, str]],
):
    try:
        frame = load_dataset(dataset_id)
    except (FileNotFoundError, ValueError):
        return [], "", True, True, True
    if not sports:
        return [], html.Div("Select one or more EE sports.", className="status error"), True, True, True
    if not current_year:
        return [], html.Div("Choose the Current fiscal year.", className="status error"), True, True, True
    plans = plan_ee_workbooks(frame, current_year, sports, ee_selection(workbook_ids, workbook_values))
    cac_lookup = (cac or {}).get("lookup")
    rows = ee_review_rows(plans, cac_lookup)
    included = [row for row in rows if row["Status"] == "Included"]
    people = sum(row["Athletes"] + row["Coaches"] for row in included)
    messages = [
        html.Div(
            f"{len(included)} of {len(rows)} workbooks will be created with {people:,} people.",
            className="status" if included else "status error",
        )
    ]
    registered_athletes = len(ee_registered_athletes(plans))
    mail_merge, no_email = ee_mail_merge(plans)
    guardians = int((mail_merge["Contact"] == "Guardian").sum())
    messages.append(
        html.Div(
            f"The registered athlete list will contain {registered_athletes:,} actively registered athletes. "
            f"The mail merge has {len(mail_merge):,} recipients "
            f"({guardians:,} via a guardian for athletes under 19).",
            className="hint",
            style={"marginTop": "8px"},
        )
    )
    if no_email:
        shown = no_email[:15]
        more = f" … and {len(no_email) - 15} more" if len(no_email) > 15 else ""
        messages.append(
            html.Div(
                f"{len(no_email)} registered athlete(s) have no athlete or guardian email and are left out "
                "of the mail merge: " + "; ".join(shown) + more + ".",
                className="warning",
            )
        )
    if is_profile_fetch(frame):
        messages.insert(
            0,
            html.Div(
                "The loaded data was fetched as Profiles, which only carry each person's latest "
                "nomination, so earlier-year nominations (e.g. Previous-year coaches who have since "
                "renewed) are missing. Re-fetch with API data type “Nominations” before building EE templates.",
                className="status error",
            ),
        )
    available = set(available_fiscal_years(frame))
    missing_years = sorted({year for plan in plans for year in plan.years} - available)
    if missing_years:
        messages.append(
            html.Div(
                f"The fetched data has no nominations for {', '.join(missing_years)}. "
                "Workbooks using that year will be incomplete or omitted.",
                className="warning",
            )
        )
    if cac_lookup is not None:
        unmatched = [
            f"{plan.label}: {coach}"
            for plan in plans
            for coach in ee_cac_matches(plan, cac_lookup)[1]
        ]
        if unmatched:
            shown = unmatched[:15]
            more = f" … and {len(unmatched) - 15} more" if len(unmatched) > 15 else ""
            messages.append(
                html.Div(
                    f"{len(unmatched)} nominated coach(es) were not found in the CAC report by NCCP number "
                    "and will have blank CAC columns: "
                    + "; ".join(shown) + more + ".",
                    className="warning",
                )
            )
    shared_nccp = ee_shared_nccp_coaches(plans)
    if shared_nccp:
        messages.append(
            html.Div(
                "These coaches share an NCCP number under different names. All are kept in the "
                "workbook; check whether they are the same person or a data-entry error: "
                + "; ".join(shared_nccp) + ".",
                className="warning",
            )
        )
    omitted = [row["Workbook"] for row in rows if row["Status"] != "Included"]
    if omitted:
        messages.append(
            html.Div(f"Omitted from the ZIP: {', '.join(omitted)}.", className="warning")
        )
    return rows, messages, not included, registered_athletes == 0, mail_merge.empty


@app.callback(
    Output("ee-file", "data"),
    Output("ee-athletes-file", "data"),
    Output("ee-mail-merge-file", "data"),
    Output("ee-download-status", "children"),
    Input("ee-download", "n_clicks"),
    Input("ee-athletes-download", "n_clicks"),
    Input("ee-mail-merge-download", "n_clicks"),
    State("dataset-id", "data"),
    State("ee-current-year", "value"),
    State("ee-sports", "value"),
    State({"type": "ee-year-dd", "workbook": ALL}, "id"),
    State({"type": "ee-year-dd", "workbook": ALL}, "value"),
    State("ee-cac", "data"),
    prevent_initial_call=True,
)
def download_ee_templates(
    _: int,
    __: int,
    ___: int,
    dataset_id: str | None,
    current_year: str,
    sports: list[str] | None,
    workbook_ids: list[dict[str, str]],
    workbook_values: list[str],
    cac: dict[str, Any] | None,
):
    outputs = ["ee-download", "ee-athletes-download", "ee-mail-merge-download"]
    triggered = callback_context.triggered_id if callback_context.triggered_id in outputs else outputs[0]

    def respond(download: Any, message: Any):
        files = [download if name == triggered else no_update for name in outputs]
        return (*files, message)

    if not sports or not current_year:
        return respond(no_update, html.Div("Choose the Current fiscal year and EE sports.", className="status error"))
    try:
        frame = load_dataset(dataset_id)
        sport_years = ee_selection(workbook_ids, workbook_values)
        if triggered == "ee-athletes-download":
            artifact = build_ee_athlete_list(frame, current_year, sports, sport_years)
        elif triggered == "ee-mail-merge-download":
            artifact = build_ee_mail_merge(frame, current_year, sports, sport_years)
        else:
            artifact = build_ee_sport_templates(
                frame,
                current_year,
                sports,
                sport_years=sport_years,
                cac_lookup=(cac or {}).get("lookup"),
            )
    except (FileNotFoundError, ValueError) as exc:
        return respond(no_update, html.Div(str(exc), className="status error"))
    empty_messages = {
        "ee-download": "No workbook has qualifying athletes or coaches.",
        "ee-athletes-download": "No actively registered athletes were found in the selected workbooks.",
        "ee-mail-merge-download": "No registered athletes with a contact email were found.",
    }
    if artifact.row_count == 0:
        return respond(no_update, html.Div(empty_messages[triggered], className="status error"))
    nouns = {
        "ee-download": "people",
        "ee-athletes-download": "registered athletes",
        "ee-mail-merge-download": "mail-merge recipients",
    }
    return respond(
        dcc.send_bytes(artifact.content, artifact.filename),
        html.Div(
            f"Prepared {artifact.filename} with {artifact.row_count:,} {nouns[triggered]}.",
            className="status",
        ),
    )

@app.callback(
    Output("cu-conversion", "data"),
    Output("cu-upload-status", "children"),
    Output("cu-upload", "contents"),
    Input("cu-github", "n_clicks"),
    Input("cu-upload", "contents"),
    State("cu-upload", "filename"),
    prevent_initial_call=True,
)
def load_conversion_dataset(_: int | None, contents: list[str] | str | None, filename: list[str] | str | None):
    source_note = None
    decisions = empty_decisions()
    try:
        if callback_context.triggered_id == "cu-github":
            frame, info = fetch_conversion_from_github(CONVERSION_GITHUB)
            decisions = fetch_decisions_from_github(CONVERSION_GITHUB, info["sha"])
            info["source"] = "github"
            filename = Path(info["path"]).name
            when = info["date"][:10]
            source_note = html.Div(
                [
                    f"From {info['repo']} ({info['branch']}), commit ",
                    html.A(info["sha"][:7], href=info["url"], target="_blank") if info["url"] else info["sha"][:7],
                    f" on {when}: {info['message']}",
                ],
                className="hint",
                style={"marginTop": "8px"},
            )
        elif contents:
            files = list(zip(contents, filename or [""] * len(contents))) if isinstance(contents, list) \
                else [(contents, filename or "")]
            datasets, decision_files = [], []
            for item, name in files:
                raw = base64.b64decode(item.split(",", 1)[1]) if item and "," in item else b""
                (decision_files if is_decisions_csv(raw) else datasets).append((item, name, raw))
            if len(datasets) != 1:
                raise ValueError("Upload one conversion dataset CSV (and optionally its review-decisions file).")
            item, filename, _raw = datasets[0]
            frame = read_conversion_csv(item, filename or "")
            for _item, _name, raw in decision_files:
                decisions = merge_decisions(decisions, parse_decisions_csv(raw))
            info = {}
        else:
            # The upload being cleared after a load re-triggers this callback; keep what's loaded.
            return no_update, no_update, no_update
    except (ValueError, UnicodeDecodeError, pd.errors.ParserError) as exc:
        return None, html.Div(str(exc), className="status error"), None
    summary = dataset_summary(frame)
    message = (
        f"Loaded {filename}: {summary['rows']:,} rows, {summary['athletes']:,} athletes, "
        f"{summary['first_year']}–{summary['latest_year']} "
        f"({summary['latest_year_rows']:,} rows in {summary['latest_year']})."
    )
    notes = [html.Div(message, className="status")]
    if source_note is not None:
        notes.append(source_note)
    if len(decisions):
        notes.append(html.Div(
            f"{len(decisions):,} saved review decisions loaded; those nominations won't need reviewing again.",
            className="hint", style={"marginTop": "8px"},
        ))
    if not summary["profile_ids"]:
        notes.append(html.Div(
            "No Profile_ID column yet; it will be added and filled for athletes with a Registration profile.",
            className="hint", style={"marginTop": "8px"},
        ))
    # Clear the upload so the same file can be uploaded again later.
    store = {
        "dataset_id": save_dataset(frame),
        "decisions_id": save_dataset(decisions),
        "filename": filename,
        "summary": summary,
        "source": info.get("source", "upload"),
        "sha": info.get("sha", ""),
    }
    return store, notes, None


@app.callback(
    Output("cu-years", "options"),
    Output("cu-years", "value"),
    Output("cu-source-note", "children"),
    Input("dataset-id", "data"),
)
def update_conversion_years(dataset_id: str | None):
    try:
        frame = load_dataset(dataset_id)
    except (FileNotFoundError, ValueError):
        return [], [], html.Div("Fetch data in step 1 first.", className="hint")
    years = nomination_fiscal_years(frame)
    kinds = row_kinds(frame)
    if kinds and not {"profile", "nomination"} <= kinds:
        note = html.Div(
            "This fetch has only "
            + ("profile rows (claimed nominations)" if "profile" in kinds else "nomination rows (unclaimed nominations)")
            + ". Fetch Profiles + Nominations with Role = All roles or Athlete so no nominations are missed.",
            className="warning",
        )
    else:
        note = html.Div("Using claimed (profile) and unclaimed (nomination) rows.", className="hint")
    return [{"label": year, "value": year} for year in reversed(years)], years, note


def _conversion_inputs(dataset_id: str | None, conversion: dict | None, years: list[str] | None):
    if not conversion:
        raise ValueError("Upload the conversion dataset in step 3.")
    if not years:
        raise ValueError("Choose at least one nomination fiscal year.")
    frame = load_dataset(dataset_id)
    try:
        dataset = load_dataset(conversion.get("dataset_id"))
    except FileNotFoundError as exc:
        raise FileNotFoundError("The uploaded conversion dataset expired. Upload it again.") from exc
    return frame, dataset, _saved_decisions(conversion)


def _saved_decisions(conversion: dict) -> pd.DataFrame:
    try:
        return load_dataset(conversion.get("decisions_id"))
    except FileNotFoundError:
        return empty_decisions()


@app.callback(
    Output("cu-plan", "data"),
    Output("cu-plan-status", "children"),
    Output("cu-review", "data"),
    Output("cu-plan-table", "data"),
    Output("cu-count-new", "children"),
    Output("cu-count-returning", "children"),
    Output("cu-count-upgrades", "children"),
    Output("cu-count-current", "children"),
    Output("cu-count-review", "children"),
    Output("cu-count-excluded", "children"),
    Output("cu-download", "disabled"),
    Input("dataset-id", "data"),
    Input("cu-conversion", "data"),
    Input("cu-years", "value"),
)
def build_conversion_plan(dataset_id: str | None, conversion: dict | None, years: list[str] | None):
    blank = ("—",) * 6
    try:
        frame, dataset, saved = _conversion_inputs(dataset_id, conversion, years)
        candidates, excluded = prepare_nominations(frame, years)
        plan = match_nominations(candidates, dataset, saved)
    except (FileNotFoundError, ValueError) as exc:
        return None, html.Div(str(exc), className="hint"), [], [], *blank, True
    counts = plan_counts(plan)
    order = {ACTION_REVIEW: 0, ACTION_ADD: 1, ACTION_UPGRADE: 2, ACTION_CSS_ONLY: 3, ACTION_UP_TO_DATE: 4}
    table = plan.assign(_o=plan["Action"].map(order)).sort_values(["_o", "Year", "Last Name"]).drop(columns="_o")
    changes = len(plan) - counts["up_to_date"] - counts["review"]
    reused = int(plan["Status"].isin([STATUS_SAVED_SAME, STATUS_SAVED_DIFFERENT]).sum())
    status = html.Div(
        f"{len(plan):,} athlete nominations checked against {len(dataset):,} dataset rows: "
        f"{changes:,} will change the dataset"
        + (f", {counts['review']:,} need a decision" if counts["review"] else "")
        + (f", {counts['css_only']:,} CSS-only Multisport" if counts["css_only"] else "")
        + (f"; {reused:,} settled by saved review decisions" if reused else "")
        + ".",
        className="status",
    )
    store = {"plan_id": save_dataset(plan), "excluded_id": save_dataset(excluded)}
    return (
        store,
        status,
        review_rows(plan),
        table.to_dict("records"),
        f"{counts['new_athletes']:,}",
        f"{counts['returning']:,}",
        f"{counts['upgrades']:,}",
        f"{counts['up_to_date']:,}",
        f"{counts['review']:,}",
        f"{len(excluded):,}",
        False,
    )


def _conversion_result(
    plan_store: dict | None, conversion: dict | None, review: list[dict] | None, typo: list[str] | None
):
    """Apply the review decisions to the loaded dataset (shared by download and push)."""
    if not plan_store or not conversion:
        raise ValueError("Load the dataset and nominations first.")
    try:
        plan = load_dataset(plan_store["plan_id"])
        excluded = load_dataset(plan_store["excluded_id"])
        dataset = load_dataset(conversion["dataset_id"])
    except (FileNotFoundError, ValueError, KeyError) as exc:
        raise ValueError("This review expired. Load the conversion dataset again.") from exc
    decisions = {row["Review ID"]: row.get("Decision") or HOLD for row in review or []}
    result = apply_update(dataset, plan, decisions, apply_typo_dobs="apply" in (typo or []))
    saved = merge_decisions(_saved_decisions(conversion), result.decisions)
    return result, excluded, saved


def _result_messages(result) -> list[Any]:
    counts = result.counts
    parts = [
        f"{counts['rows_added']:,} rows added",
        f"{counts['programs_upgraded']:,} programs upgraded",
        f"{counts['dob_changes']:,} athletes' DOB updated",
        f"{counts['dob_filled']:,} missing DOBs filled in",
        f"{counts['held']:,} held for review",
    ]
    if counts["multisport_removed"]:
        parts.append(f"{counts['multisport_removed']:,} Multisport rows folded into CSS")
    messages = [html.Div(
        f"Rows {counts['rows_before']:,} → {counts['rows_after']:,}: " + ", ".join(parts) + ".",
        className="status",
    )]
    if counts["convert_changes_untouched"]:
        messages.append(html.Div(
            f"{counts['convert_changes_untouched']:,} Convert Year flags changed for athletes not in this update. "
            "The dataset was edited outside this tool; check conversion_update_changes.csv.",
            className="warning",
        ))
    return messages


@app.callback(
    Output("cu-file", "data"),
    Output("cu-download-status", "children"),
    Input("cu-download", "n_clicks"),
    State("cu-plan", "data"),
    State("cu-conversion", "data"),
    State("cu-review", "data"),
    State("cu-typo-dobs", "value"),
    prevent_initial_call=True,
)
def download_conversion_update(
    _: int, plan_store: dict | None, conversion: dict | None, review: list[dict] | None, typo: list[str] | None
):
    try:
        result, excluded, saved = _conversion_result(plan_store, conversion, review, typo)
    except ValueError as exc:
        return no_update, html.Div(str(exc), className="status error")
    stamp = datetime.now().strftime("%Y%m%d_%H%M")
    content = build_update_zip(result, excluded, conversion.get("filename") or "Conversion_Data_2026_final.csv", saved)
    return dcc.send_bytes(content, f"conversion_update_{stamp}.zip"), _result_messages(result)


@app.callback(
    Output("cu-push", "disabled"),
    Output("cu-push-note", "children"),
    Input("cu-plan", "data"),
    Input("cu-conversion", "data"),
)
def update_push_availability(plan_store: dict | None, conversion: dict | None):
    if not CONVERSION_GITHUB.token:
        return True, (
            "Push to GitHub needs a GITHUB_TOKEN in .env (a fine-grained token with Contents: Read and write "
            f"on {CONVERSION_GITHUB.repo}). Until then, download the ZIP and upload the CSV on GitHub."
        )
    if not conversion:
        return True, ""
    if conversion.get("source") != "github":
        return True, "Push is only available when the dataset was loaded with Load latest from GitHub."
    if not plan_store:
        return True, ""
    return False, f"Pushes to {CONVERSION_GITHUB.label}."


@app.callback(
    Output("cu-push-status", "children"),
    Output("cu-conversion", "data", allow_duplicate=True),
    Input("cu-push-confirm", "submit_n_clicks"),
    State("cu-plan", "data"),
    State("cu-conversion", "data"),
    State("cu-review", "data"),
    State("cu-typo-dobs", "value"),
    State("cu-commit-message", "value"),
    State("cu-years", "value"),
    prevent_initial_call=True,
)
def push_conversion_update(
    _: int,
    plan_store: dict | None,
    conversion: dict | None,
    review: list[dict] | None,
    typo: list[str] | None,
    message: str | None,
    years: list[str] | None,
):
    def error(text: str):
        return html.Div(text, className="status error"), no_update

    if not conversion or conversion.get("source") != "github" or not conversion.get("sha"):
        return error("Load the dataset with Load latest from GitHub before pushing.")
    try:
        result, _excluded, saved = _conversion_result(plan_store, conversion, review, typo)
    except ValueError as exc:
        return error(str(exc))
    if not result.counts["data_changes"] and result.decisions.empty:
        return error("Nothing to push: the dataset is already up to date.")
    full_message = commit_message(result, years)
    if message and message.strip():
        title, _, body = full_message.partition("\n\n")
        full_message = message.strip() + "\n\n" + body
    content = result.dataset.to_csv(index=False).encode("utf-8")
    try:
        pushed = push_conversion_to_github(
            CONVERSION_GITHUB, content, conversion["sha"], full_message,
            extra_files={CONVERSION_GITHUB.decisions_path: saved.to_csv(index=False).encode("utf-8")},
        )
    except ValueError as exc:
        return error(str(exc))
    # The loaded dataset now matches GitHub, so the plan rebuilds as "already up to date".
    updated = {
        **conversion,
        "dataset_id": save_dataset(result.dataset),
        "decisions_id": save_dataset(saved),
        "sha": pushed["sha"],
        "summary": dataset_summary(result.dataset),
    }
    messages = _result_messages(result) + [html.Div(
        ["Pushed to GitHub: ", html.A(f"commit {pushed['sha'][:7]}", href=pushed["url"], target="_blank"),
         f" on {CONVERSION_GITHUB.repo} ({CONVERSION_GITHUB.branch})."],
        className="status",
    )]
    return messages, updated


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
        debug=os.environ.get("DASH_DEBUG", "").lower() == "true",
        host=urlsplit(APP_URL).hostname or "127.0.0.1",
        port=local_port,
    )
