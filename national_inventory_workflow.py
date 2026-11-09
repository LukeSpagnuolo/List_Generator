"""Build the Name/Sport based National Inventory service report."""

from __future__ import annotations

import base64
import io
from pathlib import Path
from typing import Iterable

import pandas as pd

from prebuilt_reports_7000 import LOU_SPORTS


SERVICES = [
    "BPA",
    "Medical",
    "Nutrition",
    "Physiology",
    "Physiotherapy",
    "S&C",
    "Data Solutions",
    "Gameplan",
]
OUTPUT_COLUMNS = ["Name", "Sport", *SERVICES, "Registration"]
TRUE_VALUES = {"true", "1", "yes", "y"}
LOU_ALIASES = {
    "alpine skiing": "alpine ski",
    "para alpine skiing": "para alpine ski",
    "freestyle skiing": "freestyle ski",
    "skateboard": "skateboarding",
}


def normalize(value: object) -> str:
    return " ".join(str(value or "").strip().lower().split())


def normalize_name(value: object) -> str:
    """Normalize both `First Last` and common `Last, First` upload formats."""
    text = str(value or "").strip()
    if text.count(",") == 1:
        last, first = text.split(",", 1)
        text = f"{first} {last}"
    return normalize(text)


def normalize_sport(value: object) -> str:
    key = normalize(value)
    return LOU_ALIASES.get(key, key)


def _column(frame: pd.DataFrame, candidates: Iterable[str]) -> str:
    by_key = {normalize(name).replace("_", " "): name for name in frame.columns}
    for candidate in candidates:
        actual = by_key.get(normalize(candidate).replace("_", " "))
        if actual is not None:
            return actual
    return ""


def name_sport_rows(frame: pd.DataFrame) -> pd.DataFrame:
    """Reduce any supported source to unique, nonblank Name/Sport rows."""
    name_col = _column(frame, ["Name", "Athlete Name", "Full Name"])
    sport_col = _column(frame, ["Sport", "Sport Name", "Nomination Sport"])
    if not name_col:
        first = _column(frame, ["First Name", "first_name", "person.first_name"])
        last = _column(frame, ["Last Name", "last_name", "person.last_name"])
        if first and last:
            names = (
                frame[first].fillna("").astype(str).str.strip()
                + " "
                + frame[last].fillna("").astype(str).str.strip()
            ).str.strip()
        else:
            raise ValueError("A Name column, or both First Name and Last Name columns, is required.")
    else:
        names = frame[name_col]
    if not sport_col:
        raise ValueError("A Sport column is required.")

    result = pd.DataFrame({"Name": names, "Sport": frame[sport_col]})
    result["Name"] = result["Name"].fillna("").astype(str).str.strip()
    result["Sport"] = result["Sport"].fillna("").astype(str).str.strip()
    result = result[(result["Name"] != "") & (result["Sport"] != "")].copy()
    result["_name"] = result["Name"].map(normalize_name)
    result["_sport"] = result["Sport"].map(normalize_sport)
    return result.drop_duplicates(["_name", "_sport"], keep="first")[
        ["Name", "Sport"]
    ].reset_index(drop=True)


def registration_cohort(frame: pd.DataFrame, fiscal_year: str) -> pd.DataFrame:
    """Select approved SC-carded athletes for the requested fiscal year."""
    role = _column(frame, ["role", "role_slug", "Role"])
    approved = _column(frame, ["nomination_approved", "current_nomination.approved"])
    year = _column(frame, ["nomination_fiscal_year", "current_nomination.fiscal_year", "Fiscal Year"])
    level = _column(frame, ["level_category", "Level Category"])
    carding = _column(frame, ["athlete_carding", "carding_level_mapped", "Carding Level"])
    work = frame.copy()
    if not role or not approved or (not level and not carding):
        raise ValueError("Registration data is missing role, approval, or carding fields.")
    work = work[work[role].map(normalize) == "athlete"]
    work = work[work[approved].fillna("").astype(str).str.lower().isin(TRUE_VALUES)]
    if fiscal_year:
        if not year:
            raise ValueError("Registration data does not contain a fiscal-year field.")
        work = work[work[year].map(normalize) == normalize(fiscal_year)]
    # Sport Canada card levels used by the registration system. Level Category
    # remains a fallback for endpoint versions that omit or sparsely fill carding.
    sc_levels = {"sr", "sri", "sr1", "sr2", "d", "di", "c1", "c1i"}
    sc_mask = pd.Series(False, index=work.index)
    if carding:
        sc_mask |= work[carding].map(normalize).isin(sc_levels)
    if level:
        sc_mask |= work[level].map(normalize) == "canadian elite"
    work = work[sc_mask]
    return name_sport_rows(work)


def parse_upload(contents: str, filename: str) -> pd.DataFrame:
    """Read a base64 Dash upload as CSV or Excel."""
    try:
        _, encoded = contents.split(",", 1)
        payload = base64.b64decode(encoded)
    except (ValueError, TypeError, base64.binascii.Error) as exc:
        raise ValueError(f"{filename}: the uploaded file could not be decoded.") from exc
    suffix = Path(filename or "").suffix.lower()
    try:
        if suffix == ".csv":
            return pd.read_csv(io.BytesIO(payload))
        if suffix in {".xlsx", ".xlsm", ".xls"}:
            sheets = pd.read_excel(io.BytesIO(payload), sheet_name=None)
            for frame in sheets.values():
                if _column(frame, ["Name", "Athlete Name", "Full Name"]) and _column(
                    frame, ["Sport", "Sport Name", "Nomination Sport"]
                ):
                    return frame
            return next(iter(sheets.values()))
    except Exception as exc:
        raise ValueError(f"{filename}: {exc}") from exc
    raise ValueError(f"{filename}: upload a CSV or Excel file.")


def uploaded_cohort(contents: list[str], filenames: list[str]) -> pd.DataFrame:
    if not contents:
        raise ValueError("Upload at least one CSV or Excel file.")
    frames = [name_sport_rows(parse_upload(body, name)) for body, name in zip(contents, filenames)]
    return name_sport_rows(pd.concat(frames, ignore_index=True))


def lou_sports_in(cohort: pd.DataFrame) -> list[str]:
    labels: dict[str, str] = {}
    for sport in cohort.get("Sport", pd.Series(dtype="object")):
        raw_key = normalize(sport)
        key = normalize_sport(sport)
        if raw_key in LOU_SPORTS or key in LOU_SPORTS:
            labels.setdefault(key, str(sport).strip())
    return [labels[key] for key in sorted(labels, key=lambda key: labels[key].lower())]


def active_claimed_pairs(frame: pd.DataFrame, fiscal_year: str) -> set[tuple[str, str]]:
    status = _column(frame, ["enrollment_status", "current_enrollment.admin_status", "Enrollment Status"])
    claimed = _column(frame, ["nomination_claimed", "current_nomination.redeemed", "Nomination Claimed"])
    year = _column(frame, ["nomination_fiscal_year", "current_nomination.fiscal_year", "Fiscal Year"])
    if not status or not claimed:
        return set()
    work = frame[
        (frame[status].map(normalize) == "active")
        & frame[claimed].fillna("").astype(str).str.lower().isin(TRUE_VALUES)
    ].copy()
    if fiscal_year:
        if not year:
            return set()
        work = work[work[year].map(normalize) == normalize(fiscal_year)]
    try:
        pairs = name_sport_rows(work)
    except ValueError:
        return set()
    return {(normalize_name(row.Name), normalize_sport(row.Sport)) for row in pairs.itertuples()}


def build_service_report(
    cohort: pd.DataFrame,
    services_by_sport: dict[str, list[str]],
    registration_frame: pd.DataFrame,
    fiscal_year: str,
) -> pd.DataFrame:
    result = name_sport_rows(cohort)
    selected = {normalize_sport(sport): set(services) for sport, services in services_by_sport.items()}
    registered = active_claimed_pairs(registration_frame, fiscal_year)
    for service in SERVICES:
        result[service] = result["Sport"].map(
            lambda sport: "Partial Claim" if service in selected.get(normalize_sport(sport), set()) else ""
        )
    result["Registration"] = [
        "Partial Claim" if (normalize_name(name), normalize_sport(sport)) in registered else ""
        for name, sport in zip(result["Name"], result["Sport"])
    ]
    return result[OUTPUT_COLUMNS]
