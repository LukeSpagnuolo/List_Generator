"""Prebuilt report definitions carried forward from List Generator 5000."""

from __future__ import annotations

import base64
import difflib
import io
import re
import zipfile
from copy import copy
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook


TRUE_VALUES = {"true", "1", "yes", "y"}
FALSE_VALUES = {"false", "0", "no", "n"}
TEST_SPORTS = {"cinderball (test)", "skimboarding cross (test)"}
LOU_SPORTS = {
    "alpine ski", "alpine skiing", "para alpine ski", "para alpine skiing",
    "athletics", "cycling", "diving", "freestyle ski", "freestyle skiing",
    "artistic gymnastics", "rowing", "rugby", "sailing", "skateboard",
    "skateboarding", "snowboard", "soccer", "swimming", "triathlon",
    "para triathlon", "volleyball", "wheelchair rugby",
}
ENHANCED_EXCELLENCE_SPORTS = {
    "alpine ski", "alpine skiing", "artistic swimming", "athletics", "diving",
    "basketball", "biathlon", "artistic gymnastics", "canoe/kayak", "canoe kayak",
    "cross country skiing", "cross-country skiing", "cross-country ski", "curling",
    "cycling", "judo", "rowing", "field hockey", "figure skating",
    "freestyle skiing", "freestyle ski", "wheelchair athletics", "wheelchair rugby",
    "wheelchair tennis", "wrestling", "rugby", "sailing", "snowboard", "swimming",
    "triathlon", "volleyball", "wheelchair basketball",
}
# Default fiscal-year mode for each EE sport workbook, keyed by canonical sport
# (see ee_sport_key). Para variants inherit their parent sport's default.
EE_DEFAULT_FISCAL_MODES = {
    "alpine skiing": "both",
    "athletics": "both",
    "basketball": "both",
    "biathlon": "both",
    "canoe/kayak": "both",
    "cross country skiing": "both",
    "curling": "both",
    "cycling": "both",
    "diving": "both",
    "field hockey": "both",
    "figure skating": "both",
    "freestyle skiing": "both",
    "gymnastics": "both",
    "judo": "both",
    "rowing": "both",
    "rugby": "both",
    "sailing": "both",
    "snowboard": "both",
    "swimming": "both",
    "triathlon": "both",
    "volleyball": "both",
    "wheelchair athletics": "both",
    "wheelchair basketball": "both",
    "wheelchair rugby": "both",
    "wheelchair tennis": "both",
    "wrestling": "both",
}
EE_SPORT_ALIASES = {
    "alpine ski": "alpine skiing",
    "freestyle ski": "freestyle skiing",
    "cross-country ski": "cross country skiing",
    "cross-country skiing": "cross country skiing",
    "cross country ski": "cross country skiing",
    "canoe kayak": "canoe/kayak",
    "canoe-kayak": "canoe/kayak",
    "artistic gymnastics": "gymnastics",
    "wheelchair sports": "wheelchair athletics",
}
# Athlete carding levels included in EE workbooks and the registered athlete list.
EE_ATHLETE_CARDING = {"prov dev 1", "prov dev 2"}
EE_FISCAL_MODE_LABELS = {"current": "Current", "previous": "Previous", "both": "Both"}
# Coaches-sheet header -> (CAC coach report column, value written when the coach
# is in the report but that field is empty).
CAC_TEMPLATE_COLUMNS = {
    "CAC NCCP Comp-Dev Status": ("Comp Dev Level", "No Status"),
    "CAC NCCP Safe Sport Training Status": ("Safesport Completion Date", "Not Completed"),
    "CAC ProCoach Designation": ("ProCoach Designation", "No ProCoach Status"),
    "CAC ProCoach Standing": ("ProCoach Standing", "No ProCoach Status"),
}

FIELD_ALIASES = {
    "role": ["role", "role_slug", "Role"],
    "first_name": ["first_name", "person.first_name", "First Name"],
    "last_name": ["last_name", "person.last_name", "Last Name"],
    "email": ["email", "person.email", "Email"],
    "guardian_first_name": ["guardian_first_name", "person.guardian.first_name"],
    "guardian_last_name": ["guardian_last_name", "person.guardian.last_name"],
    "guardian_email": ["guardian_email", "person.guardian.email"],
    "age": ["age", "Age"],
    "dob": ["dob", "person.dob", "date_of_birth", "Date of Birth"],
    "sport": ["sport", "sport.name", "nomination_sport_name", "Sport"],
    "nomination_sport": ["nomination_sport_name", "current_nomination.sport.name", "sport"],
    "enrollment_status": ["enrollment_status", "current_enrollment.admin_status"],
    "nomination_fiscal_year": ["nomination_fiscal_year", "fiscal_year", "Fiscal Year"],
    "nomination_claimed": ["nomination_claimed", "current_nomination.redeemed"],
    "nomination_approved": ["nomination_approved", "current_nomination.approved"],
    "athlete_carding": ["athlete_carding", "carding_level_mapped", "card_level"],
    "level_category": ["level_category", "Level Category"],
    "sex_of_competition": ["sex_of_competition", "Sex of Competition"],
    "gender": ["gender", "Gender"],
    "discipline": ["discipline", "Discipline"],
    "ethnicity": ["athlete_ethnicity", "ethnicity", "Ethnicity"],
    "nccp_number": ["nccp_number", "current_nomination.nccp_number", "NCCP Number"],
    "current_campus": [
        "current_city_campus",
        "residence_city_campus",
        "current_campus",
        "campus_label",
    ],
    "person_id": ["person_id", "profile_id", "id", "person.id", "profile.id"],
    "nomination_id": ["nomination_id", "current_nomination.id", "Nomination ID"],
    "season": ["season", "Season"],
    "training_group": ["training_group", "training_groups", "training_group_name"],
}

REPORT_OPTIONS = [
    {"label": "Pending Unmatched Report", "value": "pending_unmatched"},
    {"label": "Duplicate Profiles (Name + Sport)", "value": "duplicate_profiles"},
    {"label": "Guardian Email Matches Athlete Email", "value": "guardian_email_match"},
    {"label": "Unclaimed Nominations", "value": "unclaimed_nominations"},
    {"label": "Mail Merge", "value": "mail_merge"},
    {"label": "Active Athletes", "value": "active_athletes"},
    {"label": "ViaSport LOU List", "value": "viasport_lou"},
    {"label": "ViaSport Enhanced Excellence List", "value": "enhanced_excellence"},
    {"label": "ViaSport SC Carded List (2A)", "value": "sc_carded"},
    {"label": "ViaSport High Performance Athletes (2B)", "value": "high_performance"},
    {"label": "ViaSport Coaches (4B)", "value": "coaches"},
    {"label": "ViaSport Conversion Tab (3)", "value": "conversion"},
    {"label": "All ViaSport Reports (ZIP)", "value": "viasport_all"},
]

REPORT_GUIDE = """
### Common rules

- Every preset uses data from the last successful **Fetch profiles** action.
- The fiscal-year selector applies to every prebuilt report, including each
  ViaSport report and the combined ViaSport ZIP, **except** Duplicate Profiles
  and Guardian Email Matches Athlete Email. Those two check for identity
  problems across the whole fetched dataset, so limiting them to one fiscal
  year could hide issues on records without a matching nomination year.
- Test sports are removed from every preset.
- Fixed-format reports ignore extra API columns.

### Pending Unmatched Report

Keeps pending profile rows and pairs them with nomination rows when names are
similar and sports are compatible. Pairs already linked by matching athlete and
guardian email are skipped. Output: role, name, guardian email, sport,
enrollment status, and athlete email.

### Duplicate Profiles (Name + Sport)

Groups rows by exact first name, last name, and sport (case- and
whitespace-insensitive) and keeps every group where the same person/sport
combination appears under more than one email address. Rows with a blank
name or email are ignored. Output: role, name, sport, email, enrollment
status, and nomination claimed, sorted so each duplicate group sits together.

### Guardian Email Matches Athlete Email

Keeps rows where the athlete's own email and the guardian's email are the
same address (case-insensitive), which usually means the guardian email was
entered incorrectly and needs to be corrected. Duplicate rows for the same
person and email are collapsed. Output: role, name, email, guardian name,
guardian email, age, sport, and enrollment status.

### Unclaimed Nominations

Keeps approved but unclaimed nominations, sorts by last name, removes duplicate
people, and exports nomination contact fields.

### Mail Merge

Uses the Unclaimed Nominations cohort. The recipient is the guardian for an
athlete under 19, otherwise the athlete. Exports First Name, Last Name, and To.
Lists over 450 rows are split into CSV files inside a ZIP.

### Active Athletes

Keeps claimed nominations, adds Level Category, and removes internal identifier
columns before export.

### ViaSport LOU List

Uses the selected LOU sports, fiscal-year selection, and the standard ordered
export columns.

### ViaSport Enhanced Excellence List

Uses the selected Enhanced Excellence sports, fiscal-year selection, and the
standard ordered export columns.

EE Sport Templates have their own tab.

### SC Carded List (2A)

Uses the selected fiscal year. Keeps approved athletes in Canadian Elite and exports
Sport, Name, Gender, Card level, Province, and Nomination Claimed.

### High Performance Athletes (2B)

Uses the selected fiscal year. Keeps approved athletes and exports Nomination ID,
Sport, Card level, sex of competition, gender, age, discipline, current campus,
ethnicity, and nomination claimed.

### Coaches (4B)

Uses the selected fiscal year. Keeps approved coaches and exports Nomination ID, Sport,
Gender, Current City Campus, Ethnicity, Nomination Claimed, and NCCP Number.

### Conversion Tab (3)

Uses the High Performance Athletes cohort, grouped by sport. Exports total
athletes, average age, and F/M/X sex-of-competition counts.

### All ViaSport Reports

Creates a ZIP containing SC Carded, High Performance Athletes, and Coaches CSVs.
"""

EE_TEMPLATE_GUIDE = """
Creates one formatted Excel workbook per Enhanced Excellence sport and packages
the workbooks in a ZIP.

- **Fiscal years.** Each workbook uses the Current fiscal year, the Previous
  fiscal year (the one immediately before Current, e.g. 2024-2025 for
  2025-2026), or Both. Every sport defaults to Both; the setting can be
  changed per workbook.
- **Grouping.** Para variants share a workbook with their parent sport.
  Wheelchair Athletics, Rugby, and Tennis share one *Wheelchair sports*
  workbook; Wheelchair Basketball has its own.
- **Rows.** Only approved nominations are used. The Athletes sheet is
  restricted to `Prov Dev 1` and `Prov Dev 2` (`Prov Dev 3` is excluded); qualifying
  coaches populate the Coaches sheet. A person appearing in both selected years
  is listed once.
- **Registered with CSI Pacific** is `YES` only when Nomination Claimed is true
  and Enrollment Status is Active.
- **CAC Locker Status** (Coaches sheet, last four columns) is filled from an
  uploaded CAC coach report by matching NCCP numbers: Comp Dev Level, Safe
  Sport completion date, ProCoach Designation, and ProCoach Standing. A matched
  coach with no value shows *No Status* / *Not Completed* / *No ProCoach
  Status*. A coach not found in the report (including one without a valid
  NCCP number) is left blank. Without an upload these columns stay blank.
- Questionnaire fields and the Domestic Results, National Team, and Service
  Provider entry areas remain blank for manual completion.
- Workbooks with no qualifying athletes or coaches are left out of the ZIP and
  flagged in the review table.
- **Registered athlete list** is one spreadsheet of every athlete across the
  workbooks who is actively registered (the same rule as *Registered with CSI
  Pacific*), with Last Name, First Name, Sport, Email, and Guardian Email.
- **Mail merge** uses the same athletes. Athletes under 19 are contacted
  through their guardian's email; everyone else directly. Age comes from the
  age field, or date of birth when age is blank. If the preferred email is
  missing, the other one is used; athletes with no email at all are listed in
  the review and left out. Columns: First Name, Last Name, To, Contact
  (Athlete/Guardian), Sport. Lists over 450 rows are split into CSVs in a ZIP.
"""

SEASON_BY_SPORT = {
    "alpine ski": "Winter", "biathlon": "Winter", "bobsleigh": "Winter",
    "cross-country ski": "Winter", "curling": "Winter", "figure skating": "Winter",
    "freestyle ski": "Winter", "hockey": "Winter", "luge": "Winter",
    "nordic combined": "Winter", "para alpine ski": "Winter",
    "para cross-country ski": "Winter", "para ice hockey": "Winter",
    "para snowboard": "Winter", "ringette": "Winter", "skeleton": "Winter",
    "ski cross": "Winter", "ski jumping": "Winter", "snowboard": "Winter",
    "speed skating": "Winter", "wheelchair curling": "Winter",
}


@dataclass
class ReportArtifact:
    content: bytes
    filename: str
    mime_type: str = "text/csv"
    row_count: int = 0


def normalize(value) -> str:
    return " ".join(str(value or "").strip().lower().split())


def column(frame: pd.DataFrame, logical_name: str) -> str:
    return next((name for name in FIELD_ALIASES.get(logical_name, []) if name in frame), "")


def series(frame: pd.DataFrame, logical_name: str) -> pd.Series:
    name = column(frame, logical_name)
    return frame[name] if name else pd.Series("", index=frame.index, dtype="object")


def truthy(values: pd.Series) -> pd.Series:
    return values.fillna("").astype(str).str.strip().str.lower().isin(TRUE_VALUES)


def falsey(values: pd.Series) -> pd.Series:
    return values.fillna("").astype(str).str.strip().str.lower().isin(FALSE_VALUES)


def remove_test_sports(frame: pd.DataFrame) -> pd.DataFrame:
    keep = pd.Series(True, index=frame.index)
    for logical in ("sport", "nomination_sport"):
        values = series(frame, logical).fillna("").astype(str).map(normalize)
        keep &= ~(values.isin(TEST_SPORTS) | values.str.contains("(test", regex=False))
    return frame.loc[keep]


def fiscal_year(frame: pd.DataFrame, value: str) -> pd.DataFrame:
    if not value:
        return frame
    name = column(frame, "nomination_fiscal_year")
    if not name:
        return frame
    return frame[frame[name].fillna("").astype(str).map(normalize) == normalize(value)]


def add_level_category(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    carding = series(result, "athlete_carding").fillna("").astype(str).str.strip()
    category = pd.Series("", index=result.index, dtype="object")
    category[carding.isin({"Prov Dev 1", "Prov Dev 2", "Prov Dev 3"})] = "Provincial Development"
    category[carding.str.contains("NSO Affiliated", case=False, na=False)] = "Canadian Development"
    category[carding.str.contains("PSO Affiliated", case=False, na=False)] = "PSO Affiliated (Non Carded)"
    category[carding.isin({"SR", "SRI", "SR1", "SR2", "D", "DI", "C", "C1", "C1I", "GamePlan - Retired", "GamePlan Retired"})] = "Canadian Elite"
    result["level_category"] = category
    return result


def selected_sports(frame: pd.DataFrame, values: list[str] | None) -> pd.DataFrame:
    if not values:
        return frame
    selected = {normalize(value) for value in values}
    keep = pd.Series(False, index=frame.index)
    for logical in ("sport", "nomination_sport"):
        keep |= series(frame, logical).fillna("").astype(str).map(normalize).isin(selected)
    return frame.loc[keep]


def sport_values(frame: pd.DataFrame) -> list[str]:
    labels: dict[str, str] = {}
    for logical in ("sport", "nomination_sport"):
        for value in series(frame, logical).dropna().astype(str).str.strip():
            if value:
                labels.setdefault(normalize(value), value)
    return [labels[key] for key in sorted(labels, key=lambda item: labels[item].lower())]


def default_sports(values: list[str], report: str) -> list[str]:
    configured = LOU_SPORTS if report == "lou" else ENHANCED_EXCELLENCE_SPORTS
    selected = []
    for value in values:
        key = normalize(value)
        if key in configured or (
            report == "enhanced" and key.startswith("para ") and key[5:] in configured
        ):
            selected.append(value)
    return selected


def ordered_export(frame: pd.DataFrame) -> pd.DataFrame:
    internal = {"profile_id", "nomination_id", "row_kind", "enrollment_id", "row_id"}
    return frame[[name for name in frame.columns if name not in internal]].copy()


def fixed_export(frame: pd.DataFrame, specs: list[tuple[str, str]]) -> pd.DataFrame:
    output = pd.DataFrame(index=frame.index)
    for label, logical in specs:
        output[label] = series(frame, logical).values
    return output


MAIL_MERGE_ADULT_AGE = 19
MAIL_MERGE_CHUNK_SIZE = 450


def athlete_ages(frame: pd.DataFrame, today: date | None = None) -> pd.Series:
    """Age from the age field, calculated from date of birth where age is blank."""
    ages = pd.to_numeric(series(frame, "age"), errors="coerce")
    births = pd.to_datetime(series(frame, "dob"), errors="coerce")
    today = today or date.today()
    from_dob = births.map(
        lambda born: today.year - born.year - ((today.month, today.day) < (born.month, born.day))
        if pd.notna(born)
        else float("nan")
    )
    return ages.fillna(pd.to_numeric(from_dob, errors="coerce"))


def mail_merge_recipients(frame: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """Recipient email and contact type for each row.

    Athletes under 19 are contacted through their guardian; everyone else directly.
    If the preferred email is blank, the other one is used.
    """
    athlete = series(frame, "email").fillna("").astype(str).str.strip()
    guardian = series(frame, "guardian_email").fillna("").astype(str).str.strip()
    minor = athlete_ages(frame) < MAIL_MERGE_ADULT_AGE
    use_guardian = guardian.ne("") & (minor | athlete.eq(""))
    recipient = guardian.where(use_guardian, athlete)
    contact = pd.Series("Athlete", index=frame.index).where(~use_guardian, "Guardian")
    return recipient, contact.where(recipient.ne(""), "")


def mail_merge_artifact(output: pd.DataFrame, base_name: str) -> ReportArtifact:
    """One CSV, or a ZIP of CSVs when the list is longer than one mail-merge batch."""
    chunks = [
        output.iloc[index:index + MAIL_MERGE_CHUNK_SIZE]
        for index in range(0, len(output), MAIL_MERGE_CHUNK_SIZE)
    ]
    if len(chunks) <= 1:
        return csv_artifact(output, f"{base_name}.csv")
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for index, chunk in enumerate(chunks, 1):
            archive.writestr(f"{base_name}_part{index}.csv", chunk.to_csv(index=False))
    return ReportArtifact(buffer.getvalue(), f"{base_name}.zip", "application/zip", len(output))


def unclaimed_base(frame: pd.DataFrame, year: str) -> pd.DataFrame:
    result = remove_test_sports(fiscal_year(frame, year))
    result = result[truthy(series(result, "nomination_approved")) & falsey(series(result, "nomination_claimed"))]
    first = column(result, "first_name")
    last = column(result, "last_name")
    if last:
        result = result.sort_values(last, key=lambda values: values.fillna("").astype(str).str.lower())
    keys = [name for name in (first, last) if name]
    return result.drop_duplicates(keys, keep="first") if keys else result


def pending_unmatched(frame: pd.DataFrame, year: str) -> pd.DataFrame:
    work = remove_test_sports(fiscal_year(frame, year))
    kind = next((name for name in ("row_kind", "kind", "row_type", "type") if name in work), "")
    first, last = column(work, "first_name"), column(work, "last_name")
    if not kind or not first or not last:
        return pd.DataFrame()
    kinds = work[kind].fillna("").astype(str).str.lower()
    profiles, nominations = work[kinds == "profile"], work[kinds == "nomination"]
    pending = normalize
    status = column(profiles, "enrollment_status")
    if status:
        profiles = profiles[profiles[status].map(pending) == "pending"]
    matches = []
    for profile_index, profile in profiles.iterrows():
        profile_name = normalize(f"{profile.get(first, '')} {profile.get(last, '')}")
        for nomination_index, nomination in nominations.iterrows():
            nomination_name = normalize(f"{nomination.get(first, '')} {nomination.get(last, '')}")
            if not profile_name or not nomination_name:
                continue
            if difflib.SequenceMatcher(None, profile_name, nomination_name).ratio() < 0.82:
                continue
            profile_sport = normalize(series(pd.DataFrame([profile]), "sport").iloc[0])
            nomination_sport = normalize(series(pd.DataFrame([nomination]), "nomination_sport").iloc[0])
            if profile_sport and nomination_sport and "multisport" not in {profile_sport, nomination_sport} and profile_sport != nomination_sport:
                continue
            matches.extend((profile_index, nomination_index))
    if not matches:
        return pd.DataFrame()
    result = work.loc[list(dict.fromkeys(matches))]
    return fixed_export(
        result,
        [
            ("Role", "role"), ("First Name", "first_name"), ("Last Name", "last_name"),
            ("Guardian Email", "guardian_email"), ("Sport", "sport"),
            ("Enrollment Status", "enrollment_status"), ("Email", "email"),
        ],
    )


def duplicate_profiles(frame: pd.DataFrame) -> pd.DataFrame:
    work = remove_test_sports(frame)
    if not (column(work, "first_name") and column(work, "last_name") and column(work, "email")):
        return pd.DataFrame()
    identity = pd.DataFrame(
        {
            "first": series(work, "first_name").fillna("").astype(str).map(normalize),
            "last": series(work, "last_name").fillna("").astype(str).map(normalize),
            "sport": series(work, "sport").fillna("").astype(str).map(normalize),
            "email": series(work, "email").fillna("").astype(str).str.strip().str.lower(),
        },
        index=work.index,
    )
    identity = identity[(identity["first"] != "") & (identity["last"] != "") & (identity["email"] != "")]
    if identity.empty:
        return pd.DataFrame()
    email_counts = identity.groupby(["first", "last", "sport"])["email"].transform("nunique")
    duplicate_index = identity.index[email_counts > 1]
    if duplicate_index.empty:
        return pd.DataFrame()
    output = fixed_export(
        work.loc[duplicate_index],
        [
            ("Role", "role"), ("First Name", "first_name"), ("Last Name", "last_name"),
            ("Sport", "sport"), ("Email", "email"), ("Enrollment Status", "enrollment_status"),
            ("Nomination Claimed", "nomination_claimed"),
        ],
    )
    sort_columns = [name for name in ("Last Name", "First Name", "Sport", "Email") if name in output]
    return output.sort_values(sort_columns) if sort_columns else output


def guardian_email_match(frame: pd.DataFrame) -> pd.DataFrame:
    work = remove_test_sports(frame)
    email = series(work, "email").fillna("").astype(str).str.strip()
    guardian_email = series(work, "guardian_email").fillna("").astype(str).str.strip()
    work = work[(email != "") & (email.str.lower() == guardian_email.str.lower())]
    if work.empty:
        return pd.DataFrame()
    keys = [name for name in (column(work, "first_name"), column(work, "last_name"), column(work, "email")) if name]
    if keys:
        work = work.drop_duplicates(keys)
    output = fixed_export(
        work,
        [
            ("Role", "role"), ("First Name", "first_name"), ("Last Name", "last_name"),
            ("Email", "email"), ("Guardian First Name", "guardian_first_name"),
            ("Guardian Last Name", "guardian_last_name"), ("Guardian Email", "guardian_email"),
            ("Age", "age"), ("Sport", "sport"), ("Enrollment Status", "enrollment_status"),
        ],
    )
    sort_columns = [name for name in ("Last Name", "First Name") if name in output]
    return output.sort_values(sort_columns) if sort_columns else output


def national_inventory(frame: pd.DataFrame, year: str) -> pd.DataFrame:
    work = add_level_category(remove_test_sports(fiscal_year(frame, year)))
    work = work[truthy(series(work, "nomination_approved"))]
    work = work[series(work, "level_category").isin({"Canadian Elite", "Canadian Development"})]
    dedup = [column(work, name) for name in ("role", "first_name", "last_name", "sport", "nomination_claimed")]
    dedup = [name for name in dedup if name]
    if dedup:
        work = work.drop_duplicates(dedup)
    output = fixed_export(
        work,
        [
            ("Season", "season"), ("Training Group", "training_group"), ("Role", "role"),
            ("First Name", "first_name"), ("Last Name", "last_name"), ("Sport", "sport"),
            ("Enrollment Status", "enrollment_status"),
            ("Nomination Approved", "nomination_approved"),
            ("Nomination Claimed", "nomination_claimed"), ("Carding Level", "athlete_carding"),
            ("Level Category", "level_category"), ("Sex of Competition", "sex_of_competition"),
            ("NCCP Number", "nccp_number"),
        ],
    )
    missing = output["Season"].fillna("").astype(str).str.strip() == ""
    output.loc[missing, "Season"] = output.loc[missing, "Sport"].map(
        lambda sport: SEASON_BY_SPORT.get(normalize(sport), "Summer" if normalize(sport) else "")
    )
    return output


def viasport_base(
    frame: pd.DataFrame, role: str, year: str, level: str = ""
) -> pd.DataFrame:
    work = add_level_category(remove_test_sports(fiscal_year(frame, year)))
    work = work[series(work, "role").map(normalize) == normalize(role)]
    work = work[truthy(series(work, "nomination_approved"))]
    if level:
        work = work[series(work, "level_category").map(normalize) == normalize(level)]
    return work


def sc_carded(frame: pd.DataFrame, year: str = "") -> pd.DataFrame:
    work = viasport_base(frame, "athlete", year, "Canadian Elite")
    output = fixed_export(
        work,
        [
            ("Sport", "sport"), ("Gender", "gender"), ("Card level", "athlete_carding"),
            ("Nomination Claimed", "nomination_claimed"),
        ],
    )
    name = (series(work, "first_name").fillna("").astype(str).str.strip() + " " + series(work, "last_name").fillna("").astype(str).str.strip()).str.strip()
    output.insert(1, "Name", name.values)
    province = series(work, "current_campus").fillna("").astype(str).str.strip().map(
        lambda value: "British Columbia" if value else ""
    )
    output.insert(4, "Province", province.values)
    return output[["Sport", "Name", "Gender", "Card level", "Province", "Nomination Claimed"]]


def high_performance(frame: pd.DataFrame, year: str = "") -> pd.DataFrame:
    return fixed_export(
        viasport_base(frame, "athlete", year),
        [
            ("Nomination ID", "nomination_id"), ("Sport", "sport"),
            ("Card level", "athlete_carding"), ("Sex of Competition", "sex_of_competition"),
            ("Gender", "gender"), ("Age", "age"), ("Discipline", "discipline"),
            ("Current City Campus", "current_campus"), ("Ethnicity", "ethnicity"),
            ("Nomination Claimed", "nomination_claimed"),
        ],
    )


def coaches(frame: pd.DataFrame, year: str = "") -> pd.DataFrame:
    return fixed_export(
        viasport_base(frame, "coach", year),
        [
            ("Nomination ID", "nomination_id"), ("Sport", "sport"), ("Gender", "gender"),
            ("Current City Campus", "current_campus"), ("Ethnicity", "ethnicity"),
            ("Nomination Claimed", "nomination_claimed"), ("NCCP Number", "nccp_number"),
        ],
    )


def conversion(frame: pd.DataFrame, year: str = "") -> pd.DataFrame:
    work = viasport_base(frame, "athlete", year)
    sport = column(work, "sport")
    if not sport:
        return pd.DataFrame(columns=["Sport", "Total Athletes", "Average Age", "Sex of Competition (F/M/X)"])
    rows = []
    for sport_name, group in work.groupby(sport):
        age = pd.to_numeric(series(group, "age"), errors="coerce")
        sex = series(group, "sex_of_competition").map(normalize)
        female = int(sex.isin({"f", "female", "woman", "women", "girl", "girls"}).sum())
        male = int(sex.isin({"m", "male", "man", "men", "boy", "boys"}).sum())
        rows.append({
            "Sport": sport_name,
            "Total Athletes": len(group),
            "Average Age": "" if pd.isna(age.mean()) else f"{age.mean():.1f}",
            "Sex of Competition (F/M/X)": f"{female}/{male}/{len(group) - female - male}",
        })
    return pd.DataFrame(rows).sort_values("Sport", key=lambda values: values.astype(str).str.lower())


def _copy_row_style(ws, source_row: int, target_row: int, max_column: int) -> None:
    ws.row_dimensions[target_row].height = ws.row_dimensions[source_row].height
    for column_index in range(1, max_column + 1):
        source = ws.cell(source_row, column_index)
        target = ws.cell(target_row, column_index)
        if source.has_style:
            target._style = copy(source._style)
        target.number_format = source.number_format
        target.alignment = copy(source.alignment)
        target.protection = copy(source.protection)


def _prepare_template_rows(
    ws, data_start: int, original_end: int, total_row: int, needed: int, max_column: int
) -> tuple[int, int]:
    capacity = original_end - data_start + 1
    extra = max(0, needed - capacity)
    if extra:
        ws.insert_rows(total_row, amount=extra)
        for row_index in range(total_row, total_row + extra):
            _copy_row_style(ws, original_end, row_index, max_column)
    data_end = original_end + extra
    new_total_row = total_row + extra
    for row_index in range(data_start, data_end + 1):
        for column_index in range(1, max_column + 1):
            ws.cell(row_index, column_index).value = None
    return data_end, new_total_row


def _template_column(ws, header: str, header_row: int = 6) -> int | None:
    target = normalize(header)
    for cell in ws[header_row]:
        if normalize(cell.value) == target:
            return cell.column
    return None


def _safe_filename(value: str) -> str:
    cleaned = re.sub(r'[<>:"/\\|?*]+', "-", str(value)).strip().strip(".")
    return cleaned or "Unknown Sport"


def _short_year(value: str) -> str:
    match = re.fullmatch(r"(\d{4})-(\d{4})", str(value or "").strip())
    return f"{match.group(1)}-{match.group(2)[2:]}" if match else str(value or "All Years")


def available_fiscal_years(frame: pd.DataFrame) -> list[str]:
    name = column(frame, "nomination_fiscal_year")
    if not name:
        return []
    return sorted(
        value
        for value in frame[name].dropna().astype(str).str.strip().unique()
        if value
    )


def previous_fiscal_year(year: str, available: list[str] | None = None) -> str:
    """Return the fiscal year before ``year``, keeping its format (2025-2026 or 2025-26)."""
    text = str(year or "").strip()
    match = re.fullmatch(r"(\d{4})\s*-\s*(\d{2}|\d{4})", text)
    if match:
        start = int(match.group(1))
        end = str(start) if len(match.group(2)) == 4 else str(start)[2:]
        return f"{start - 1}-{end}"
    if available and text in available:
        index = available.index(text)
        return available[index - 1] if index else ""
    return ""


def ee_sport_key(sport: str) -> str:
    key = normalize(sport).removeprefix("para ")
    return EE_SPORT_ALIASES.get(key, key)


def default_ee_fiscal_mode(sports: list[str]) -> str | None:
    """Return the scheduled fiscal-year mode for the first sport that has one."""
    for sport in sports:
        mode = EE_DEFAULT_FISCAL_MODES.get(ee_sport_key(sport))
        if mode:
            return mode
    return None


def ee_fiscal_years(mode: str, current_year: str, available: list[str] | None = None) -> list[str]:
    if not current_year:
        return []
    previous = previous_fiscal_year(current_year, available)
    if mode == "previous":
        return [previous] if previous else []
    if mode == "both":
        return [value for value in (previous, current_year) if value]
    if mode in ("", "current"):
        return [current_year]
    return [mode]


def ee_sport_groups(
    frame: pd.DataFrame,
    chosen_sports: list[str],
) -> list[tuple[str, list[str]]]:
    available = sport_values(frame)
    by_normalized = {normalize(value): value for value in available}
    selected = {normalize(value) for value in chosen_sports}
    groups: list[tuple[str, list[str]]] = []
    grouped: set[str] = set()

    wheelchair_members = {
        value
        for value in available
        if normalize(value) in {
            "wheelchair rugby",
            "wheelchair tennis",
            "wheelchair athletics",
        }
    }
    selected_wheelchair = selected.intersection({normalize(value) for value in wheelchair_members})
    if selected_wheelchair:
        groups.append(("Wheelchair sports", sorted(wheelchair_members)))
        grouped.update(normalize(value) for value in wheelchair_members)

    for sport in chosen_sports:
        normalized = normalize(sport)
        if normalized in grouped:
            continue
        if normalized == "wheelchair basketball":
            groups.append((sport, [sport]))
            grouped.add(normalized)
            continue
        parent = normalized.removeprefix("para ")
        members = [
            by_normalized[value]
            for value in (parent, f"para {parent}")
            if value in by_normalized
        ]
        if len(members) > 1:
            label = by_normalized[parent]
            groups.append((label, members))
            grouped.update(normalize(value) for value in members)
        else:
            groups.append((sport, [sport]))
            grouped.add(normalized)

    order = {normalize(value): index for index, value in enumerate(chosen_sports)}
    groups.sort(
        key=lambda group: min(
            order.get(normalize(member), len(chosen_sports))
            for member in group[1]
        )
    )
    return groups


def _ee_base(frame: pd.DataFrame) -> pd.DataFrame:
    base = remove_test_sports(frame)
    return base[truthy(series(base, "nomination_approved"))]


def ee_workbook_groups(frame: pd.DataFrame, sports: list[str]) -> list[tuple[str, list[str]]]:
    """Workbook label and member sports for the selected EE sports."""
    return ee_sport_groups(_ee_base(frame), sports)


@dataclass
class EEWorkbookPlan:
    label: str
    members: list[str]
    mode: str
    years: list[str]
    athletes: pd.DataFrame
    coaches: pd.DataFrame


def _ee_registered(frame: pd.DataFrame) -> pd.Series:
    return truthy(series(frame, "nomination_claimed")) & (
        series(frame, "enrollment_status").fillna("").astype(str).map(normalize) == "active"
    )


def _ee_people(frame: pd.DataFrame, match_nccp: bool = False) -> pd.DataFrame:
    """Sort by last name and collapse repeat nominations of the same person.

    Only rows that are certainly the same person are merged (same name, and the
    same NCCP number for coaches), keeping the latest fiscal year. Rows without
    a name are never merged, so no nomination is dropped on uncertain matches.
    """
    result = frame.copy()
    fiscal_column = column(result, "nomination_fiscal_year")
    if fiscal_column:
        result = result.sort_values(fiscal_column, ascending=False, kind="stable")
    last_name = column(result, "last_name")
    if last_name:
        result = result.sort_values(
            last_name,
            key=lambda values: values.fillna("").astype(str).str.lower(),
            kind="stable",
        )
    names = series(result, "first_name").fillna("").astype(str).map(normalize) + "|" + series(
        result, "last_name"
    ).fillna("").astype(str).map(normalize)
    key = names
    if match_nccp:
        key = key + "|" + series(result, "nccp_number").map(normalize_nccp)
    return result[~(key.duplicated() & (names != "|"))]


def plan_ee_workbooks(
    frame: pd.DataFrame,
    year: str,
    sports: list[str] | None,
    sport_years: dict[str, str] | None = None,
) -> list[EEWorkbookPlan]:
    """Resolve each EE workbook's sports, fiscal years, athletes, and coaches.

    ``year`` is the Current fiscal year (blank uses the latest in the data).
    ``sport_years`` maps a workbook label or member sport to current/previous/both.
    """
    modes = sport_years or {}
    available = available_fiscal_years(frame)
    current_year = year or (available[-1] if available else "")
    base = _ee_base(frame)
    chosen_sports = sports or list(modes) or sport_values(base)
    fiscal_column = column(base, "nomination_fiscal_year")
    plans = []
    for label, members in ee_sport_groups(base, chosen_sports):
        mode = modes.get(label) or next(
            (modes[member] for member in members if modes.get(member)), ""
        )
        years = ee_fiscal_years(mode, current_year, available)
        if not years:
            work = base.iloc[0:0]
        elif fiscal_column:
            wanted = {normalize(value) for value in years}
            work = base[base[fiscal_column].fillna("").astype(str).map(normalize).isin(wanted)]
        else:
            work = base
        sport_frame = selected_sports(work, members)
        roles = series(sport_frame, "role").fillna("").astype(str).map(normalize)
        athletes = sport_frame[roles == "athlete"]
        athlete_levels = series(athletes, "athlete_carding").fillna("").astype(str).map(normalize)
        athletes = athletes[athlete_levels.isin(EE_ATHLETE_CARDING)]
        plans.append(
            EEWorkbookPlan(
                label=label,
                members=members,
                mode=mode,
                years=years,
                athletes=_ee_people(athletes),
                coaches=_ee_people(sport_frame[roles == "coach"], match_nccp=True),
            )
        )
    return plans


def normalize_nccp(value) -> str:
    """Canonical NCCP number: digits only, no leading zeros or trailing '.0'."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip()
    text = re.sub(r"\.0+$", "", text)
    if not text.isdigit():
        return ""
    return text.lstrip("0") or ""


def _cac_text(value) -> str:
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return ""
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    return str(value).strip()


def parse_cac_report(contents: str, filename: str) -> tuple[dict[str, dict[str, str]], dict[str, int]]:
    """Read a base64 Dash upload of the CAC coach report.

    Returns ``{nccp_number: {template_header: value}}`` plus row statistics. The
    header row is located automatically (the report has a formula row above it).
    """
    try:
        _, encoded = contents.split(",", 1)
        payload = base64.b64decode(encoded)
    except (AttributeError, ValueError, TypeError) as exc:
        raise ValueError(f"{filename}: the uploaded file could not be decoded.") from exc
    suffix = Path(filename or "").suffix.lower()
    try:
        if suffix == ".csv":
            sheets = {"csv": pd.read_csv(io.BytesIO(payload), header=None, dtype=object)}
        elif suffix in {".xlsx", ".xlsm", ".xls"}:
            sheets = pd.read_excel(io.BytesIO(payload), sheet_name=None, header=None, dtype=object)
        else:
            raise ValueError("upload the CAC coach report as an Excel or CSV file.")
    except Exception as exc:
        raise ValueError(f"{filename}: {exc}") from exc

    wanted = {normalize(source) for source, _ in CAC_TEMPLATE_COLUMNS.values()}
    for raw in sheets.values():
        for header_index in range(min(len(raw), 15)):
            headers = [normalize(value) if isinstance(value, str) else "" for value in raw.iloc[header_index]]
            nccp_positions = [i for i, name in enumerate(headers) if name in {"nccp_number", "nccp number", "nccp #", "nccp"}]
            if not nccp_positions or not wanted.intersection(headers):
                continue
            frame = raw.iloc[header_index + 1:]
            nccp_values = frame.iloc[:, nccp_positions[0]]
            sources = {
                header: (headers.index(normalize(source)) if normalize(source) in headers else None, empty)
                for header, (source, empty) in CAC_TEMPLATE_COLUMNS.items()
            }
            lookup: dict[str, dict[str, str]] = {}
            rows = 0
            for position, nccp in enumerate(nccp_values):
                key = normalize_nccp(nccp)
                if not key:
                    continue
                rows += 1
                record = lookup.setdefault(key, {header: "" for header in CAC_TEMPLATE_COLUMNS})
                for header, (index, _) in sources.items():
                    value = _cac_text(frame.iat[position, index]) if index is not None else ""
                    if value and not record[header]:
                        record[header] = value
            missing = [source for header, (source, _) in CAC_TEMPLATE_COLUMNS.items() if sources[header][0] is None]
            return lookup, {
                "rows": rows,
                "coaches": len(lookup),
                "skipped": int(len(frame) - rows),
                "missing_columns": missing,
            }
    raise ValueError(
        f"{filename}: no header row with an NCCP number column and CAC columns "
        f"({', '.join(source for source, _ in CAC_TEMPLATE_COLUMNS.values())}) was found."
    )


def cac_values(nccp, cac_lookup: dict[str, dict[str, str]]) -> dict[str, str]:
    """Values for the four CAC Locker Status columns for one coach."""
    record = cac_lookup.get(normalize_nccp(nccp))
    if record is None:
        return {}
    return {
        header: record.get(header) or empty
        for header, (_, empty) in CAC_TEMPLATE_COLUMNS.items()
    }


def ee_cac_matches(plan: "EEWorkbookPlan", cac_lookup: dict[str, dict[str, str]]) -> tuple[int, list[str]]:
    """Number of the workbook's coaches found in the CAC report, and those not found."""
    name_columns = [column(plan.coaches, "first_name"), column(plan.coaches, "last_name")]
    nccp_column = column(plan.coaches, "nccp_number")
    matched = 0
    unmatched = []
    for _, person in plan.coaches.iterrows():
        nccp = person.get(nccp_column, "") if nccp_column else ""
        if normalize_nccp(nccp) in cac_lookup:
            matched += 1
        else:
            name = " ".join(str(person.get(name, "") or "") for name in name_columns if name).strip()
            unmatched.append(f"{name or 'Unnamed coach'} ({_cac_text(nccp) or 'no NCCP'})")
    return matched, unmatched


def ee_shared_nccp_coaches(plans: list[EEWorkbookPlan]) -> list[str]:
    """Coaches listed under different names with the same NCCP number in one workbook."""
    findings = []
    for plan in plans:
        coaches = plan.coaches
        keys = series(coaches, "nccp_number").map(normalize_nccp)
        names = (
            series(coaches, "first_name").fillna("").astype(str).str.strip()
            + " "
            + series(coaches, "last_name").fillna("").astype(str).str.strip()
        )
        for key in sorted(set(keys[keys.ne("") & keys.duplicated(keep=False)])):
            people = " / ".join(names[keys == key])
            findings.append(f"{plan.label}: {people} ({key})")
    return findings


def ee_workbook_filename(plan: EEWorkbookPlan) -> str:
    year_label = ",".join(_short_year(value) for value in plan.years)
    return f"EE_{_safe_filename(plan.label)}_{year_label or 'All Years'}.xlsx"


def ee_review_rows(
    plans: list[EEWorkbookPlan], cac_lookup: dict[str, dict[str, str]] | None = None
) -> list[dict[str, object]]:
    rows = []
    for plan in plans:
        if not plan.years:
            status = "Omitted: no previous fiscal year could be determined"
        elif plan.athletes.empty and plan.coaches.empty:
            status = "Omitted: no qualifying athletes or coaches"
        else:
            status = "Included"
        rows.append({
            "Workbook": plan.label,
            "Sports": ", ".join(plan.members),
            "Fiscal years": ", ".join(plan.years) or "—",
            "Athletes": len(plan.athletes),
            "Registered athletes": int(_ee_registered(plan.athletes).sum()),
            "Coaches": len(plan.coaches),
            "Registered coaches": int(_ee_registered(plan.coaches).sum()),
            "CAC matches": (
                f"{ee_cac_matches(plan, cac_lookup)[0]} of {len(plan.coaches)}"
                if cac_lookup is not None
                else "No CAC file"
            ),
            "Status": status,
        })
    return rows


EE_ATHLETE_LIST_COLUMNS = ["Last Name", "First Name", "Sport", "Email", "Guardian Email"]


EE_MAIL_MERGE_COLUMNS = ["First Name", "Last Name", "To", "Contact", "Sport"]


def _ee_registered_rows(plans: list[EEWorkbookPlan]) -> pd.DataFrame:
    """Source rows for actively registered athletes across all workbooks, one per athlete and sport."""
    frames = [plan.athletes[_ee_registered(plan.athletes)] for plan in plans]
    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        return pd.DataFrame()
    rows = pd.concat(frames, ignore_index=True)
    key = pd.concat(
        [
            series(rows, name).fillna("").astype(str).map(normalize)
            for name in ("last_name", "first_name", "sport", "email")
        ],
        axis=1,
    )
    return rows.loc[~key.duplicated()].reset_index(drop=True)


def ee_registered_athletes(plans: list[EEWorkbookPlan]) -> pd.DataFrame:
    """Actively registered athletes (claimed nomination, active enrollment) across all workbooks."""
    rows = _ee_registered_rows(plans)
    if rows.empty:
        return pd.DataFrame(columns=EE_ATHLETE_LIST_COLUMNS)
    result = fixed_export(rows, [
        ("Last Name", "last_name"),
        ("First Name", "first_name"),
        ("Sport", "sport"),
        ("Email", "email"),
        ("Guardian Email", "guardian_email"),
    ]).fillna("")
    return result.sort_values(
        ["Sport", "Last Name", "First Name"],
        key=lambda values: values.astype(str).str.lower(),
        ignore_index=True,
    )


def ee_mail_merge(plans: list[EEWorkbookPlan]) -> tuple[pd.DataFrame, list[str]]:
    """Mail-merge rows for the registered athlete list, and athletes with no contact email.

    One row per recipient and athlete; an athlete in several sports gets one row
    with the sports joined.
    """
    rows = _ee_registered_rows(plans)
    if rows.empty:
        return pd.DataFrame(columns=EE_MAIL_MERGE_COLUMNS), []
    recipient, contact = mail_merge_recipients(rows)
    output = fixed_export(
        rows, [("First Name", "first_name"), ("Last Name", "last_name"), ("Sport", "sport")]
    ).fillna("").astype(str)
    output["To"] = recipient.values
    output["Contact"] = contact.values
    no_email = output[output["To"] == ""]
    missing = sorted(
        {f"{row['First Name']} {row['Last Name']} ({row['Sport']})".strip() for _, row in no_email.iterrows()}
    )
    output = output[output["To"] != ""]
    if output.empty:
        return pd.DataFrame(columns=EE_MAIL_MERGE_COLUMNS), missing
    keys = [
        output[name].map(normalize).rename(f"_{index}")
        for index, name in enumerate(("First Name", "Last Name", "To"))
    ]
    merged = (
        output.groupby(keys, sort=False)
        .agg({
            "First Name": "first",
            "Last Name": "first",
            "To": "first",
            "Contact": "first",
            "Sport": lambda values: "; ".join(dict.fromkeys(values)),
        })
        .reset_index(drop=True)
    )
    merged = merged.sort_values(
        ["Last Name", "First Name"],
        key=lambda values: values.str.lower(),
        ignore_index=True,
    )
    return merged[EE_MAIL_MERGE_COLUMNS], missing


def build_ee_mail_merge(
    frame: pd.DataFrame,
    year: str,
    sports: list[str] | None,
    sport_years: dict[str, str] | None = None,
) -> ReportArtifact:
    output, _ = ee_mail_merge(plan_ee_workbooks(frame, year, sports, sport_years))
    return mail_merge_artifact(output, "EE_Registered_Athletes_Mail_Merge")


def build_ee_athlete_list(
    frame: pd.DataFrame,
    year: str,
    sports: list[str] | None,
    sport_years: dict[str, str] | None = None,
) -> ReportArtifact:
    plans = plan_ee_workbooks(frame, year, sports, sport_years)
    athletes = ee_registered_athletes(plans)
    year_sets = {tuple(plan.years) for plan in plans if not plan.athletes.empty}
    year_label = (
        ",".join(_short_year(value) for value in next(iter(year_sets)))
        if len(year_sets) == 1
        else "Mixed"
    )
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        athletes.to_excel(writer, sheet_name="Registered Athletes", index=False)
        sheet = writer.sheets["Registered Athletes"]
        for letter, width in zip("ABCDE", (20, 18, 24, 34, 34)):
            sheet.column_dimensions[letter].width = width
        sheet.freeze_panes = "A2"
        if len(athletes):
            sheet.auto_filter.ref = f"A1:E{len(athletes) + 1}"
    return ReportArtifact(
        output.getvalue(),
        f"EE_Registered_Athletes_{year_label}.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        len(athletes),
    )


def build_ee_sport_templates(
    frame: pd.DataFrame,
    year: str,
    sports: list[str] | None,
    template_path: Path | None = None,
    sport_years: dict[str, str] | None = None,
    cac_lookup: dict[str, dict[str, str]] | None = None,
) -> ReportArtifact:
    template = template_path or Path(__file__).resolve().parent / "assets" / "EE_Sport_Template_Blank.xlsx"
    if not template.exists():
        raise ValueError(f"EE workbook template is missing: {template}")
    archive_buffer = io.BytesIO()
    total_people = 0
    year_sets: set[tuple[str, ...]] = set()

    with zipfile.ZipFile(archive_buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for plan in plan_ee_workbooks(frame, year, sports, sport_years):
            athletes, coaches_frame = plan.athletes, plan.coaches
            if athletes.empty and coaches_frame.empty:
                continue

            workbook = load_workbook(template)
            # Lets the EE Sport Reports tab identify a returned workbook even if it is renamed.
            workbook.properties.title = "EE Sport Template"
            workbook.properties.subject = plan.label
            workbook.properties.keywords = ",".join(_short_year(value) for value in plan.years) or "All Years"
            athlete_sheet = workbook["Athletes"]
            athlete_registered_column = _template_column(
                athlete_sheet, "Registered with CSI Pacific"
            )
            if athlete_registered_column is None:
                raise ValueError(
                    "The EE workbook Athletes sheet is missing the "
                    "'Registered with CSI Pacific' column."
                )
            athlete_end, athlete_total = _prepare_template_rows(
                athlete_sheet, 7, 38, 39, len(athletes), athlete_sheet.max_column
            )
            athlete_registered = _ee_registered(athletes)
            for offset, ((_, person), registered) in enumerate(
                zip(athletes.iterrows(), athlete_registered), start=7
            ):
                athlete_sheet.cell(offset, 1).value = person.get(column(athletes, "last_name"), "")
                athlete_sheet.cell(offset, 2).value = person.get(column(athletes, "first_name"), "")
                athlete_sheet.cell(offset, 3).value = person.get(column(athletes, "sex_of_competition"), "")
                athlete_sheet.cell(offset, 4).value = person.get(column(athletes, "athlete_carding"), "")
                athlete_sheet.cell(offset, athlete_registered_column).value = (
                    "YES" if registered else None
                )
            for column_index in range(5, athlete_registered_column + 1):
                athlete_sheet.cell(athlete_total, column_index).value = (
                    f'=COUNTIF({athlete_sheet.cell(7, column_index).coordinate}:'
                    f'{athlete_sheet.cell(athlete_end, column_index).coordinate},"YES")'
                )
            athlete_sheet.auto_filter.ref = (
                f"A6:{athlete_sheet.cell(6, athlete_sheet.max_column).column_letter}{athlete_end}"
            )

            coach_sheet = workbook["Coaches"]
            coach_registered_column = _template_column(
                coach_sheet, "Registered with CSI Pacific"
            )
            if coach_registered_column is None:
                raise ValueError(
                    "The EE workbook Coaches sheet is missing the "
                    "'Registered with CSI Pacific' column."
                )
            cac_columns = {
                header: _template_column(coach_sheet, header) for header in CAC_TEMPLATE_COLUMNS
            }
            if cac_lookup is not None and None in cac_columns.values():
                missing = [header for header, index in cac_columns.items() if index is None]
                raise ValueError(
                    "The EE workbook Coaches sheet is missing the CAC column(s): "
                    + ", ".join(missing)
                )
            coach_end, coach_total = _prepare_template_rows(
                coach_sheet, 7, 21, 22, len(coaches_frame), coach_sheet.max_column
            )
            coach_registered = _ee_registered(coaches_frame)
            for offset, ((_, person), registered) in enumerate(
                zip(coaches_frame.iterrows(), coach_registered), start=7
            ):
                coach_sheet.cell(offset, 1).value = person.get(column(coaches_frame, "last_name"), "")
                coach_sheet.cell(offset, 2).value = person.get(column(coaches_frame, "first_name"), "")
                coach_sheet.cell(offset, 3).value = person.get(column(coaches_frame, "nccp_number"), "")
                coach_sheet.cell(offset, coach_registered_column).value = (
                    "YES" if registered else None
                )
                if cac_lookup is not None:
                    values = cac_values(person.get(column(coaches_frame, "nccp_number"), ""), cac_lookup)
                    for header, value in values.items():
                        coach_sheet.cell(offset, cac_columns[header]).value = value
            for column_index in range(6, coach_registered_column + 1):
                coach_sheet.cell(coach_total, column_index).value = (
                    f'=COUNTIF({coach_sheet.cell(7, column_index).coordinate}:'
                    f'{coach_sheet.cell(coach_end, column_index).coordinate},"YES")'
                )
            coach_sheet.auto_filter.ref = (
                f"A6:{coach_sheet.cell(6, coach_sheet.max_column).column_letter}{coach_end}"
            )

            output = io.BytesIO()
            workbook.save(output)
            archive.writestr(ee_workbook_filename(plan), output.getvalue())
            total_people += len(athletes) + len(coaches_frame)
            year_sets.add(tuple(plan.years))

    if not year_sets:
        return ReportArtifact(b"", "EE_Sport_Templates.zip", "application/zip", 0)
    zip_label = (
        ",".join(_short_year(value) for value in next(iter(year_sets)))
        if len(year_sets) == 1
        else "Mixed"
    )
    return ReportArtifact(
        archive_buffer.getvalue(),
        f"EE_Sport_Templates_{zip_label}.zip",
        "application/zip",
        total_people,
    )


def csv_artifact(frame: pd.DataFrame, filename: str) -> ReportArtifact:
    return ReportArtifact(frame.to_csv(index=False).encode("utf-8"), filename, row_count=len(frame))


def build_report(
    frame: pd.DataFrame,
    report_key: str,
    fiscal_year_value: str = "",
    lou_sports: list[str] | None = None,
    enhanced_sports: list[str] | None = None,
) -> ReportArtifact:
    base_name = f"list_generator_7000_{report_key}"
    if report_key == "pending_unmatched":
        output = pending_unmatched(frame, fiscal_year_value)
    elif report_key == "duplicate_profiles":
        output = duplicate_profiles(frame)
    elif report_key == "guardian_email_match":
        output = guardian_email_match(frame)
    elif report_key == "unclaimed_nominations":
        work = unclaimed_base(frame, fiscal_year_value)
        output = fixed_export(work, [
            ("Role", "role"), ("First Name", "first_name"), ("Last Name", "last_name"),
            ("Email", "email"), ("Guardian First Name", "guardian_first_name"),
            ("Guardian Last Name", "guardian_last_name"), ("Guardian Email", "guardian_email"),
            ("Age", "age"),
        ])
    elif report_key == "mail_merge":
        work = unclaimed_base(frame, fiscal_year_value)
        recipient, _ = mail_merge_recipients(work)
        output = fixed_export(work, [("First Name", "first_name"), ("Last Name", "last_name")])
        output["To"] = recipient.values
        return mail_merge_artifact(output, base_name)
    elif report_key == "national_inventory":
        output = national_inventory(frame, fiscal_year_value)
    elif report_key == "active_athletes":
        work = add_level_category(remove_test_sports(fiscal_year(frame, fiscal_year_value)))
        output = ordered_export(work[truthy(series(work, "nomination_claimed"))])
    elif report_key == "viasport_lou":
        output = ordered_export(add_level_category(selected_sports(remove_test_sports(fiscal_year(frame, fiscal_year_value)), lou_sports)))
    elif report_key == "enhanced_excellence":
        output = ordered_export(add_level_category(selected_sports(remove_test_sports(fiscal_year(frame, fiscal_year_value)), enhanced_sports)))
    elif report_key == "sc_carded":
        output = sc_carded(frame, fiscal_year_value)
    elif report_key == "high_performance":
        output = high_performance(frame, fiscal_year_value)
    elif report_key == "coaches":
        output = coaches(frame, fiscal_year_value)
    elif report_key == "conversion":
        output = conversion(frame, fiscal_year_value)
    elif report_key == "viasport_all":
        reports = {
            "sc_carded_2A": sc_carded(frame, fiscal_year_value),
            "high_performance_2B": high_performance(frame, fiscal_year_value),
            "coaches_4B": coaches(frame, fiscal_year_value),
        }
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
            for name, report in reports.items():
                archive.writestr(f"list_generator_7000_{name}.csv", report.to_csv(index=False))
        return ReportArtifact(buffer.getvalue(), "list_generator_7000_viasport_reports.zip", "application/zip", sum(len(report) for report in reports.values()))
    else:
        raise ValueError("Unknown prebuilt report.")
    return csv_artifact(output, f"{base_name}.csv")
