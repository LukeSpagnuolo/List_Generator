"""Add Registration nominations to the Conversion dataset (Conversion_Data_2026_final.csv).

Rules (agreed with the data owner, Nov 2026; see the Conversion repo's
DATA_CLEANUP_2026-11-06.md and scripts/clean_dataset.py):

* Same athlete only when name + date of birth + sport (sport *group*) agree.
  A Registration profile ID, once stored, also identifies the athlete.
* Excluded: coaches, test sports, unapproved nominations, and carding levels
  that are not a conversion program (e.g. "GamePlan - Retired").
* Fiscal year 2026-2027 is Year 2027.
* One row per athlete / sport / year; the higher program level wins.
* Add-only: existing rows are never deleted, except a Multisport (CSS) row when
  the same athlete has another sport in that year (that row gets CSS = YES).
* DOBs: the nomination DOB has priority. A confirmed athlete (profile ID, reviewed
  as same person, or a DOB typo in their newest year) gets the newest nomination's
  DOB on all rows; for other matches it only fills blank DOBs.
* Anything that can't be confirmed goes to review with a Decision: Hold,
  Same person, or Different person. Decisions are saved
  (conversion_review_decisions.csv) so a nomination is only reviewed once.
"""

from __future__ import annotations

import base64
import io
import re
import unicodedata
import zipfile
from dataclasses import dataclass, field
from datetime import date
from difflib import SequenceMatcher
from typing import Any
from urllib.parse import quote

import pandas as pd
import requests


LEVEL = {"Prov Dev 3": 1, "Prov Dev 2": 2, "Prov Dev 1": 3, "Uncarded": 4, "SC Carded": 5}
CARDING_TO_PROGRAM = {
    "prov dev 1": "Prov Dev 1",
    "prov dev 2": "Prov Dev 2",
    "prov dev 3": "Prov Dev 3",
    "nso affiliated (uncarded)": "Uncarded",
    "pso affiliated (uncarded)": "Uncarded",
    **{level: "SC Carded" for level in ("sr", "sri", "sr1", "sr2", "d", "di", "c1", "c1i")},
}
SPORT_RENAME = {"Hockey": "Ice Hockey"}
# Treated as one sport for matching, Years Targeted and Convert Year. Labels are kept as recorded.
# Kept separate on purpose: Wheelchair Basketball/Rugby/Tennis/Athletics, Biathlon vs Cross-Country Ski.
SPORT_GROUP = {
    "Para Athletics": "Athletics",
    "Para Swimming": "Swimming",
    "Para Alpine Ski": "Alpine Ski",
    "Para Cross-Country Ski": "Cross-Country Ski",
    "Para Snowboard": "Snowboard",
    "Para Rowing": "Rowing",
    "Para Cycling": "Cycling",
    "Para Ice Hockey": "Ice Hockey",
    "Para Triathlon": "Triathlon",
    "Para Sailing": "Sailing",
    "Para Badminton": "Badminton",
    "Para Equestrian": "Equestrian",
    "Shooting Para Sport": "Shooting",
    "Wheelchair Curling": "Curling",
    "Sitting Volleyball": "Volleyball",
    "Beach Volleyball": "Volleyball",
    "Trampoline": "Artistic Gymnastics",
    "Ski Cross": "Alpine Ski",
    "Wakeboard": "Water Ski",
}
SEX = {"male": "M", "female": "F", "any/combined": "X"}
CSS_ORGANIZATION = "canadian sport school"
MULTISPORT = "multisport"
TRUE_VALUES = {"true", "1", "yes", "y"}
FALSE_VALUES = {"false", "0", "no", "n"}

DATASET_COLUMNS = [
    "Sport", "First Name", "Last Name", "Gender", "Date of Birth", "Year", "Program",
    "Full_Name", "CSS", "Years Targeted", "Convert Year",
]
PROFILE_ID_COLUMN = "Profile_ID"

MIN_AGE, MAX_AGE = 9, 60  # outside this, a nomination DOB is treated as a data-entry error

HOLD, SAME, DIFFERENT = "Hold", "Same person", "Different person"
DECISIONS = (HOLD, SAME, DIFFERENT)

STATUS_NEW = "New athlete"
STATUS_NEW_SPORT = "Existing athlete, new sport"
STATUS_MATCHED = "Matched"
STATUS_MATCHED_ID = "Matched (profile ID)"
STATUS_MATCHED_TYPO = "Matched (DOB typo)"
STATUS_MATCHED_OLDER = "Matched (older year)"
REVIEW_DOB_DIFFERS = "Same name and sport, different DOB"
REVIEW_NO_DATASET_DOB = "Same name and sport, dataset has no DOB"
REVIEW_NAME_SPELLING = "Same DOB and sport, name spelled differently"
REVIEW_NO_NOMINATION_DOB = "Nomination has no DOB; name already in dataset"

ACTION_ADD = "Add row"
ACTION_UPGRADE = "Upgrade program"
ACTION_UP_TO_DATE = "Already up to date"
ACTION_REVIEW = "Needs review"
ACTION_CSS_ONLY = "Mark CSS (Multisport)"

REVIEW_COLUMNS = [
    "Review ID", "Year", "Name", "DOB", "Sport", "Program", "Issue",
    "Possible match", "Detail", "Decision",
]
PLAN_COLUMNS = [
    "Year", "First Name", "Last Name", "DOB", "Sport", "Program", "Gender", "CSS",
    "Profile ID", "Status", "Action", "Dataset key", "Detail", "Review ID",
]
CHANGE_COLUMNS = ["Change", "Full_Name", "Name", "Sport", "Year", "Field", "Old", "New", "Note"]
EXCLUDED_COLUMNS = ["Name", "Sport", "Fiscal year", "Reason"]

# Review decisions are saved (next to the dataset on GitHub, and in the ZIP) so a nomination is only
# reviewed once. A saved decision is found by nomination year + name + DOB + sport group.
DECISIONS_FILENAME = "conversion_review_decisions.csv"
DECISION_COLUMNS = [
    "Year", "First Name", "Last Name", "DOB", "Sport", "Issue", "Decision", "Possible match", "Dataset key",
    "Decided",
]
STATUS_SAVED_SAME = "Decided earlier: same person"
STATUS_SAVED_DIFFERENT = "Decided earlier: different person"
STATUS_FILED_NO_DOB = "Matched (no DOB; already filed this year)"
# Confirmed matches whose nomination DOB is applied to all the athlete's rows (newest year wins).
DOB_OVERWRITE_STATUSES = {STATUS_MATCHED, STATUS_MATCHED_ID, STATUS_SAVED_SAME}


def norm(value: Any) -> str:
    """Dataset Full_Name key format: lowercase letters only, accents dropped."""
    text = unicodedata.normalize("NFKD", str(value or "")).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z]", "", text.lower())


def root_letters(value: Any) -> str:
    """Strip accents to the root letter (é -> e, Ï -> I); other characters such as ’ are kept."""
    decomposed = unicodedata.normalize("NFKD", str(value or ""))
    return unicodedata.normalize("NFC", "".join(ch for ch in decomposed if not unicodedata.combining(ch)))


def legacy_norm(value: Any) -> str:
    """Older key format that dropped accented letters entirely ("Josué" -> "josu").

    Re-keyed in the dataset on 9 Nov 2026; still matched so an older copy of the CSV can't create duplicates.
    """
    return re.sub(r"[^a-z]", "", str(value or "").lower())


def sport_group(sport: str) -> str:
    return SPORT_GROUP.get(sport, sport)


def base_key(key: str) -> str:
    return re.sub(r"_[0-9a-z]+$", "", key) if "_" in key else key


def parse_dob(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    parsed = pd.to_datetime(text, errors="coerce", format="%Y-%m-%d")
    if pd.isna(parsed):
        parsed = pd.to_datetime(text, errors="coerce")
    return "" if pd.isna(parsed) else parsed.strftime("%Y-%m-%d")


def same_dob(first: str, second: str) -> bool:
    """DOBs differing in only one of year/month/day, or with day and month swapped (a typo)."""
    if not first or not second:
        return False
    a, b = pd.Timestamp(first), pd.Timestamp(second)
    diffs = (a.year != b.year) + (a.month != b.month) + (a.day != b.day)
    return diffs <= 1 or (a.year == b.year and a.month == b.day and a.day == b.month)


def fiscal_end_year(value: Any) -> str:
    match = re.fullmatch(r"\s*(\d{4})\s*[-/]\s*(\d{2}|\d{4})\s*", str(value or ""))
    if not match:
        return ""
    end = match.group(2)
    return end if len(end) == 4 else match.group(1)[:2] + end


def _column(frame: pd.DataFrame, *candidates: str) -> pd.Series:
    name = next((candidate for candidate in candidates if candidate in frame), "")
    if not name:
        return pd.Series("", index=frame.index, dtype="object")
    return frame[name].fillna("").astype(str).str.strip()


# --------------------------------------------------------------------------- input


def parse_conversion_csv(raw: bytes) -> pd.DataFrame:
    """Read the conversion dataset (all values as text) and check its columns."""
    try:
        frame = pd.read_csv(io.BytesIO(raw), dtype=str, keep_default_na=False)
    except UnicodeDecodeError:
        frame = pd.read_csv(io.BytesIO(raw), dtype=str, keep_default_na=False, encoding="latin1")
    missing = [name for name in DATASET_COLUMNS if name not in frame.columns]
    if missing:
        raise ValueError(f"This doesn't look like the conversion dataset; missing columns: {', '.join(missing)}.")
    if PROFILE_ID_COLUMN not in frame.columns:
        frame[PROFILE_ID_COLUMN] = ""
    return frame


def read_conversion_csv(contents: str, filename: str = "") -> pd.DataFrame:
    """Decode a dcc.Upload CSV and check it is the conversion dataset."""
    if not contents or "," not in contents:
        raise ValueError("No file was received.")
    if filename and not filename.lower().endswith(".csv"):
        raise ValueError("Upload the conversion dataset as a .csv file.")
    return parse_conversion_csv(base64.b64decode(contents.split(",", 1)[1]))


def empty_decisions() -> pd.DataFrame:
    return pd.DataFrame(columns=DECISION_COLUMNS)


def is_decisions_csv(raw: bytes) -> bool:
    header = raw[:2000].decode("utf-8", "ignore").splitlines()[:1]
    return bool(header) and {"Decision", "Dataset key"} <= {name.strip() for name in header[0].split(",")}


def parse_decisions_csv(raw: bytes) -> pd.DataFrame:
    """Read a saved review-decisions file (conversion_review_decisions.csv)."""
    if not raw.strip():
        return empty_decisions()
    frame = pd.read_csv(io.BytesIO(raw), dtype=str, keep_default_na=False)
    missing = [name for name in ("Year", "First Name", "Last Name", "DOB", "Sport", "Decision", "Dataset key")
               if name not in frame.columns]
    if missing:
        raise ValueError(f"This doesn't look like a review-decisions file; missing columns: {', '.join(missing)}.")
    for name in DECISION_COLUMNS:
        if name not in frame:
            frame[name] = ""
    return frame[DECISION_COLUMNS]


def decision_identity(year: Any, first: Any, last: Any, dob: Any, sport: Any) -> tuple[str, str, str, str]:
    return str(year).strip(), norm(first) + norm(last), parse_dob(dob), sport_group(str(sport).strip())


def _decision_index(saved: pd.DataFrame | None) -> dict[tuple[str, str, str, str], dict[str, str]]:
    if saved is None or saved.empty:
        return {}
    index = {}
    for record in saved.to_dict("records"):
        if record.get("Decision") in (SAME, DIFFERENT):
            index[decision_identity(record["Year"], record["First Name"], record["Last Name"], record["DOB"],
                                    record["Sport"])] = record
    return index


def merge_decisions(saved: pd.DataFrame | None, new: pd.DataFrame | None) -> pd.DataFrame:
    """Saved decisions plus this run's; a new decision replaces a saved one for the same nomination."""
    frames = [frame[DECISION_COLUMNS] for frame in (saved, new) if frame is not None and not frame.empty]
    if not frames:
        return empty_decisions()
    merged = pd.concat(frames, ignore_index=True).fillna("")
    identity = [
        decision_identity(*values)
        for values in zip(merged["Year"], merged["First Name"], merged["Last Name"], merged["DOB"], merged["Sport"])
    ]
    merged = merged.assign(_id=identity).drop_duplicates("_id", keep="last").drop(columns="_id")
    merged["_y"] = pd.to_numeric(merged["Year"], errors="coerce")
    return merged.sort_values(["_y", "Last Name", "First Name"], kind="stable").drop(columns="_y").reset_index(drop=True)


@dataclass
class GitHubSource:
    repo: str = "LukeSpagnuolo/Conversion"
    branch: str = "main"
    path: str = "Conversion_Data_2026_final.csv"
    token: str = ""

    @property
    def label(self) -> str:
        return f"{self.repo} · {self.branch} · {self.path}"

    @property
    def decisions_path(self) -> str:
        """Saved review decisions live next to the dataset."""
        folder = self.path.rsplit("/", 1)[0] + "/" if "/" in self.path else ""
        return folder + DECISIONS_FILENAME


def fetch_conversion_from_github(
    source: GitHubSource, session: Any = None, timeout: tuple[int, int] = (10, 60)
) -> tuple[pd.DataFrame, dict[str, str]]:
    """Download the dataset at the latest commit that touched it on the branch.

    The CSV is read at that exact commit, so the commit shown always matches the data loaded.
    A token is only needed for a private repo (or to avoid GitHub's anonymous rate limit).
    """
    session = session or requests
    headers = {"Accept": "application/vnd.github+json"}
    if source.token:
        headers["Authorization"] = f"Bearer {source.token}"
    path = quote(source.path)
    try:
        response = session.get(
            f"https://api.github.com/repos/{source.repo}/commits",
            params={"path": source.path, "sha": source.branch, "per_page": 1},
            headers=headers,
            timeout=timeout,
        )
    except requests.RequestException as exc:
        raise ValueError(f"Couldn't reach GitHub: {exc}") from exc
    if response.status_code in (401, 403):
        raise ValueError(
            "GitHub refused the request (rate limit or a private repo). "
            "Set GITHUB_TOKEN in .env, or upload the file instead."
        )
    if response.status_code == 404:
        raise ValueError(f"GitHub couldn't find {source.label}. Check CONVERSION_GITHUB_* in .env.")
    if response.status_code != 200:
        raise ValueError(f"GitHub returned HTTP {response.status_code}.")
    commits = response.json()
    if not commits:
        raise ValueError(f"No commits touch {source.path} on {source.repo} {source.branch}.")
    commit = commits[0]
    sha = commit["sha"]
    try:
        raw = session.get(
            f"https://raw.githubusercontent.com/{source.repo}/{sha}/{path}",
            headers={"Authorization": f"Bearer {source.token}"} if source.token else {},
            timeout=timeout,
        )
    except requests.RequestException as exc:
        raise ValueError(f"Couldn't download the file from GitHub: {exc}") from exc
    if raw.status_code != 200:
        raise ValueError(f"GitHub returned HTTP {raw.status_code} for {source.path} at {sha[:7]}.")
    info = {
        "repo": source.repo,
        "branch": source.branch,
        "path": source.path,
        "sha": sha,
        "date": (commit.get("commit", {}).get("committer") or commit.get("commit", {}).get("author") or {}).get("date", ""),
        "message": (commit.get("commit", {}).get("message") or "").splitlines()[0] if commit.get("commit") else "",
        "url": commit.get("html_url", ""),
    }
    return parse_conversion_csv(raw.content), info


def fetch_decisions_from_github(
    source: GitHubSource, sha: str, session: Any = None, timeout: tuple[int, int] = (10, 60)
) -> pd.DataFrame:
    """Saved review decisions at the commit the dataset was loaded from (none yet: an empty table)."""
    session = session or requests
    try:
        raw = session.get(
            f"https://raw.githubusercontent.com/{source.repo}/{sha}/{quote(source.decisions_path)}",
            headers={"Authorization": f"Bearer {source.token}"} if source.token else {},
            timeout=timeout,
        )
    except requests.RequestException as exc:
        raise ValueError(f"Couldn't download the saved review decisions from GitHub: {exc}") from exc
    if raw.status_code == 404:
        return empty_decisions()
    if raw.status_code != 200:
        raise ValueError(f"GitHub returned HTTP {raw.status_code} for {source.decisions_path} at {sha[:7]}.")
    return parse_decisions_csv(raw.content)


def _github_error(response: Any, action: str) -> ValueError:
    if response.status_code == 401:
        return ValueError("GitHub rejected GITHUB_TOKEN (expired or mistyped). Create a new token and update .env.")
    if response.status_code in (403, 404):
        return ValueError(
            f"GitHub refused to {action}. GITHUB_TOKEN needs Contents: Read and write access to this repo."
        )
    if response.status_code == 409:
        return ValueError("GitHub reported a conflict; reload the dataset from GitHub and try again.")
    return ValueError(f"GitHub returned HTTP {response.status_code} while trying to {action}.")


def latest_file_commit(source: GitHubSource, session: Any = None, timeout: tuple[int, int] = (10, 60)) -> str:
    session = session or requests
    headers = {"Accept": "application/vnd.github+json"}
    if source.token:
        headers["Authorization"] = f"Bearer {source.token}"
    response = session.get(
        f"https://api.github.com/repos/{source.repo}/commits",
        params={"path": source.path, "sha": source.branch, "per_page": 1},
        headers=headers,
        timeout=timeout,
    )
    if response.status_code != 200:
        raise _github_error(response, "read the latest commit")
    commits = response.json()
    return commits[0]["sha"] if commits else ""


def push_conversion_to_github(
    source: GitHubSource,
    content: bytes,
    loaded_sha: str,
    message: str,
    session: Any = None,
    timeout: tuple[int, int] = (10, 120),
    extra_files: dict[str, bytes] | None = None,
) -> dict[str, str]:
    """Commit ``content`` as source.path on source.branch (plus ``extra_files``, path -> bytes, in the same commit).

    Refuses if the file changed on GitHub since ``loaded_sha`` (the commit the dataset was loaded from),
    and only fast-forwards the branch, so nobody else's work can be overwritten.
    """
    if not source.token:
        raise ValueError("Set GITHUB_TOKEN in .env to push to GitHub.")
    session = session or requests
    api = f"https://api.github.com/repos/{source.repo}"
    headers = {"Accept": "application/vnd.github+json", "Authorization": f"Bearer {source.token}"}

    def call(method: str, url: str, action: str, ok: tuple[int, ...] = (200, 201), **kwargs: Any) -> dict:
        try:
            response = getattr(session, method)(url, headers=headers, timeout=timeout, **kwargs)
        except requests.RequestException as exc:
            raise ValueError(f"Couldn't reach GitHub to {action}: {exc}") from exc
        if response.status_code == 422 and method == "patch":
            raise ValueError("Someone pushed to GitHub while you were working. Reload the dataset from GitHub and try again.")
        if response.status_code not in ok:
            raise _github_error(response, action)
        return response.json()

    latest = latest_file_commit(source, session)
    if latest != loaded_sha:
        raise ValueError(
            f"{source.path} changed on GitHub since you loaded it ({loaded_sha[:7]} -> {latest[:7]}). "
            "Reload it from GitHub and run the update again."
        )
    head = call("get", f"{api}/git/ref/heads/{quote(source.branch)}", "read the branch")["object"]["sha"]
    base_tree = call("get", f"{api}/git/commits/{head}", "read the latest commit")["tree"]["sha"]
    entries = []
    for path, data in {source.path: content, **(extra_files or {})}.items():
        blob = call("post", f"{api}/git/blobs", "upload the file",
                    json={"content": base64.b64encode(data).decode("ascii"), "encoding": "base64"})["sha"]
        entries.append({"path": path, "mode": "100644", "type": "blob", "sha": blob})
    tree = call("post", f"{api}/git/trees", "build the commit", json={"base_tree": base_tree, "tree": entries})["sha"]
    commit = call("post", f"{api}/git/commits", "create the commit",
                  json={"message": message, "tree": tree, "parents": [head]})
    call("patch", f"{api}/git/refs/heads/{quote(source.branch)}", "update the branch",
         json={"sha": commit["sha"], "force": False})
    return {"sha": commit["sha"], "url": commit.get("html_url") or f"https://github.com/{source.repo}/commit/{commit['sha']}"}


def commit_message(result: "UpdateResult", years: list[str] | None = None) -> str:
    counts = result.counts
    title = (
        f"Conversion update: +{counts['rows_added']:,} rows, "
        f"{counts['programs_upgraded']:,} program upgrades"
        + (f" ({', '.join(years)})" if years else "")
    )
    lines = [
        f"- Rows {counts['rows_before']:,} -> {counts['rows_after']:,}",
        f"- {counts['dob_changes']:,} athletes' DOB updated, {counts.get('dob_filled', 0):,} missing DOBs filled in",
        f"- {counts['multisport_removed']:,} Multisport rows folded into CSS",
        f"- {counts['profile_ids_stored']:,} profile IDs stored",
        f"- {counts['held']:,} nominations held for review",
        f"- Convert Year flags changed: {counts['convert_changes']:,}",
        "",
        "Made with the List Generator 7000 Conversion Update tab.",
    ]
    return title + "\n\n" + "\n".join(lines)


def dataset_summary(frame: pd.DataFrame) -> dict[str, Any]:
    years = pd.to_numeric(frame["Year"], errors="coerce").dropna().astype(int)
    return {
        "rows": len(frame),
        "athletes": frame["Full_Name"].nunique(),
        "first_year": int(years.min()) if len(years) else None,
        "latest_year": int(years.max()) if len(years) else None,
        "latest_year_rows": int((years == years.max()).sum()) if len(years) else 0,
        "profile_ids": int((frame[PROFILE_ID_COLUMN] != "").sum()),
    }


def nomination_fiscal_years(frame: pd.DataFrame) -> list[str]:
    years = _column(frame, "nomination_fiscal_year", "current_nomination.fiscal_year", "Fiscal Year")
    return sorted(value for value in years.unique() if fiscal_end_year(value))


def row_kinds(frame: pd.DataFrame) -> set[str]:
    return set(_column(frame, "row_kind").str.lower()) - {""}


def prepare_nominations(
    frame: pd.DataFrame, fiscal_years: list[str] | None = None
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Turn fetched registration rows into one candidate per athlete/sport group/year.

    Works on profile rows (claimed nominations) and nomination rows (unclaimed).
    Returns (candidates, excluded).
    """
    work = pd.DataFrame(index=frame.index)
    work["first"] = _column(frame, "first_name", "person.first_name").map(root_letters)
    work["last"] = _column(frame, "last_name", "person.last_name").map(root_letters)
    work["role"] = _column(frame, "role", "role_slug").str.lower()
    work["fiscal"] = _column(frame, "nomination_fiscal_year", "current_nomination.fiscal_year", "Fiscal Year")
    work["Year"] = work["fiscal"].map(fiscal_end_year)
    sport = _column(frame, "nomination_sport_name", "current_nomination.sport.name")
    work["Sport"] = sport.where(sport != "", _column(frame, "sport", "sport.name")).replace(SPORT_RENAME)
    work["carding"] = _column(frame, "athlete_carding", "carding_level_mapped", "card_level")
    work["Program"] = work["carding"].str.lower().map(CARDING_TO_PROGRAM)
    work["DOB"] = _column(frame, "dob", "person.dob").map(parse_dob)
    work["Gender"] = _column(frame, "sex_of_competition").str.lower().map(SEX).fillna("")
    work["approved"] = _column(frame, "nomination_approved", "current_nomination.approved").str.lower()
    work["organization"] = _column(frame, "nomination_organization", "current_nomination.organization.name")
    work["Profile ID"] = _column(frame, "profile_id")
    work["Profile ID"] = work["Profile ID"].where(work["Profile ID"] != "", _column(frame, "row_id").map(
        lambda value: value[2:] if value.startswith("p:") else ""
    ))

    reasons = pd.Series("", index=work.index, dtype="object")

    def exclude(mask: pd.Series, reason: str) -> None:
        reasons.loc[mask & (reasons == "")] = reason

    exclude(work["fiscal"] == "", "No nomination on this profile")
    if fiscal_years:
        exclude(~work["fiscal"].isin(fiscal_years), "Fiscal year not selected")
    exclude(work["role"] != "athlete", "Not an athlete")
    exclude(work["Sport"].str.lower().str.contains("(test", regex=False), "Test sport")
    exclude(work["approved"].isin(FALSE_VALUES), "Nomination not approved")
    exclude(work["Year"] == "", "Fiscal year not recognised")
    exclude(work["Program"].isna(), "Carding level is not a conversion program")
    exclude((work["first"] == "") | (work["last"] == "") | (work["Sport"] == ""), "Missing name or sport")

    shown = reasons[~reasons.isin({"", "No nomination on this profile", "Fiscal year not selected", "Not an athlete"})]
    excluded = pd.DataFrame(
        {
            "Name": (work.loc[shown.index, "first"] + " " + work.loc[shown.index, "last"]).str.strip(),
            "Sport": work.loc[shown.index, "Sport"],
            "Fiscal year": work.loc[shown.index, "fiscal"],
            "Reason": shown.where(
                shown != "Carding level is not a conversion program",
                shown + " (" + work.loc[shown.index, "carding"].replace("", "blank") + ")",
            ),
        },
        columns=EXCLUDED_COLUMNS,
    )

    work = work[reasons == ""].copy()
    work["own"] = work["first"].map(norm) + work["last"].map(norm)
    work["legacy"] = work["first"].map(legacy_norm) + work["last"].map(legacy_norm)
    work["sg"] = work["Sport"].map(sport_group)
    work["level"] = work["Program"].map(LEVEL)
    css_years = set(
        zip(work.loc[work["organization"].str.lower() == CSS_ORGANIZATION, "own"],
            work.loc[work["organization"].str.lower() == CSS_ORGANIZATION, "Year"])
    )
    work["CSS"] = [
        "YES" if (own, year) in css_years or sport.lower() == MULTISPORT else "NO"
        for own, year, sport in zip(work["own"], work["Year"], work["Sport"])
    ]
    # One candidate per athlete/sport group/year; highest program first, then a row with a profile ID.
    work["_has_id"] = work["Profile ID"] != ""
    work = work.sort_values(["level", "_has_id"], ascending=False)
    work = work.drop_duplicates(["own", "DOB", "sg", "Year"]).drop(columns="_has_id")
    work = work.sort_values(["Year", "last", "first"]).reset_index(drop=True)
    work = work.rename(columns={"first": "First Name", "last": "Last Name"})
    return work, excluded.reset_index(drop=True)


# --------------------------------------------------------------------------- matching


class _People:
    """Index of athletes in the dataset (plus athletes added during this update).

    One entry per Full_Name + DOB + sport group, indexed by base name key, (base, sport group),
    and (DOB, sport group) so matching stays fast on the full 45k-row dataset.
    """

    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []
        self.by_id: dict[str, str] = {}
        self.by_base: dict[str, list[dict[str, Any]]] = {}
        self.by_base_sg: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self.by_dob_sg: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self.track: dict[tuple[str, str], dict[str, Any]] = {}
        self.key_set: set[str] = set()

    @classmethod
    def from_dataset(cls, dataset: pd.DataFrame) -> "_People":
        people = cls()
        work = dataset.assign(
            _sg=dataset["Sport"].map(sport_group),
            _spelling=dataset["First Name"].map(norm) + dataset["Last Name"].map(norm),
        )
        for (key, dob, group), rows in work.groupby(["Full_Name", "Date of Birth", "_sg"], sort=False):
            people._insert(key, dob, group, rows["First Name"].iloc[0], rows["Last Name"].iloc[0],
                           sorted(rows["Year"].unique()), set(rows["_spelling"]))
        profile_ids = dataset[PROFILE_ID_COLUMN] if PROFILE_ID_COLUMN in dataset else [""] * len(dataset)
        for key, profile_id in zip(dataset["Full_Name"], profile_ids):
            if profile_id:
                people.by_id.setdefault(profile_id, key)
        return people

    def _insert(
        self, key: str, dob: str, group: str, first: str, last: str, years: list[str],
        spellings: set[str] | None = None,
    ) -> None:
        """Index under the key's base name and every name spelling recorded on the athlete's rows,
        so a spelling variant already filed under this athlete (a past "Same person") matches again."""
        row = {"key": key, "base": base_key(key), "dob": dob, "sg": group,
               "first": first, "last": last, "years": years}
        self.rows.append(row)
        self.key_set.add(key)
        for name in {row["base"], *(spellings or set())} - {""}:
            self.by_base.setdefault(name, []).append(row)
            self.by_base_sg.setdefault((name, group), []).append(row)
        if dob:
            self.by_dob_sg.setdefault((dob, group), []).append(row)
        self.track.setdefault((key, group), row)

    def add(self, key: str, dob: str, group: str, first: str, last: str, year: str, profile_id: str) -> None:
        existing = self.track.get((key, group))
        if existing is not None and existing["dob"] == dob:
            existing["years"] = sorted(set(existing["years"]) | {year})
        else:
            self._insert(key, dob, group, first, last, [year], {norm(first) + norm(last)})
        if profile_id:
            self.by_id.setdefault(profile_id, key)

    def add_year(self, key: str, group: str, year: str) -> None:
        row = self.track.get((key, group))
        if row is not None:
            row["years"] = sorted(set(row["years"]) | {year})

    def keys(self) -> set[str]:
        return self.key_set

    def named(self, names: list[str], group: str | None = None) -> list[dict[str, Any]]:
        found: dict[int, dict[str, Any]] = {}
        for name in dict.fromkeys(names):
            for row in self.by_base.get(name, []) if group is None else self.by_base_sg.get((name, group), []):
                found.setdefault(id(row), row)
        return list(found.values())


def _describe(row: dict[str, Any]) -> str:
    years = row["years"]
    span = f"{years[0]}-{years[-1]}" if years else ""
    return f'{row["first"]} {row["last"]} ({row["key"]}), DOB {row["dob"] or "blank"}, {row["sg"]} {span}'.strip()


def new_athlete_key(
    own: str, dob: str, people: _People, names: list[str] | None = None
) -> tuple[str, str]:
    """Key for an athlete not found in this sport: reuse a same-name+DOB athlete (multi-sport)."""
    if dob:
        named = people.named(names or [own])
        # Same name and DOB (or a one-digit / day-month-swap typo of it) in another sport: a multi-sport athlete.
        same = [row for row in named if row["dob"] == dob] or [row for row in named if same_dob(row["dob"], dob)]
        if same:
            return same[0]["key"], STATUS_NEW_SPORT
    keys = people.keys()
    if not any(name in keys for name in (names or [own])):
        return own, STATUS_NEW
    suffix = dob[:4] if dob else "new"
    candidate, counter = f"{own}_{suffix}", 2
    while candidate in keys:
        candidate, counter = f"{own}_{suffix}{chr(ord('a') + counter - 2)}", counter + 1
    return candidate, STATUS_NEW


def match_nominations(
    candidates: pd.DataFrame, dataset: pd.DataFrame, saved: pd.DataFrame | None = None
) -> pd.DataFrame:
    """Decide what each candidate nomination would do to the dataset.

    ``saved``: review decisions from earlier runs; a nomination decided before is not reviewed again.
    """
    people = _People.from_dataset(dataset)
    saved_index = _decision_index(saved)
    existing = {
        (key, sport_group(sport), year): program
        for key, sport, year, program in zip(dataset["Full_Name"], dataset["Sport"], dataset["Year"], dataset["Program"])
    }
    existing_level: dict[tuple[str, str, str], int] = {}
    for track, program in existing.items():
        existing_level[track] = max(existing_level.get(track, 0), LEVEL.get(program, 0))
    plan = []
    review_counter = 0
    for _, nom in candidates.iterrows():
        own, dob, group, year = nom["own"], nom["DOB"], nom["sg"], nom["Year"]
        names = [own, nom.get("legacy", own) or own]
        status, key, detail, review = "", "", "", ""
        id_key = people.by_id.get(nom["Profile ID"]) if nom["Profile ID"] else None
        same_name_sport = sorted(people.named(names, group), key=lambda row: row["base"] not in names)
        exact = [row for row in same_name_sport if dob and row["dob"] == dob]
        older = [row for row in same_name_sport if year in row["years"] and year < max(row["years"])]
        if id_key:
            status, key = STATUS_MATCHED_ID, id_key
        elif exact:
            status, key = STATUS_MATCHED, exact[0]["key"]
        elif not dob and people.named(names):
            filed = [row for row in same_name_sport if year in row["years"]]
            if len(filed) == 1:
                # This nomination is already in the dataset under the only same-name athlete with this year.
                status, key = STATUS_FILED_NO_DOB, filed[0]["key"]
                detail = f'dataset DOB {filed[0]["dob"] or "blank"}'
            else:
                status, review = ACTION_REVIEW, REVIEW_NO_NOMINATION_DOB
                match = (same_name_sport or people.named(names))[0]
                key, detail = match["key"], _describe(match)
        elif older and not exact:
            # An older-year nomination already filed under this athlete; their newer rows set the DOB.
            status, key = STATUS_MATCHED_OLDER, older[0]["key"]
            detail = f'older nomination; dataset DOB {older[0]["dob"] or "blank"} kept (nomination {dob or "blank"})'
        elif same_name_sport:
            typo = [row for row in same_name_sport if same_dob(row["dob"], dob)]
            blank = [row for row in same_name_sport if not row["dob"]]
            if typo:
                status, key, detail = STATUS_MATCHED_TYPO, typo[0]["key"], f'dataset DOB {typo[0]["dob"]}'
            elif blank:
                status, review, key, detail = ACTION_REVIEW, REVIEW_NO_DATASET_DOB, blank[0]["key"], _describe(blank[0])
            else:
                status, review, key = ACTION_REVIEW, REVIEW_DOB_DIFFERS, same_name_sport[0]["key"]
                detail = "; ".join(_describe(row) for row in same_name_sport)
        if not status and dob:
            similar = sorted(
                (
                    (SequenceMatcher(None, row["base"], own).ratio(), row)
                    for row in people.by_dob_sg.get((dob, group), [])
                ),
                key=lambda item: item[0],
                reverse=True,
            )
            if similar and similar[0][0] >= 0.6:
                status, review, key, detail = ACTION_REVIEW, REVIEW_NAME_SPELLING, similar[0][1]["key"], _describe(similar[0][1])
        if not status:
            key, status = new_athlete_key(own, dob, people, names)

        if status == ACTION_REVIEW:
            earlier = saved_index.get(decision_identity(year, nom["First Name"], nom["Last Name"], dob, nom["Sport"]))
            if earlier is not None:
                target = earlier["Dataset key"]
                if earlier["Decision"] == SAME:
                    if target not in people.keys():
                        target = earlier["Possible match"] if earlier["Possible match"] in people.keys() else ""
                    if target:
                        status, key = STATUS_SAVED_SAME, target
                else:
                    # Reuse the key given before (the athlete may already be in the dataset under it).
                    status = STATUS_SAVED_DIFFERENT
                    key = target or _distinct_key(own, dob, people.keys())
                if status != ACTION_REVIEW:
                    detail = f'{review} ({earlier.get("Decided") or "saved decision"})'

        if status == ACTION_REVIEW:
            review_counter += 1
            review_id, action = f"R{review_counter}", ACTION_REVIEW
            status = review
        else:
            review_id = ""
            track = (key, group, year)
            if track in existing_level:
                action = ACTION_UPGRADE if nom["level"] > existing_level[track] else ACTION_UP_TO_DATE
                if action == ACTION_UPGRADE:
                    detail = (detail + "; " if detail else "") + f"dataset has {existing[track]}"
            else:
                action = ACTION_ADD
                if status in (STATUS_NEW, STATUS_NEW_SPORT) or key not in people.keys():
                    people.add(key, dob, group, nom["First Name"], nom["Last Name"], year, nom["Profile ID"])
                else:
                    people.add_year(key, group, year)
            existing_level[track] = max(existing_level.get(track, 0), nom["level"])
            existing.setdefault(track, nom["Program"])
        plan.append(
            {
                "Year": year, "First Name": nom["First Name"], "Last Name": nom["Last Name"], "DOB": dob,
                "Sport": nom["Sport"], "Program": nom["Program"], "Gender": nom["Gender"], "CSS": nom["CSS"],
                "Profile ID": nom["Profile ID"], "Status": status, "Action": action, "Dataset key": key,
                "Detail": detail, "Review ID": review_id,
            }
        )
    result = pd.DataFrame(plan, columns=PLAN_COLUMNS)
    # An athlete can't be Multisport and another sport in the same year: the CSS nomination only sets CSS.
    multisport = dataset["Sport"].str.strip().str.lower() == MULTISPORT
    other_sport = set(zip(dataset.loc[~multisport, "Full_Name"], dataset.loc[~multisport, "Year"]))
    planned = result[(result["Action"] == ACTION_ADD) & (result["Sport"].str.lower() != MULTISPORT)]
    other_sport |= set(zip(planned["Dataset key"], planned["Year"]))
    css_only = (
        (result["Action"] == ACTION_ADD)
        & (result["Sport"].str.lower() == MULTISPORT)
        & pd.Series([item in other_sport for item in zip(result["Dataset key"], result["Year"])], index=result.index)
    )
    result.loc[css_only, "Action"] = ACTION_CSS_ONLY
    result.loc[css_only, "Detail"] = "athlete has another sport this year; that sport gets CSS = YES"
    return result


def review_rows(plan: pd.DataFrame) -> list[dict[str, str]]:
    held = plan[plan["Action"] == ACTION_REVIEW]
    return [
        {
            "Review ID": row["Review ID"], "Year": row["Year"], "Name": f'{row["First Name"]} {row["Last Name"]}',
            "DOB": row["DOB"] or "blank", "Sport": row["Sport"], "Program": row["Program"], "Issue": row["Status"],
            "Possible match": row["Dataset key"], "Detail": row["Detail"], "Decision": HOLD,
        }
        for _, row in held.iterrows()
    ]


def plan_counts(plan: pd.DataFrame) -> dict[str, int]:
    added = plan[plan["Action"] == ACTION_ADD]
    return {
        "new_athletes": int(added["Status"].eq(STATUS_NEW).sum()),
        "returning": int((~added["Status"].eq(STATUS_NEW)).sum()),
        "upgrades": int(plan["Action"].eq(ACTION_UPGRADE).sum()),
        "css_only": int(plan["Action"].eq(ACTION_CSS_ONLY).sum()),
        "up_to_date": int(plan["Action"].eq(ACTION_UP_TO_DATE).sum()),
        "review": int(plan["Action"].eq(ACTION_REVIEW).sum()),
    }


# --------------------------------------------------------------------------- applying


def recompute_targets(dataset: pd.DataFrame) -> pd.DataFrame:
    """Years Targeted and Convert Year, per Full_Name + sport group (Multisport is a wildcard predecessor).

    Same rules as the Conversion repo's scripts/clean_dataset.py step 9.
    """
    work = dataset.copy()
    years = pd.to_numeric(work["Year"], errors="coerce")
    groups = work["Sport"].map(sport_group)
    levels = work["Program"].map(LEVEL).fillna(0)
    work["Years Targeted"] = (
        pd.DataFrame({"k": work["Full_Name"], "g": groups, "y": years})
        .groupby(["k", "g"])["y"].transform("nunique").fillna(0).astype(int).astype(str)
    )
    best: dict[str, dict[str, dict[float, float]]] = {}
    for key, group, year, level in zip(work["Full_Name"], groups, years, levels):
        if pd.isna(year):
            continue
        by_year = best.setdefault(key, {}).setdefault(group, {})
        by_year[year] = max(by_year.get(year, 0), level)
    converted = set()
    for key, sports in best.items():
        multisport = [sport for sport in sports if sport.strip().lower() == MULTISPORT]
        for sport, own in sports.items():
            wildcard: dict[float, float] = {}
            if sport not in multisport:
                for ms in multisport:
                    for year, value in sports[ms].items():
                        wildcard[year] = max(wildcard.get(year, 0), value)
            ordered = sorted(set(own) | set(wildcard))
            for before, after in zip(ordered, ordered[1:]):
                if after not in own:
                    continue
                previous = [value for value in (own.get(before), wildcard.get(before)) if value is not None]
                if previous and own[after] > max(previous):
                    converted.add((key, sport, after))
    work["Convert Year"] = ["Y" if item in converted else "N" for item in zip(work["Full_Name"], groups, years)]
    return work


@dataclass
class UpdateResult:
    dataset: pd.DataFrame
    changes: pd.DataFrame
    held: pd.DataFrame
    counts: dict[str, int]
    decisions: pd.DataFrame = field(default_factory=lambda: empty_decisions())  # made in this run


def _distinct_key(own: str, dob: str, keys: set[str]) -> str:
    """A Full_Name key for someone confirmed to be a different person from existing athletes."""
    if own not in keys:
        return own
    suffix = dob[:4] if dob else "new"
    candidate, counter = f"{own}_{suffix}", 0
    while candidate in keys:
        counter += 1
        candidate = f"{own}_{suffix}{chr(ord('a') + counter)}"
    return candidate


def plausible_dob(dob: str, year: str) -> bool:
    try:
        age = int(year) - int(dob[:4])
    except (TypeError, ValueError):
        return False
    return MIN_AGE <= age <= MAX_AGE


def apply_update(
    dataset: pd.DataFrame,
    plan: pd.DataFrame,
    decisions: dict[str, str] | None = None,
    apply_typo_dobs: bool = True,
) -> UpdateResult:
    """Apply the plan. ``decisions`` maps Review ID -> Hold / Same person / Different person.

    DOBs (the nomination DOB has priority): a confirmed athlete (profile ID match, same person by review,
    or a DOB-typo match from their newest year when ``apply_typo_dobs``) gets the nomination DOB on all their
    rows; any other matched athlete only has blank DOBs filled in. With several nominations, the newest
    year's DOB is used. A DOB giving an implausible age is never applied. Finally, an athlete's blank DOBs
    are filled from their other rows when those agree on one DOB.
    """
    decisions = decisions or {}
    ds = dataset.copy()
    if PROFILE_ID_COLUMN not in ds:
        ds[PROFILE_ID_COLUMN] = ""
    before_convert = dict(zip(zip(ds["Full_Name"], ds["Sport"], ds["Year"]), ds["Convert Year"]))
    changes: list[dict[str, str]] = []

    def log(change, key, name, sport, year, field_, old, new, note=""):
        changes.append({"Change": change, "Full_Name": key, "Name": name, "Sport": sport, "Year": year,
                        "Field": field_, "Old": old, "New": new, "Note": note})

    keys_in_use = set(ds["Full_Name"])
    groups = ds["Sport"].map(sport_group)
    tracks = set(zip(ds["Full_Name"], groups, ds["Year"]))
    max_year = pd.to_numeric(ds["Year"], errors="coerce").groupby(ds["Full_Name"]).max().to_dict()
    known_dob = ds[ds["Date of Birth"] != ""].groupby("Full_Name")["Date of Birth"].first().to_dict()
    held, resolved, new_rows, decided = [], [], [], []
    # key -> (rank, dob, name, note). The newest nomination wins; in the same year a confirmed one, and among
    # those the athlete's Registration profile (profile ID match), so the result doesn't depend on row order.
    dob_votes: dict[str, tuple[tuple[int, bool, bool, str], str, str, str]] = {}

    def vote(key: str, item: pd.Series, overwrite: bool, note: str) -> None:
        if not item["DOB"]:
            return
        name = f'{item["First Name"]} {item["Last Name"]}'
        if not plausible_dob(item["DOB"], item["Year"]):
            if overwrite and known_dob.get(key) and known_dob[key] != item["DOB"]:
                log("DOB kept", key, name, item["Sport"], item["Year"], "Date of Birth", known_dob[key],
                    known_dob[key], f'nomination DOB {item["DOB"]} gives an implausible age; check Registration')
            return
        rank = (int(item["Year"]), overwrite, item["Status"] == STATUS_MATCHED_ID, item["DOB"])
        if key not in dob_votes or rank > dob_votes[key][0]:
            dob_votes[key] = (rank, item["DOB"], name, note)

    # Pass 1: final key and action per nomination; program upgrades and DOB fixes on existing rows.
    for _, item in plan.iterrows():
        item = item.copy()
        action, key, status = item["Action"], item["Dataset key"], item["Status"]
        name = f'{item["First Name"]} {item["Last Name"]}'
        group = sport_group(item["Sport"])
        note = status
        if action == ACTION_REVIEW:
            decision = decisions.get(item["Review ID"], HOLD)
            possible = key
            if decision == SAME:
                note = f"{status}: reviewed as same person"
                vote(key, item, True, "Reviewed as same person; nomination DOB applied")
            elif decision == DIFFERENT:
                own = norm(item["First Name"]) + norm(item["Last Name"])
                key = _distinct_key(own, item["DOB"], keys_in_use)
                note = f"{status}: reviewed as different person"
            else:
                held.append(item)
                continue
            decided.append({
                "Year": item["Year"], "First Name": item["First Name"], "Last Name": item["Last Name"],
                "DOB": item["DOB"], "Sport": item["Sport"], "Issue": status, "Decision": decision,
                "Possible match": possible, "Dataset key": key, "Decided": date.today().isoformat(),
            })
            action = ACTION_UPGRADE if (key, group, item["Year"]) in tracks else ACTION_ADD
        elif status == STATUS_MATCHED_TYPO and int(item["Year"]) >= max_year.get(key, 0):
            if apply_typo_dobs:
                vote(key, item, True, "Newest nomination DOB applied (DOB typo match)")
            else:
                vote(key, item, False, "Nomination DOB filled in")
        elif status in DOB_OVERWRITE_STATUSES:
            vote(key, item, True, f"Nomination DOB applied ({status.lower()})")
        elif status not in (STATUS_NEW, STATUS_NEW_SPORT):
            vote(key, item, False, "Nomination DOB filled in")

        if action in (ACTION_UP_TO_DATE, ACTION_CSS_ONLY):
            pass
        elif action == ACTION_UPGRADE:
            rows = ds.index[(ds["Full_Name"] == key) & (groups == group) & (ds["Year"] == item["Year"])]
            for index in rows:
                old = ds.at[index, "Program"]
                if LEVEL.get(item["Program"], 0) > LEVEL.get(old, 0):
                    ds.at[index, "Program"] = item["Program"]
                    log("Program upgraded", key, name, ds.at[index, "Sport"], item["Year"], "Program", old,
                        item["Program"], "higher program level kept")
        elif action == ACTION_ADD:
            # A known athlete keeps the DOB on file (already updated above when the nomination DOB applies).
            dob = known_dob.get(key) or item["DOB"]
            new_rows.append({
                "Sport": item["Sport"], "First Name": item["First Name"], "Last Name": item["Last Name"],
                "Gender": item["Gender"], "Date of Birth": dob, "Year": item["Year"], "Program": item["Program"],
                "Full_Name": key, "CSS": item["CSS"], "Years Targeted": "", "Convert Year": "",
                PROFILE_ID_COLUMN: item["Profile ID"],
            })
            log("Row added", key, name, item["Sport"], item["Year"], "row", "", item["Program"], note)
            keys_in_use.add(key)
            tracks.add((key, group, item["Year"]))
            if dob:
                known_dob.setdefault(key, dob)
        item["Dataset key"] = key
        resolved.append(item)

    if new_rows:
        ds = pd.concat([ds, pd.DataFrame(new_rows)], ignore_index=True).fillna("")
    _merge_dobs(ds, dob_votes, log)

    # Pass 2: CSS flags and profile IDs (now that new rows exist).
    for item in resolved:
        _mark_css_and_id(ds, item["Dataset key"], item, f'{item["First Name"]} {item["Last Name"]}', log)
    touched = {item["Dataset key"] for item in resolved}

    ds = _resolve_multisport(ds, touched, log)
    ds = recompute_targets(ds)
    for key, sport, year, flag in zip(ds["Full_Name"], ds["Sport"], ds["Year"], ds["Convert Year"]):
        old = before_convert.get((key, sport, year))
        if old is not None and old != flag:
            log("Convert Year recalculated", key, "", sport, year, "Convert Year", old, flag,
                "" if key in touched else "athlete not in this update")
    ds["_y"] = pd.to_numeric(ds["Year"], errors="coerce")
    ds = ds.sort_values(["Full_Name", "Sport", "_y"], kind="stable").drop(columns="_y").reset_index(drop=True)
    columns = list(dataset.columns) + ([PROFILE_ID_COLUMN] if PROFILE_ID_COLUMN not in dataset.columns else [])
    ds = ds[columns]

    change_frame = pd.DataFrame(changes, columns=CHANGE_COLUMNS)
    changed = change_frame["Change"]
    counts = {
        "rows_added": int(changed.eq("Row added").sum()),
        "programs_upgraded": int(changed.eq("Program upgraded").sum()),
        "dob_changes": int(changed.eq("DOB updated").sum()),
        "dob_filled": int(changed.isin(["DOB filled in"]).sum()),
        "multisport_removed": int(changed.eq("Multisport row removed").sum()),
        "convert_changes": int(changed.eq("Convert Year recalculated").sum()),
        "convert_changes_untouched": int(
            (changed.eq("Convert Year recalculated") & change_frame["Note"].eq("athlete not in this update")).sum()
        ),
        "profile_ids_stored": int(changed.eq("Profile ID stored").sum()),
        "held": len(held),
        # Changes to the data itself (not the notes repeated every run: profile ID conflicts, DOBs kept).
        "data_changes": int((~changed.isin(["Profile ID conflict", "DOB kept"])).sum()),
        "rows_before": len(dataset),
        "rows_after": len(ds),
    }
    held_frame = pd.DataFrame(held, columns=PLAN_COLUMNS) if held else pd.DataFrame(columns=PLAN_COLUMNS)
    # A "Same person" decision can change the DOB on file, so the athlete's other nominations for that year
    # (matched by the old DOB) would come up for review next time: save them as the same person too.
    same_tracks = {(d["Dataset key"], sport_group(d["Sport"]), d["Year"]) for d in decided if d["Decision"] == SAME}
    for item in resolved:
        track = (item["Dataset key"], sport_group(item["Sport"]), item["Year"])
        if track in same_tracks and item["DOB"] and item["Status"] != ACTION_REVIEW and not item["Review ID"]:
            decided.append({
                "Year": item["Year"], "First Name": item["First Name"], "Last Name": item["Last Name"],
                "DOB": item["DOB"], "Sport": item["Sport"], "Issue": "Other nomination of a reviewed athlete",
                "Decision": SAME, "Possible match": item["Dataset key"], "Dataset key": item["Dataset key"],
                "Decided": date.today().isoformat() + " (implied)",
            })
    decided_frame = pd.DataFrame(decided, columns=DECISION_COLUMNS) if decided else empty_decisions()
    return UpdateResult(ds, change_frame, held_frame, counts, decided_frame)


def _merge_dobs(ds: pd.DataFrame, votes: dict[str, tuple[tuple[int, bool, bool, str], str, str, str]], log) -> None:
    """Nomination DOBs first (see apply_update), then blanks filled from the athlete's other rows."""
    by_key = ds.groupby("Full_Name", sort=False).groups
    for key, ((_year, overwrite, _by_id, _dob), dob, name, note) in votes.items():
        rows = by_key.get(key)
        if rows is None:
            continue
        current = ds.loc[rows, "Date of Birth"]
        if overwrite:
            change = current.index[current != dob]
        else:
            change = current.index[current == ""]
        if not len(change):
            continue
        old = sorted(set(ds.loc[change, "Date of Birth"]) - {""})
        ds.loc[change, "Date of Birth"] = dob
        if old:
            log("DOB updated", key, name, "all", "", "Date of Birth", "; ".join(old), dob, f"{note} ({len(change)} rows)")
        else:
            log("DOB filled in", key, name, "all", "", "Date of Birth", "blank", dob,
                f"{note if not overwrite else 'Nomination DOB filled in'} ({len(change)} rows)")
    blank = ds["Date of Birth"] == ""
    if blank.any():
        known = ds.loc[~blank & ds["Full_Name"].isin(set(ds.loc[blank, "Full_Name"]))]
        single = known.groupby("Full_Name")["Date of Birth"].agg(lambda values: values.iloc[0] if values.nunique() == 1 else "")
        for key, dob in single[single != ""].items():
            change = ds.index[(ds["Full_Name"] == key) & blank]
            ds.loc[change, "Date of Birth"] = dob
            log("DOB filled in", key, "", "all", "", "Date of Birth", "blank", dob,
                f"from the athlete's other rows ({len(change)} rows)")


def _mark_css_and_id(ds: pd.DataFrame, key: str, item: pd.Series, name: str, log) -> None:
    if item["CSS"] == "YES":
        rows = ds.index[(ds["Full_Name"] == key) & (ds["Year"] == item["Year"]) & (ds["CSS"] != "YES")]
        for index in rows:
            log("CSS set", key, name, ds.at[index, "Sport"], item["Year"], "CSS", ds.at[index, "CSS"], "YES",
                "Canadian Sport School nomination this year")
        ds.loc[rows, "CSS"] = "YES"
    profile_id = item["Profile ID"]
    if profile_id:
        mine = ds["Full_Name"] == key
        others = set(ds.loc[mine & (ds[PROFILE_ID_COLUMN] != ""), PROFILE_ID_COLUMN]) - {profile_id}
        if others:
            log("Profile ID conflict", key, name, item["Sport"], item["Year"], PROFILE_ID_COLUMN,
                "; ".join(sorted(others)), profile_id, "not overwritten; check for a duplicate profile")
            return
        blank = ds.index[mine & (ds[PROFILE_ID_COLUMN] == "")]
        if len(blank):
            ds.loc[blank, PROFILE_ID_COLUMN] = profile_id
            log("Profile ID stored", key, name, "all", "", PROFILE_ID_COLUMN, "", profile_id, f"{len(blank)} rows")


def _resolve_multisport(ds: pd.DataFrame, touched: set[str], log) -> pd.DataFrame:
    """No Multisport + another sport in the same year: drop Multisport, set CSS on the other sport."""
    multisport = ds["Sport"].str.strip().str.lower() == MULTISPORT
    in_scope = ds["Full_Name"].isin(touched)
    other = set(zip(ds.loc[~multisport, "Full_Name"], ds.loc[~multisport, "Year"]))
    drop = []
    for index in ds.index[multisport & in_scope]:
        key, year = ds.at[index, "Full_Name"], ds.at[index, "Year"]
        if (key, year) not in other:
            continue
        targets = ds.index[(ds["Full_Name"] == key) & (ds["Year"] == year) & ~multisport]
        best = max(LEVEL.get(ds.at[target, "Program"], 0) for target in targets)
        note = "athlete has another sport this year"
        if LEVEL.get(ds.at[index, "Program"], 0) > best:
            note += f'; Multisport level {ds.at[index, "Program"]} was higher (sport level kept)'
        for target in targets:
            if ds.at[target, "CSS"] != "YES":
                log("CSS set", key, "", ds.at[target, "Sport"], year, "CSS", ds.at[target, "CSS"], "YES",
                    "also nominated by CSS as Multisport this year")
                ds.at[target, "CSS"] = "YES"
        log("Multisport row removed", key, "", "Multisport", year, "row", ds.at[index, "Program"], "removed", note)
        drop.append(index)
    return ds.drop(index=drop)


# --------------------------------------------------------------------------- output


def build_update_zip(
    result: UpdateResult, excluded: pd.DataFrame, dataset_filename: str, decisions: pd.DataFrame | None = None
) -> bytes:
    """``decisions``: every saved review decision (earlier ones plus this run's), to load next time."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(dataset_filename or "Conversion_Data_2026_final.csv", result.dataset.to_csv(index=False))
        archive.writestr("conversion_update_changes.csv", result.changes.to_csv(index=False))
        archive.writestr("conversion_update_held_for_review.csv", result.held.to_csv(index=False))
        archive.writestr("conversion_update_excluded.csv", excluded.to_csv(index=False))
        all_decisions = merge_decisions(decisions, result.decisions) if decisions is not None else result.decisions
        archive.writestr(DECISIONS_FILENAME, all_decisions.to_csv(index=False))
    return buffer.getvalue()


CONVERSION_GUIDE = """
**What is added.** Approved athlete nominations from the fetched data — claimed nominations (profile rows)
and unclaimed ones (nomination rows). Fetch **Profiles + Nominations** so neither is missed.

**Excluded.** Coaches and staff, test sports, unapproved nominations, and carding levels that aren't a
conversion program (for example *GamePlan - Retired*). Excluded rows are listed in the download.

**Names.** Accents are stripped to the root letter (Zoé → Zoe) in names and Full_Name keys.

**Programs.** Level order, low to high: Prov Dev 3, Prov Dev 2, Prov Dev 1, Uncarded, SC Carded. Prov Dev 1–3 as recorded; NSO/PSO Affiliated → Uncarded; SR, SR1, SR2, SRI, D, DI, C1, C1I →
SC Carded. Fiscal year 2026-2027 → Year 2027. One row per athlete, sport and year — the higher program wins.

**Matching.** Same athlete only when **name + date of birth + sport** agree (Para and discipline sports count
as their parent sport). A stored Registration profile ID also identifies the athlete. A one-digit or
day/month-swap DOB typo still matches. A nomination without a DOB matches when exactly one same-name
athlete in that sport already has a row that year.

**DOBs.** The nomination DOB has priority. For a confirmed athlete (profile ID, *Same person*, or a DOB typo in
their newest year) the newest nomination's DOB is applied to all their rows; any other match only fills in
blank DOBs. Blank DOBs are also filled from the athlete's other rows when those agree. A DOB giving an
implausible age is never applied.

**Review.** Same name and sport with a different or missing DOB, a similar name with the same DOB and sport,
or a nomination without a DOB whose name is already in the dataset. *Hold* leaves the nomination out;
*Same person* files it under the possible match and applies the nomination DOB to that athlete;
*Different person* adds it as a new athlete. Same/Different decisions are saved to
`conversion_review_decisions.csv` (next to the dataset on GitHub when you push, and in the ZIP) and reused
automatically, so each nomination is only reviewed once.

**CSS.** A Canadian Sport School nomination sets CSS = YES for that athlete's year. An athlete can't be in
Multisport and another sport in the same year: the Multisport row is dropped and the other sport gets CSS = YES.

**Never deleted.** Existing rows are kept (apart from that Multisport rule). Years Targeted and Convert Year
are recalculated for the whole dataset, and every change is listed in `conversion_update_changes.csv`.
"""
