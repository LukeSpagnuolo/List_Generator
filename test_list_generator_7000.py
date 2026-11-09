import base64
import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
from openpyxl import load_workbook

import list_generator_7000 as app
from prebuilt_reports_7000 import (
    REPORT_OPTIONS,
    build_ee_athlete_list,
    build_ee_mail_merge,
    build_ee_sport_templates,
    build_report,
    default_ee_fiscal_mode,
    ee_shared_nccp_coaches,
    parse_cac_report,
    ee_review_rows,
    plan_ee_workbooks,
    previous_fiscal_year,
    duplicate_profiles,
    guardian_email_match,
)
from national_inventory_workflow import (
    OUTPUT_COLUMNS,
    build_service_report,
    name_sport_rows,
    registration_cohort,
)


class FakeResponse:
    def __init__(self, status_code, payload=None, headers=None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class FakeSession:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.urls = []

    def get(self, url, **_):
        self.urls.append(url)
        return next(self.responses)


class ListGenerator7000Tests(unittest.TestCase):
    def test_national_inventory_is_its_own_workflow(self):
        self.assertNotIn("national_inventory", {option["value"] for option in REPORT_OPTIONS})
        self.assertEqual(
            app.show_workflow("national_inventory"),
            ({"display": "none"}, {"display": "none"}, {"display": "block"}, {"display": "none"}, {"display": "none"}),
        )
        self.assertEqual(
            app.show_workflow("ee_templates"),
            ({"display": "none"}, {"display": "none"}, {"display": "none"}, {"display": "block"}, {"display": "none"}),
        )
        self.assertEqual(
            app.show_workflow("conversion_update"),
            ({"display": "none"},) * 4 + ({"display": "block"},),
        )


    def test_conversion_push_uses_review_decisions_and_reloads_as_up_to_date(self):
        nominations = pd.DataFrame([
            {"row_kind": "nomination", "role": "Athlete", "first_name": "Ana", "last_name": "Row",
             "sport": "Rowing", "nomination_fiscal_year": "2026-2027", "athlete_carding": "Prov Dev 1",
             "dob": "2008-01-01", "sex_of_competition": "Female", "nomination_approved": "True"},
        ])
        conversion = pd.DataFrame([{
            "Sport": "Rowing", "First Name": "Ana", "Last Name": "Row", "Gender": "F", "Date of Birth": "2008-01-01",
            "Year": "2026", "Program": "Prov Dev 2", "Full_Name": "anarow", "CSS": "NO", "Years Targeted": "1",
            "Convert Year": "N", "Profile_ID": "",
        }])
        with tempfile.TemporaryDirectory() as directory, patch.object(app, "CACHE_DIR", Path(directory)):
            fetch_id = app.save_dataset(nominations)
            store = {"dataset_id": app.save_dataset(conversion), "filename": "c.csv", "summary": {},
                     "source": "github", "sha": "loaded123"}
            plan_store, *_ = app.build_conversion_plan(fetch_id, store, ["2026-2027"])
            with patch.object(app, "CONVERSION_GITHUB", app.GitHubSource(token="t0k")):
                self.assertEqual(app.update_push_availability(plan_store, store)[0], False)
                self.assertEqual(app.update_push_availability(plan_store, {**store, "source": "upload"})[0], True)
            with patch.object(app, "push_conversion_to_github", return_value={"sha": "new333", "url": "u"}) as push:
                status, updated = app.push_conversion_update(1, plan_store, store, [], ["apply"], "", ["2026-2027"])
            source, content, loaded_sha, message = push.call_args[0]
            self.assertEqual(loaded_sha, "loaded123")
            self.assertTrue(message.startswith("Conversion update: +1 rows"))
            self.assertEqual(len(pd.read_csv(io.BytesIO(content))), 2)
            self.assertEqual(updated["sha"], "new333")
            replan = app.build_conversion_plan(fetch_id, updated, ["2026-2027"])
            self.assertEqual(replan[7], "1")  # Already up to date
            again = app.push_conversion_update(1, replan[0], updated, [], ["apply"], "", ["2026-2027"])
            self.assertIn("Nothing to push", again[0].children)

    def test_review_decisions_are_saved_and_reused(self):
        nominations = pd.DataFrame([
            {"row_kind": "nomination", "role": "Athlete", "first_name": "Lou", "last_name": "Blank",
             "sport": "Rowing", "nomination_fiscal_year": "2026-2027", "athlete_carding": "Prov Dev 1",
             "dob": "2007-07-07", "sex_of_competition": "Female", "nomination_approved": "True"},
        ])
        conversion = pd.DataFrame([{
            "Sport": "Rowing", "First Name": "Lou", "Last Name": "Blank", "Gender": "F", "Date of Birth": "",
            "Year": "2026", "Program": "Prov Dev 2", "Full_Name": "loublank", "CSS": "NO", "Years Targeted": "1",
            "Convert Year": "N", "Profile_ID": "",
        }])

        def encoded(frame):
            return "data:text/csv;base64," + base64.b64encode(frame.to_csv(index=False).encode()).decode()

        with tempfile.TemporaryDirectory() as directory, patch.object(app, "CACHE_DIR", Path(directory)):
            fetch_id = app.save_dataset(nominations)
            store = {"dataset_id": app.save_dataset(conversion), "filename": "c.csv", "summary": {},
                     "source": "github", "sha": "loaded123"}
            plan_store, _, review, *_ = app.build_conversion_plan(fetch_id, store, ["2026-2027"])
            self.assertEqual(len(review), 1)
            review[0]["Decision"] = "Same person"
            with patch.object(app, "push_conversion_to_github", return_value={"sha": "new333", "url": "u"}) as push:
                app.push_conversion_update(1, plan_store, store, review, ["apply"], "", ["2026-2027"])
            saved_bytes = push.call_args.kwargs["extra_files"]["conversion_review_decisions.csv"]
            saved = pd.read_csv(io.BytesIO(saved_bytes), dtype=str, keep_default_na=False)
            self.assertEqual(saved[["Decision", "Dataset key"]].values.tolist(), [["Same person", "loublank"]])

            # Uploading the old dataset with the saved decisions file: nothing left to review.
            with patch.object(app, "callback_context", SimpleNamespace(triggered_id="cu-upload")):
                loaded, notes, _ = app.load_conversion_dataset(
                    None, [encoded(conversion), "data:text/csv;base64," + base64.b64encode(saved_bytes).decode()],
                    ["c.csv", "conversion_review_decisions.csv"],
                )
            self.assertIn("1 saved review decisions", str(notes))
            replan = app.build_conversion_plan(fetch_id, loaded, ["2026-2027"])
            self.assertEqual(replan[2], [])
            self.assertIn("settled by saved review decisions", str(replan[1]))

    def test_push_is_disabled_without_a_token(self):
        with patch.object(app, "CONVERSION_GITHUB", app.GitHubSource(token="")):
            disabled, note = app.update_push_availability({"plan_id": "x"}, {"source": "github"})
        self.assertTrue(disabled)
        self.assertIn("GITHUB_TOKEN", note)

    def test_national_inventory_lou_selector_defaults_to_configured_sports(self):
        options, selected = app.update_national_inventory_lou_sports([
            {"Name": "Alex", "Sport": "Soccer"},
            {"Name": "Jamie", "Sport": "Swimming"},
            {"Name": "Morgan", "Sport": "Archery"},
        ])
        self.assertEqual([option["value"] for option in options], ["Soccer", "Swimming"])
        self.assertEqual(selected, ["Soccer", "Swimming"])

    def test_name_sport_rows_deduplicates_normalized_pairs(self):
        frame = pd.DataFrame(
            [
                {"Name": " Alex  Smith ", "Sport": "Soccer"},
                {"Name": "alex smith", "Sport": " soccer "},
                {"Name": "Alex Smith", "Sport": "Swimming"},
                {"Name": "", "Sport": "Rowing"},
            ]
        )
        result = name_sport_rows(frame)
        self.assertEqual(result.to_dict("records"), [
            {"Name": "Alex  Smith", "Sport": "Soccer"},
            {"Name": "Alex Smith", "Sport": "Swimming"},
        ])

    def test_registration_cohort_selects_approved_sc_athletes(self):
        frame = pd.DataFrame(
            [
                {"first_name": "Alex", "last_name": "Smith", "sport": "Soccer", "role": "athlete", "nomination_approved": "true", "nomination_fiscal_year": "2025-26", "level_category": "Canadian Elite"},
                {"first_name": "Dev", "last_name": "Athlete", "sport": "Rowing", "role": "athlete", "nomination_approved": "true", "nomination_fiscal_year": "2025-26", "level_category": "Canadian Development"},
                {"first_name": "Old", "last_name": "Year", "sport": "Soccer", "role": "athlete", "nomination_approved": "true", "nomination_fiscal_year": "2024-25", "level_category": "Canadian Elite"},
            ]
        )
        self.assertEqual(
            registration_cohort(frame, "2025-26").to_dict("records"),
            [{"Name": "Alex Smith", "Sport": "Soccer"}],
        )

    def test_services_and_registration_populate_partial_claim(self):
        cohort = pd.DataFrame([
            {"Name": "Alex Smith", "Sport": "Soccer"},
            {"Name": "Alex Smith", "Sport": "Swimming"},
            {"Name": "Jamie Lee", "Sport": "Soccer"},
        ])
        registration = pd.DataFrame([
            {"first_name": "alex", "last_name": "smith", "sport": "SOCCER", "enrollment_status": "Active", "nomination_claimed": "true", "nomination_fiscal_year": "2025-26"},
            {"first_name": "Jamie", "last_name": "Lee", "sport": "Soccer", "enrollment_status": "Inactive", "nomination_claimed": "true", "nomination_fiscal_year": "2025-26"},
        ])
        result = build_service_report(
            cohort, {"Soccer": ["S&C", "Nutrition"]}, registration, "2025-26"
        )
        self.assertEqual(result.columns.tolist(), OUTPUT_COLUMNS)
        self.assertEqual(result.loc[0, "S&C"], "Partial Claim")
        self.assertEqual(result.loc[0, "Registration"], "Partial Claim")
        self.assertEqual(result.loc[1, "S&C"], "")
        self.assertEqual(result.loc[2, "Registration"], "")

    def test_flatten_profile_keeps_the_supplied_campus(self):
        profile = {
            "person": {"first_name": "Avery"},
            "residence_city": {"name": "Victoria"},
            "carding_level_id": 5,
        }
        record = app.flatten_profile(profile, 1)
        self.assertEqual(record["campus_id"], "1")
        self.assertEqual(record["campus_label"], "CSI Pacific - Victoria")
        self.assertEqual(record["carding_level_mapped"], "D")

    def test_select_fetch_source_separates_mixed_api_rows(self):
        frame = pd.DataFrame([
            {"row_kind": "profile", "first_name": "Profile"},
            {"row_kind": "nomination", "first_name": "Nomination"},
            {"row_kind": "nomination", "first_name": "Second nomination"},
        ])
        self.assertEqual(
            app.select_fetch_source(frame, "nomination")["first_name"].tolist(),
            ["Nomination", "Second nomination"],
        )
        self.assertEqual(
            app.select_fetch_source(frame, "profile")["first_name"].tolist(),
            ["Profile"],
        )

    def test_select_fetch_source_does_not_treat_profile_rows_as_nominations(self):
        frame = pd.DataFrame([{"first_name": "Profile"}])
        self.assertTrue(app.select_fetch_source(frame, "nomination").empty)

    def test_filters_apply_and_remove_test_sports(self):
        frame = pd.DataFrame(
            [
                {
                    "person.first_name": "A",
                    "campus_by_birth": "North",
                    "current_campus": "South",
                    "current_enrollment.admin_status": "Active",
                    "current_nomination.redeemed": "True",
                    "sport.name": "Rowing",
                },
                {
                    "person.first_name": "B",
                    "campus_by_birth": "North",
                    "current_campus": "South",
                    "current_enrollment.admin_status": "Active",
                    "current_nomination.redeemed": "True",
                    "sport.name": "Cinderball (TEST)",
                },
                {
                    "person.first_name": "C",
                    "campus_by_birth": "West",
                    "current_campus": "South",
                    "current_enrollment.admin_status": "Inactive",
                    "current_nomination.redeemed": "False",
                    "sport.name": "Cycling",
                },
            ]
        )
        result = app.apply_filters(frame, "North", "South", ["Active"], ["True"])
        self.assertEqual(result["person.first_name"].tolist(), ["A"])

    def test_sensitive_fields_are_not_selected_by_default(self):
        self.assertNotIn("person.dob", app.DEFAULT_FIELDS)
        self.assertNotIn("disability", app.DEFAULT_FIELDS)
        self.assertIn("email", app.DEFAULT_FIELDS)

    def test_export_choices_are_unique_and_resolve_api_aliases(self):
        labels = [label for _, label, _ in app.EXPORT_COLUMNS]
        self.assertEqual(len(labels), len(set(labels)))
        frame = pd.DataFrame([{"person.first_name": "Avery", "sport.name": "Rowing"}])
        exported = app.build_export(frame, ["first_name", "sport"])
        self.assertEqual(exported.columns.tolist(), ["First Name", "Sport"])
        self.assertEqual(exported.iloc[0].tolist(), ["Avery", "Rowing"])

    def test_clearing_export_columns_produces_no_columns(self):
        frame = pd.DataFrame([{"first_name": "Avery"}])
        self.assertEqual(app.build_export(frame, []).columns.tolist(), [])

    def test_only_the_relevant_prebuilt_sport_selector_is_visible(self):
        self.assertEqual(
            app.show_prebuilt_sport_selector("viasport_lou"),
            ({"display": "block"}, {"display": "none"}),
        )
        self.assertEqual(
            app.show_prebuilt_sport_selector("enhanced_excellence"),
            ({"display": "none"}, {"display": "block"}),
        )
        self.assertEqual(
            app.show_prebuilt_sport_selector("coaches"),
            ({"display": "none"}, {"display": "none"}),
        )

    def test_pagination_follows_next_and_reports_complete(self):
        session = FakeSession(
            [
                FakeResponse(
                    200,
                    {
                        "results": [{"id": 1}],
                        "next": "https://example.test/profiles?page=2",
                    },
                ),
                FakeResponse(200, {"results": [{"id": 2}], "next": None}),
            ]
        )
        result = app.fetch_paginated(
            "https://example.test/profiles?campus_id=1",
            {"Authorization": "Bearer test"},
            session=session,
            sleep=lambda _: None,
        )
        self.assertTrue(result.complete)
        self.assertEqual(result.pages, 2)
        self.assertEqual([row["id"] for row in result.rows], [1, 2])
        self.assertIn("limit=100", session.urls[0])

    def test_pagination_marks_partial_results(self):
        session = FakeSession(
            [
                FakeResponse(
                    200,
                    {
                        "results": [{"id": 1}],
                        "next": "https://example.test/profiles?page=2",
                    },
                ),
                FakeResponse(500, {}),
            ]
        )
        result = app.fetch_paginated(
            "https://example.test/profiles",
            {},
            session=session,
            sleep=lambda _: None,
        )
        self.assertFalse(result.complete)
        self.assertEqual(result.rows, [{"id": 1}])
        self.assertIn("HTTP 500", result.error)

    def test_pagination_explains_forbidden_reporting_access(self):
        result = app.fetch_paginated(
            "https://example.test/report-rows",
            {"Authorization": "Bearer test"},
            session=FakeSession([FakeResponse(403, {})]),
            sleep=lambda _: None,
        )
        self.assertFalse(result.complete)
        self.assertIn("active acting profile", result.error)
        self.assertIn("page 1", result.error)

    def test_column_dictionary_explains_forbidden_reporting_access(self):
        with patch.object(app.requests, "get", return_value=FakeResponse(403, {})):
            with self.assertRaisesRegex(PermissionError, "Contact an Apps administrator"):
                app.fetch_report_columns("test")

    def test_report_rows_payload_and_column_dictionary_are_supported(self):
        definitions = [{"key": "role", "label": "Role", "target": "cells"}]
        session = FakeSession(
            [
                FakeResponse(
                    200,
                    {
                        "report": "report_rows",
                        "report_rows": [{"cells": {"role": "athlete"}}],
                        "columns": definitions,
                        "next": None,
                    },
                )
            ]
        )
        result = app.fetch_paginated(
            "https://example.test/report-rows",
            {},
            session=session,
            sleep=lambda _: None,
        )
        self.assertTrue(result.complete)
        self.assertEqual(result.columns, definitions)
        self.assertEqual(
            app.flatten_report_row(result.rows[0], definitions)["role"], "athlete"
        )

    def test_dataset_cache_uses_separate_opaque_files(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(app, "CACHE_DIR", Path(directory)):
                first_id = app.save_dataset(pd.DataFrame([{"name": "First"}]))
                second_id = app.save_dataset(pd.DataFrame([{"name": "Second"}]))
                self.assertNotEqual(first_id, second_id)
                self.assertEqual(app.load_dataset(first_id).iloc[0]["name"], "First")
                self.assertEqual(app.load_dataset(second_id).iloc[0]["name"], "Second")

    def test_every_prebuilt_report_can_be_built(self):
        frame = pd.DataFrame(
            [
                {
                    "row_kind": "profile",
                    "role": "athlete",
                    "first_name": "Avery",
                    "last_name": "Elite",
                    "email": "avery@example.test",
                    "guardian_email": "guardian@example.test",
                    "age": "18",
                    "sport": "Rowing",
                    "enrollment_status": "Pending",
                    "nomination_fiscal_year": "2025-2026",
                    "nomination_claimed": "true",
                    "nomination_approved": "true",
                    "athlete_carding": "SR",
                    "gender": "Woman",
                    "sex_of_competition": "F",
                    "current_city_campus": "Victoria",
                    "nomination_id": "N1",
                },
                {
                    "row_kind": "nomination",
                    "role": "athlete",
                    "first_name": "Avery",
                    "last_name": "Elite",
                    "email": "different@example.test",
                    "guardian_email": "different-guardian@example.test",
                    "age": "18",
                    "sport": "Rowing",
                    "nomination_sport_name": "Rowing",
                    "nomination_fiscal_year": "2025-2026",
                    "nomination_claimed": "false",
                    "nomination_approved": "true",
                    "athlete_carding": "SR",
                    "gender": "Woman",
                    "sex_of_competition": "F",
                    "current_city_campus": "Victoria",
                    "nomination_id": "N2",
                },
                {
                    "row_kind": "nomination",
                    "role": "coach",
                    "first_name": "Casey",
                    "last_name": "Coach",
                    "email": "casey@example.test",
                    "sport": "Rowing",
                    "nomination_fiscal_year": "2025-2026",
                    "nomination_claimed": "true",
                    "nomination_approved": "true",
                    "gender": "Non-binary",
                    "current_city_campus": "Victoria",
                    "nccp_number": "12345",
                    "nomination_id": "N3",
                },
                {
                    "row_kind": "profile",
                    "role": "athlete",
                    "first_name": "Jamie",
                    "last_name": "Minor",
                    "email": "jamie@example.test",
                    "guardian_email": "jamie@example.test",
                    "age": "16",
                    "sport": "Rowing",
                    "enrollment_status": "Active",
                    "nomination_fiscal_year": "2025-2026",
                    "nomination_claimed": "true",
                    "nomination_approved": "true",
                    "nomination_id": "N4",
                },
            ]
        )
        keys = {option["value"] for option in REPORT_OPTIONS}
        self.assertEqual(len(keys), 13)
        self.assertNotIn("ee_sport_template", keys)
        for key in keys:
            with self.subTest(report=key):
                artifact = build_report(
                    frame,
                    key,
                    "2025-2026",
                    ["Rowing"],
                    ["Rowing"],
                )
                self.assertTrue(artifact.content)
                self.assertGreater(artifact.row_count, 0)
                if artifact.mime_type == "application/zip":
                    with zipfile.ZipFile(io.BytesIO(artifact.content)) as archive:
                        self.assertTrue(archive.namelist())
                else:
                    self.assertTrue(artifact.filename.endswith(".csv"))

    def test_duplicate_profiles_flags_same_name_and_sport_different_email(self):
        frame = pd.DataFrame(
            [
                {"first_name": "Avery", "last_name": "Elite", "sport": "Rowing", "email": "avery@example.test"},
                {"first_name": "avery", "last_name": " elite ", "sport": "ROWING", "email": "different@example.test"},
                {"first_name": "Avery", "last_name": "Elite", "sport": "Swimming", "email": "avery@example.test"},
                {"first_name": "Casey", "last_name": "Coach", "sport": "Rowing", "email": "casey@example.test"},
                {"first_name": "", "last_name": "", "sport": "Rowing", "email": "no-name@example.test"},
            ]
        )
        output = duplicate_profiles(frame)
        self.assertEqual(sorted(output["Email"]), ["avery@example.test", "different@example.test"])
        self.assertTrue((output["Sport"].str.lower() == "rowing").all())
        self.assertNotIn("Casey", output["First Name"].values)

    def test_duplicate_profiles_ignores_matching_emails_and_test_sports(self):
        frame = pd.DataFrame(
            [
                {"first_name": "Avery", "last_name": "Elite", "sport": "Rowing", "email": "avery@example.test"},
                {"first_name": "Avery", "last_name": "Elite", "sport": "Rowing", "email": "avery@example.test"},
                {"first_name": "Dup", "last_name": "Test", "sport": "Cinderball (test)", "email": "one@example.test"},
                {"first_name": "Dup", "last_name": "Test", "sport": "Cinderball (test)", "email": "two@example.test"},
            ]
        )
        self.assertTrue(duplicate_profiles(frame).empty)

    def test_guardian_email_match_finds_and_dedupes_matching_addresses(self):
        frame = pd.DataFrame(
            [
                {
                    "first_name": "Jamie", "last_name": "Minor", "email": "Jamie@Example.test",
                    "guardian_email": "  jamie@example.test  ", "sport": "Rowing",
                },
                {
                    "row_kind": "nomination",
                    "first_name": "Jamie", "last_name": "Minor", "email": "Jamie@Example.test",
                    "guardian_email": "jamie@example.test", "sport": "Rowing",
                },
                {
                    "first_name": "Avery", "last_name": "Elite", "email": "avery@example.test",
                    "guardian_email": "guardian@example.test", "sport": "Rowing",
                },
                {
                    "first_name": "No", "last_name": "Guardian", "email": "no-guardian@example.test",
                    "guardian_email": "", "sport": "Rowing",
                },
            ]
        )
        output = guardian_email_match(frame)
        self.assertEqual(len(output), 1)
        self.assertEqual(output.iloc[0]["First Name"], "Jamie")

    def test_numbered_viasport_reports_respect_selected_fiscal_year(self):
        frame = pd.DataFrame(
            [
                {
                    "role": "athlete",
                    "first_name": "Current",
                    "sport": "Rowing",
                    "nomination_fiscal_year": "2025-2026",
                    "nomination_approved": "true",
                    "athlete_carding": "SR",
                },
                {
                    "role": "athlete",
                    "first_name": "Previous",
                    "sport": "Rowing",
                    "nomination_fiscal_year": "2024-2025",
                    "nomination_approved": "true",
                    "athlete_carding": "SR",
                },
            ]
        )
        artifact = build_report(frame, "high_performance", "2024-2025")
        exported = pd.read_csv(io.BytesIO(artifact.content))
        self.assertEqual(len(exported), 1)
        self.assertEqual(artifact.row_count, 1)

    def test_ee_sport_template_populates_workbook_and_registration_rule(self):
        frame = pd.DataFrame(
            [
                {
                    "role": "athlete",
                    "first_name": "Avery",
                    "last_name": "Athlete",
                    "sport": "Rowing",
                    "sex_of_competition": "F",
                    "athlete_carding": "Prov Dev 1",
                    "nomination_fiscal_year": "2025-2026",
                    "nomination_approved": "true",
                    "nomination_claimed": "true",
                    "enrollment_status": "Active",
                },
                {
                    "role": "coach",
                    "first_name": "Casey",
                    "last_name": "Coach",
                    "sport": "Rowing",
                    "nccp_number": "12345",
                    "nomination_fiscal_year": "2025-2026",
                    "nomination_approved": "true",
                    "nomination_claimed": "true",
                    "enrollment_status": "Pending",
                },
                {
                    "role": "athlete",
                    "first_name": "Elite",
                    "last_name": "Excluded",
                    "sport": "Rowing",
                    "sex_of_competition": "M",
                    "athlete_carding": "SR",
                    "nomination_fiscal_year": "2025-2026",
                    "nomination_approved": "true",
                    "nomination_claimed": "true",
                    "enrollment_status": "Active",
                },
                {
                    "role": "athlete",
                    "first_name": "Wrong",
                    "last_name": "Year",
                    "sport": "Rowing",
                    "nomination_fiscal_year": "2024-2025",
                    "nomination_approved": "true",
                    "nomination_claimed": "true",
                    "enrollment_status": "Active",
                },
            ]
        )
        artifact = build_ee_sport_templates(frame, "2025-2026", ["Rowing"])
        self.assertEqual(artifact.filename, "EE_Sport_Templates_2025-26.zip")
        self.assertEqual(artifact.row_count, 2)
        with zipfile.ZipFile(io.BytesIO(artifact.content)) as archive:
            self.assertEqual(archive.namelist(), ["EE_Rowing_2025-26.xlsx"])
            workbook = load_workbook(io.BytesIO(archive.read(archive.namelist()[0])), data_only=False)
        self.assertEqual(workbook.sheetnames, [
            "Athletes", "Coaches", "Domestic Results",
            "National Team", "Sevice Provider",
        ])
        self.assertEqual(workbook["Athletes"]["A7"].value, "Athlete")
        self.assertEqual(workbook["Athletes"]["B7"].value, "Avery")
        self.assertEqual(workbook["Athletes"]["C7"].value, "F")
        self.assertEqual(workbook["Athletes"]["D7"].value, "Prov Dev 1")
        self.assertEqual(workbook["Athletes"]["M7"].value, "YES")
        self.assertIsNone(workbook["Athletes"]["A8"].value)
        self.assertEqual(workbook["Coaches"]["A7"].value, "Coach")
        self.assertEqual(workbook["Coaches"]["C7"].value, "12345")
        self.assertIsNone(workbook["Coaches"]["L7"].value)
        self.assertTrue(str(workbook["Athletes"]["E39"].value).startswith("=COUNTIF"))

    def test_ee_sport_template_supports_different_year_modes_per_sport(self):
        frame = pd.DataFrame([
            {"role": "athlete", "first_name": "Current", "last_name": "Rower", "sport": "Rowing", "athlete_carding": "Prov Dev 1", "nomination_fiscal_year": "2025-2026", "nomination_approved": "true"},
            {"role": "athlete", "first_name": "Previous", "last_name": "Skier", "sport": "Alpine Ski", "athlete_carding": "Prov Dev 1", "nomination_fiscal_year": "2024-2025", "nomination_approved": "true"},
            {"role": "athlete", "first_name": "Current", "last_name": "Skier", "sport": "Alpine Ski", "athlete_carding": "Prov Dev 1", "nomination_fiscal_year": "2025-2026", "nomination_approved": "true"},
        ])
        artifact = build_ee_sport_templates(
            frame,
            "2025-2026",
            ["Rowing", "Alpine Ski"],
            sport_years={"Rowing": "current", "Alpine Ski": "both"},
        )
        self.assertEqual(artifact.filename, "EE_Sport_Templates_Mixed.zip")
        self.assertEqual(artifact.row_count, 3)
        with zipfile.ZipFile(io.BytesIO(artifact.content)) as archive:
            self.assertEqual(
                archive.namelist(),
                ["EE_Rowing_2025-26.xlsx", "EE_Alpine Ski_2024-25,2025-26.xlsx"],
            )

    def test_ee_sport_template_groups_para_and_wheelchair_sports(self):
        frame = pd.DataFrame([
            {"role": "athlete", "first_name": "Able", "last_name": "Athlete", "sport": "Athletics", "athlete_carding": "Prov Dev 1", "nomination_fiscal_year": "2025-2026", "nomination_approved": "true"},
            {"role": "athlete", "first_name": "Para", "last_name": "Athlete", "sport": "Para Athletics", "athlete_carding": "Prov Dev 1", "nomination_fiscal_year": "2025-2026", "nomination_approved": "true"},
            {"role": "athlete", "first_name": "Wheelchair", "last_name": "Basketball", "sport": "Wheelchair Basketball", "athlete_carding": "Prov Dev 1", "nomination_fiscal_year": "2025-2026", "nomination_approved": "true"},
            {"role": "athlete", "first_name": "Rugby", "last_name": "Player", "sport": "Wheelchair Rugby", "athlete_carding": "Prov Dev 1", "nomination_fiscal_year": "2025-2026", "nomination_approved": "true"},
            {"role": "athlete", "first_name": "Tennis", "last_name": "Player", "sport": "Wheelchair Tennis", "athlete_carding": "Prov Dev 1", "nomination_fiscal_year": "2025-2026", "nomination_approved": "true"},
            {"role": "athlete", "first_name": "Wheelchair", "last_name": "Runner", "sport": "Wheelchair Athletics", "athlete_carding": "Prov Dev 1", "nomination_fiscal_year": "2025-2026", "nomination_approved": "true"},
        ])
        artifact = build_ee_sport_templates(
            frame,
            "2025-2026",
            ["Para Athletics", "Wheelchair Basketball", "Wheelchair Rugby", "Wheelchair Tennis", "Wheelchair Athletics"],
        )
        self.assertEqual(artifact.row_count, 6)
        with zipfile.ZipFile(io.BytesIO(artifact.content)) as archive:
            self.assertEqual(
                archive.namelist(),
                [
                    "EE_Athletics_2025-26.xlsx",
                    "EE_Wheelchair Basketball_2025-26.xlsx",
                    "EE_Wheelchair sports_2025-26.xlsx",
                ],
            )


    def test_previous_fiscal_year_keeps_format(self):
        self.assertEqual(previous_fiscal_year("2025-2026"), "2024-2025")
        self.assertEqual(previous_fiscal_year("2025-26"), "2024-25")
        self.assertEqual(previous_fiscal_year("FY B", ["FY A", "FY B"]), "FY A")
        self.assertEqual(previous_fiscal_year(""), "")

    def test_ee_schedule_defaults_handle_sport_name_variants(self):
        self.assertEqual(default_ee_fiscal_mode(["Alpine Ski"]), "both")
        self.assertEqual(default_ee_fiscal_mode(["Para Alpine Ski"]), "both")
        self.assertEqual(default_ee_fiscal_mode(["Cross-Country Ski"]), "both")
        self.assertEqual(default_ee_fiscal_mode(["Canoe Kayak"]), "both")
        self.assertEqual(default_ee_fiscal_mode(["Artistic Gymnastics"]), "both")
        self.assertEqual(default_ee_fiscal_mode(["Wheelchair Basketball"]), "both")
        self.assertEqual(
            default_ee_fiscal_mode(["Wheelchair sports", "Wheelchair Rugby"]), "both"
        )
        self.assertIsNone(default_ee_fiscal_mode(["Artistic Swimming"]))

    def test_ee_previous_mode_uses_year_before_selected_current_year(self):
        frame = pd.DataFrame([
            {"role": "athlete", "first_name": "A", "last_name": "Old", "sport": "Rowing", "athlete_carding": "Prov Dev 1", "nomination_fiscal_year": "2023-2024", "nomination_approved": "true"},
            {"role": "athlete", "first_name": "B", "last_name": "Prev", "sport": "Rowing", "athlete_carding": "Prov Dev 2", "nomination_fiscal_year": "2024-2025", "nomination_approved": "true"},
            {"role": "athlete", "first_name": "C", "last_name": "Now", "sport": "Rowing", "athlete_carding": "Prov Dev 1", "nomination_fiscal_year": "2025-2026", "nomination_approved": "true"},
            {"role": "athlete", "first_name": "D", "last_name": "Next", "sport": "Rowing", "athlete_carding": "Prov Dev 1", "nomination_fiscal_year": "2026-2027", "nomination_approved": "true"},
        ])
        [plan] = plan_ee_workbooks(frame, "2025-2026", ["Rowing"], {"Rowing": "previous"})
        self.assertEqual(plan.years, ["2024-2025"])
        self.assertEqual(plan.athletes["last_name"].tolist(), ["Prev"])

    def test_ee_both_mode_lists_each_person_once_using_latest_year(self):
        frame = pd.DataFrame([
            {"role": "athlete", "first_name": "Sam", "last_name": "Skier", "sport": "Alpine Ski", "athlete_carding": "Prov Dev 2", "nomination_fiscal_year": "2024-2025", "nomination_approved": "true", "nomination_claimed": "false"},
            {"role": "athlete", "first_name": "Sam", "last_name": "Skier", "sport": "Alpine Ski", "athlete_carding": "Prov Dev 1", "nomination_fiscal_year": "2025-2026", "nomination_approved": "true", "nomination_claimed": "true", "enrollment_status": "Active"},
        ])
        [plan] = plan_ee_workbooks(frame, "2025-2026", ["Alpine Ski"], {"Alpine Ski": "both"})
        self.assertEqual(len(plan.athletes), 1)
        self.assertEqual(plan.athletes["athlete_carding"].iloc[0], "Prov Dev 1")
        [row] = ee_review_rows([plan])
        self.assertEqual(row["Registered athletes"], 1)
        self.assertEqual(row["Status"], "Included")

    def test_ee_review_flags_workbooks_without_rows(self):
        frame = pd.DataFrame([
            {"role": "athlete", "first_name": "C", "last_name": "Now", "sport": "Rowing", "athlete_carding": "Prov Dev 1", "nomination_fiscal_year": "2025-2026", "nomination_approved": "true"},
        ])
        plans = plan_ee_workbooks(frame, "2025-2026", ["Rowing"], {"Rowing": "previous"})
        self.assertEqual(ee_review_rows(plans)[0]["Status"], "Omitted: no qualifying athletes or coaches")
        artifact = build_ee_sport_templates(frame, "2025-2026", ["Rowing"], sport_years={"Rowing": "previous"})
        self.assertEqual(artifact.row_count, 0)

    def test_ee_coaches_sheet_has_cac_locker_status_columns(self):
        frame = pd.DataFrame([
            {"role": "coach", "first_name": f"Coach{index}", "last_name": f"Last{index:02d}", "sport": "Rowing", "nccp_number": str(1000 + index), "nomination_fiscal_year": "2025-2026", "nomination_approved": "true"}
            for index in range(20)
        ])
        artifact = build_ee_sport_templates(frame, "2025-2026", ["Rowing"])
        with zipfile.ZipFile(io.BytesIO(artifact.content)) as archive:
            workbook = load_workbook(io.BytesIO(archive.read("EE_Rowing_2025-26.xlsx")))
        sheet = workbook["Coaches"]
        self.assertEqual(sheet["M5"].value, "CAC LOCKER STATUS")
        self.assertIn("M5:P5", {str(cells) for cells in sheet.merged_cells.ranges})
        self.assertEqual(
            [sheet.cell(6, column).value for column in range(13, 17)],
            [
                "CAC NCCP Comp-Dev Status",
                "CAC NCCP Safe Sport Training Status",
                "CAC ProCoach Designation",
                "CAC ProCoach Standing",
            ],
        )
        self.assertEqual(sheet["A26"].value, "Last19")
        self.assertTrue(all(sheet.cell(row, column).value is None for row in range(7, 27) for column in range(13, 17)))
        self.assertEqual(sheet.auto_filter.ref, "A6:P26")
        self.assertEqual(sheet["L27"].value, '=COUNTIF(L7:L26,"YES")')

    def cac_upload(self, rows):
        import base64
        from openpyxl import Workbook
        workbook = Workbook()
        sheet = workbook.active
        sheet.append([None] * 14 + ["Count of cells with an entry"])
        sheet.append(["last_name", "first_name", "nccp_number", "Safesport Completion Date", "Comp Dev Level", "Comp Dev Status", "ProCoach Designation", "ProCoach Standing"])
        for row in rows:
            sheet.append(row)
        buffer = io.BytesIO()
        workbook.save(buffer)
        return "data:application/octet-stream;base64," + base64.b64encode(buffer.getvalue()).decode()

    def test_parse_cac_report_finds_header_and_merges_duplicate_nccp_rows(self):
        from datetime import datetime
        contents = self.cac_upload([
            ["Fowler", "Janice", 127736.0, datetime(2022, 10, 5), "Certified", "CertifiedWAG", "Registered", "Not current"],
            ["Fowler", "Janice", 127736.0, None, None, None, None, None],
            ["No", "Number", None, datetime(2021, 1, 1), "Trained", None, None, None],
        ])
        lookup, stats = parse_cac_report(contents, "cac.xlsx")
        self.assertEqual(stats["coaches"], 1)
        self.assertEqual(stats["skipped"], 1)
        self.assertEqual(
            lookup["127736"],
            {
                "CAC NCCP Comp-Dev Status": "Certified",
                "CAC NCCP Safe Sport Training Status": "2022-10-05",
                "CAC ProCoach Designation": "Registered",
                "CAC ProCoach Standing": "Not current",
            },
        )
        with self.assertRaises(ValueError):
            parse_cac_report(self.cac_upload([]), "notes.txt")

    def test_ee_coaches_sheet_fills_cac_columns_by_nccp_number(self):
        from datetime import datetime
        lookup, _ = parse_cac_report(
            self.cac_upload([["A", "Coach", 3638, datetime(2021, 1, 26), None, None, "Chartered", "Good"]]),
            "cac.xlsx",
        )
        frame = pd.DataFrame([
            {"role": "coach", "first_name": "Matched", "last_name": "Alpha", "sport": "Rowing", "nccp_number": "0003638", "nomination_fiscal_year": "2025-2026", "nomination_approved": "true"},
            {"role": "coach", "first_name": "Missing", "last_name": "Bravo", "sport": "Rowing", "nccp_number": "555", "nomination_fiscal_year": "2025-2026", "nomination_approved": "true"},
            {"role": "coach", "first_name": "Invalid", "last_name": "Charlie", "sport": "Rowing", "nccp_number": "COACH", "nomination_fiscal_year": "2025-2026", "nomination_approved": "true"},
        ])
        artifact = build_ee_sport_templates(frame, "2025-2026", ["Rowing"], cac_lookup=lookup)
        with zipfile.ZipFile(io.BytesIO(artifact.content)) as archive:
            sheet = load_workbook(io.BytesIO(archive.read("EE_Rowing_2025-26.xlsx")))["Coaches"]
        self.assertEqual(
            [sheet.cell(7, column).value for column in range(13, 17)],
            ["No Status", "2021-01-26", "Chartered", "Good"],
        )
        self.assertEqual([sheet.cell(8, column).value for column in range(13, 17)], [None, None, None, None])
        self.assertIsNone(sheet.cell(9, 13).value)
        [row] = ee_review_rows(plan_ee_workbooks(frame, "2025-2026", ["Rowing"]), lookup)
        self.assertEqual(row["CAC matches"], "1 of 3")

        artifact = build_ee_sport_templates(frame, "2025-2026", ["Rowing"])
        with zipfile.ZipFile(io.BytesIO(artifact.content)) as archive:
            sheet = load_workbook(io.BytesIO(archive.read("EE_Rowing_2025-26.xlsx")))["Coaches"]
        self.assertIsNone(sheet.cell(7, 13).value)

    def test_ee_registered_athlete_list_combines_workbooks(self):
        base = {"role": "athlete", "athlete_carding": "Prov Dev 1", "nomination_fiscal_year": "2025-2026", "nomination_approved": "true", "nomination_claimed": "true", "enrollment_status": "Active"}
        frame = pd.DataFrame([
            {**base, "first_name": "Rae", "last_name": "Rower", "sport": "Rowing", "email": "rae@example.com"},
            {**base, "first_name": "Sky", "last_name": "Skier", "sport": "Para Alpine Ski", "email": "sky@example.com", "guardian_email": "parent@example.com"},
            {**base, "first_name": "Al", "last_name": "Alpine", "sport": "Alpine Ski", "email": "al@example.com"},
            {**base, "first_name": "Not", "last_name": "Claimed", "sport": "Rowing", "email": "n@example.com", "nomination_claimed": "false"},
            {**base, "first_name": "Not", "last_name": "Active", "sport": "Rowing", "email": "i@example.com", "enrollment_status": "Inactive"},
            {**base, "first_name": "Senior", "last_name": "Card", "sport": "Rowing", "email": "s@example.com", "athlete_carding": "SR1"},
            {**base, "first_name": "Coach", "last_name": "Person", "sport": "Rowing", "email": "c@example.com", "role": "coach"},
        ])
        artifact = build_ee_athlete_list(frame, "2025-2026", ["Rowing", "Alpine Ski", "Para Alpine Ski"])
        self.assertEqual(artifact.filename, "EE_Registered_Athletes_2025-26.xlsx")
        self.assertEqual(artifact.row_count, 3)
        sheet = load_workbook(io.BytesIO(artifact.content))["Registered Athletes"]
        self.assertEqual(
            [[cell.value for cell in row] for row in sheet.iter_rows()],
            [
                ["Last Name", "First Name", "Sport", "Email", "Guardian Email"],
                ["Alpine", "Al", "Alpine Ski", "al@example.com", None],
                ["Skier", "Sky", "Para Alpine Ski", "sky@example.com", "parent@example.com"],
                ["Rower", "Rae", "Rowing", "rae@example.com", None],
            ],
        )

    def test_ee_keeps_every_coach_nomination_with_or_without_cac_match(self):
        from datetime import datetime
        lookup, _ = parse_cac_report(
            self.cac_upload([["A", "Coach", 1, datetime(2021, 1, 1), "Certified", None, None, None]]),
            "cac.xlsx",
        )
        base = {"role": "coach", "sport": "Cycling", "nomination_approved": "true"}
        frame = pd.DataFrame([
            {**base, "first_name": "In", "last_name": "Cac", "nccp_number": "1", "nomination_fiscal_year": "2025-2026"},
            {**base, "first_name": "In", "last_name": "Cac", "nccp_number": "1", "nomination_fiscal_year": "2024-2025"},
            {**base, "first_name": "Not", "last_name": "Cac", "nccp_number": "2", "nomination_fiscal_year": "2025-2026"},
            {**base, "first_name": "No", "last_name": "Number", "nccp_number": "", "nomination_fiscal_year": "2025-2026"},
            {**base, "first_name": "Sam", "last_name": "Same", "nccp_number": "3", "nomination_fiscal_year": "2025-2026"},
            {**base, "first_name": "Sam", "last_name": "Same", "nccp_number": "4", "nomination_fiscal_year": "2025-2026"},
            {**base, "first_name": "Abby", "last_name": "Shared", "nccp_number": "5", "nomination_fiscal_year": "2025-2026"},
            {**base, "first_name": "Ed", "last_name": "Shared", "nccp_number": "5", "nomination_fiscal_year": "2025-2026"},
            {**base, "first_name": "", "last_name": "", "nccp_number": "6", "nomination_fiscal_year": "2025-2026"},
            {**base, "first_name": "", "last_name": "", "nccp_number": "7", "nomination_fiscal_year": "2025-2026"},
        ])
        plans = plan_ee_workbooks(frame, "2025-2026", ["Cycling"], {"Cycling": "both"})
        self.assertEqual(len(plans[0].coaches), 9)
        self.assertEqual(ee_shared_nccp_coaches(plans), ["Cycling: Abby Shared / Ed Shared (5)"])
        artifact = build_ee_sport_templates(
            frame, "2025-2026", ["Cycling"], sport_years={"Cycling": "both"}, cac_lookup=lookup
        )
        self.assertEqual(artifact.row_count, 9)
        with zipfile.ZipFile(io.BytesIO(artifact.content)) as archive:
            sheet = load_workbook(io.BytesIO(archive.read(archive.namelist()[0])))["Coaches"]
        written = [sheet.cell(row, 2).value for row in range(7, 16)]
        self.assertEqual(sorted(name or "" for name in written), sorted(["", "", "In", "Not", "No", "Sam", "Sam", "Abby", "Ed"]))
        self.assertEqual(sum(sheet.cell(row, 13).value == "Certified" for row in range(7, 16)), 1)

    def test_ee_review_warns_when_data_was_fetched_as_profiles(self):
        base = {"role": "athlete", "first_name": "A", "last_name": "B", "sport": "Rowing", "athlete_carding": "Prov Dev 1", "nomination_fiscal_year": "2025-2026", "nomination_approved": "true"}
        ids = [{"type": "ee-year-dd", "workbook": "Rowing"}]
        for kind, warned in (("profile", True), ("nomination", False)):
            frame = pd.DataFrame([{**base, "row_kind": kind}])
            with self.subTest(kind=kind), patch.object(app, "load_dataset", return_value=frame):
                _, status, _, _, _ = app.review_ee_templates("x", "2025-2026", ["Rowing"], ["current"], None, ids)
                self.assertEqual(any("fetched as Profiles" in str(item.children) for item in status), warned)

    def test_select_fetch_source_all_keeps_profiles_and_nominations(self):
        frame = pd.DataFrame([
            {"first_name": "Profile", "row_kind": "profile"},
            {"first_name": "Nomination", "row_kind": "nomination"},
        ])
        self.assertEqual(
            app.select_fetch_source(frame, "all")["first_name"].tolist(),
            ["Profile", "Nomination"],
        )
        self.assertEqual(app.describe_scope("all", "", "all"), "Will request profiles and nominations for all roles from all campuses.")

    def test_ee_excludes_prov_dev_3_athletes(self):
        base = {"role": "athlete", "sport": "Rowing", "nomination_fiscal_year": "2025-2026", "nomination_approved": "true", "nomination_claimed": "true", "enrollment_status": "Active"}
        frame = pd.DataFrame([
            {**base, "first_name": "One", "last_name": "A", "athlete_carding": "Prov Dev 1"},
            {**base, "first_name": "Two", "last_name": "B", "athlete_carding": "Prov Dev 2"},
            {**base, "first_name": "Three", "last_name": "C", "athlete_carding": "Prov Dev 3"},
        ])
        [plan] = plan_ee_workbooks(frame, "2025-2026", ["Rowing"])
        self.assertEqual(plan.athletes["first_name"].tolist(), ["One", "Two"])
        self.assertEqual(build_ee_athlete_list(frame, "2025-2026", ["Rowing"]).row_count, 2)

    def test_ee_mail_merge_contacts_guardian_for_minors(self):
        base = {"role": "athlete", "athlete_carding": "Prov Dev 1", "nomination_fiscal_year": "2025-2026", "nomination_approved": "true", "nomination_claimed": "true", "enrollment_status": "Active"}
        frame = pd.DataFrame([
            {**base, "first_name": "Minor", "last_name": "A", "sport": "Rowing", "age": "16", "email": "minor@x.ca", "guardian_email": "parent@x.ca"},
            {**base, "first_name": "Adult", "last_name": "B", "sport": "Rowing", "age": "22", "email": "adult@x.ca", "guardian_email": "parent2@x.ca"},
            {**base, "first_name": "NoGuardian", "last_name": "C", "sport": "Rowing", "age": "15", "email": "kid@x.ca", "guardian_email": ""},
            {**base, "first_name": "DobOnly", "last_name": "D", "sport": "Rowing", "age": "", "dob": "2015-01-01", "email": "dob@x.ca", "guardian_email": "dobparent@x.ca"},
            {**base, "first_name": "Multi", "last_name": "E", "sport": "Rowing", "age": "25", "email": "multi@x.ca"},
            {**base, "first_name": "Multi", "last_name": "E", "sport": "Cycling", "age": "25", "email": "multi@x.ca"},
            {**base, "first_name": "Nobody", "last_name": "F", "sport": "Rowing", "age": "30", "email": "", "guardian_email": ""},
            {**base, "first_name": "Unregistered", "last_name": "G", "sport": "Rowing", "age": "30", "email": "u@x.ca", "enrollment_status": "Inactive"},
        ])
        artifact = build_ee_mail_merge(frame, "2025-2026", ["Rowing", "Cycling"])
        self.assertEqual(artifact.filename, "EE_Registered_Athletes_Mail_Merge.csv")
        output = pd.read_csv(io.BytesIO(artifact.content), keep_default_na=False)
        self.assertEqual(list(output.columns), ["First Name", "Last Name", "To", "Contact", "Sport"])
        by_name = {row["First Name"]: row for _, row in output.iterrows()}
        self.assertEqual(set(by_name), {"Minor", "Adult", "NoGuardian", "DobOnly", "Multi"})
        self.assertEqual((by_name["Minor"]["To"], by_name["Minor"]["Contact"]), ("parent@x.ca", "Guardian"))
        self.assertEqual((by_name["Adult"]["To"], by_name["Adult"]["Contact"]), ("adult@x.ca", "Athlete"))
        self.assertEqual(by_name["NoGuardian"]["To"], "kid@x.ca")
        self.assertEqual(by_name["DobOnly"]["To"], "dobparent@x.ca")
        self.assertEqual(sorted(by_name["Multi"]["Sport"].split("; ")), ["Cycling", "Rowing"])

    def test_ee_mail_merge_splits_long_lists_into_zip(self):
        base = {"role": "athlete", "sport": "Rowing", "athlete_carding": "Prov Dev 1", "nomination_fiscal_year": "2025-2026", "nomination_approved": "true", "nomination_claimed": "true", "enrollment_status": "Active", "age": "25"}
        frame = pd.DataFrame([{**base, "first_name": f"A{index}", "last_name": "L", "email": f"a{index}@x.ca"} for index in range(451)])
        artifact = build_ee_mail_merge(frame, "2025-2026", ["Rowing"])
        self.assertEqual(artifact.filename, "EE_Registered_Athletes_Mail_Merge.zip")
        with zipfile.ZipFile(io.BytesIO(artifact.content)) as archive:
            self.assertEqual(len(archive.namelist()), 2)

    def test_registration_api_defaults_to_v2(self):
        self.assertEqual(app.API_VERSION, "v2")
        self.assertTrue(app.PROFILES_URL.endswith("/api/registration/v2/report-rows/"))
        self.assertTrue(app.REPORT_COLUMNS_URL.endswith("/api/registration/v2/report-columns/"))
        self.assertTrue(app.SCHEMA_URL.endswith("/api/registration/v2/schema/"))

    def test_v1_error_wording_is_kept_when_falling_back(self):
        with patch.object(app, "API_VERSION", "v1"):
            self.assertIn(
                "needs permission to view registration reports",
                app.api_error_message(FakeResponse(403, {})),
            )

    def test_acting_profile_check(self):
        cases = [
            (FakeResponse(200, {"id": 9, "current_profile": {"id": 42}}), ""),
            (FakeResponse(200, {"id": 9, "current_profile": None}), "no active acting profile"),
            (FakeResponse(403, {}), "needs an active acting profile"),
            (FakeResponse(500, {}), ""),
        ]
        for response, expected in cases:
            with self.subTest(status=response.status_code, expected=expected):
                message = app.check_acting_profile("t", session=FakeSession([response]))
                if expected:
                    self.assertIn(expected, message)
                else:
                    self.assertEqual(message, "")
        with patch.object(app, "API_VERSION", "v1"):
            self.assertEqual(app.check_acting_profile("t", session=FakeSession([])), "")

    def test_blank_field_warnings_name_missing_permissions(self):
        frame = pd.DataFrame([
            {"row_kind": "profile", "email": "", "dob": "2000-01-01", "enrollment_status": "", "nomination_fiscal_year": "2025-2026"},
            {"row_kind": "nomination", "email": "", "dob": "", "enrollment_status": "", "nomination_fiscal_year": "2025-2026"},
        ])
        warnings = app.blank_field_warnings(frame)
        self.assertEqual(len(warnings), 2)
        self.assertIn("registration.person:view", warnings[0])
        self.assertIn("Email", warnings[0])
        self.assertIn("registration.enrollment:list", warnings[1])
        filled = frame.assign(email="a@x.ca", enrollment_status="Active")
        self.assertEqual(app.blank_field_warnings(filled), [])
        nominations_only = frame[frame["row_kind"] == "nomination"].assign(email="a@x.ca", dob="2000-01-01")
        self.assertEqual(app.blank_field_warnings(nominations_only), [])

    def test_v2_check_compares_counts_and_fill_without_personal_values(self):
        v1 = app.summarize_report_frame(pd.DataFrame([
            {"row_id": "p1", "row_kind": "profile", "role": "Athlete", "email": "a@x.ca", "nomination_fiscal_year": "2026-2027"},
            {"row_id": "p2", "row_kind": "profile", "role": "Coach", "email": "b@x.ca", "nomination_fiscal_year": "2025-2026"},
        ]), [], 1)
        v2 = app.summarize_report_frame(pd.DataFrame([
            {"row_id": "p1", "row_kind": "profile", "role": "Athlete", "email": "", "nomination_fiscal_year": "2026-2027"},
        ]), [], 1)
        comparison = app.compare_report_summaries(v1, v2)
        self.assertFalse(comparison["matches"])
        self.assertEqual(comparison["rows"], {"v1": 2, "v2": 1})
        self.assertEqual(comparison["rows_only_in_v1"], 1)
        self.assertEqual(comparison["fill_changes"]["email"], {"v1": 2, "v2": 0})
        self.assertNotIn("a@x.ca", json.dumps(comparison, default=str))
        self.assertTrue(app.compare_report_summaries(v1, v1)["matches"])

    def test_v2_check_explains_missing_rows_by_category(self):
        v1 = pd.DataFrame([
            {"row_id": "p1", "role": "Athlete", "enrollment_status": "Active", "email": "a@x.ca"},
            {"row_id": "p2", "role": "Coach", "enrollment_status": "Inactive", "email": "b@x.ca"},
        ])
        v2 = pd.DataFrame([{"row_id": "p1", "role": "Athlete", "enrollment_status": "Active", "email": ""}])
        result = app.explain_row_differences(v1, v2)
        self.assertEqual(result["rows_only_in_v1_by"]["role"], {"Coach": 1})
        self.assertEqual(result["rows_only_in_v1_by"]["enrollment_status"], {"Inactive": 1})
        self.assertEqual(result["shared_rows"], 1)
        self.assertEqual(result["shared_row_fill_changes"], {"email": {"v1": 1, "v2": 0}})

    def test_v2_probe_records_counts_and_ids_only(self):
        payload = {
            "count": 3, "next": None, "results": [
                {"row_kind": "profile", "row_id": "1", "cells": {"nomination_fiscal_year": "2025-2026", "nomination_status": "Expired", "first_name": "Ann"}},
                {"row_kind": "nomination", "row_id": "2", "cells": {"nomination_fiscal_year": "2025-2026", "nomination_status": "Expired"}},
            ],
        }
        session = FakeSession([FakeResponse(200, payload)])
        result = app.probe_report_rows("t", {"nomination_status": "expired"}, session=session)
        self.assertIn("nomination_status=expired", session.urls[0])
        self.assertEqual(result["count"], 3)
        self.assertEqual(result["first_page_fiscal_years"], {"2025-2026": 2})
        self.assertEqual(result["first_page_row_kinds"], {"profile": 1, "nomination": 1})
        self.assertNotIn("Ann", json.dumps(result))
        self.assertEqual(app.probe_report_rows("t", {}, session=FakeSession([FakeResponse(400, {})]))["http"], 400)

    def test_scope_and_actor_summaries_drop_personal_details(self):
        scopes = {"institutions": [{"id": 1, "name": "CSI Pacific", "secret": "x"}], "actor": {"email": "me@x.ca"}}
        self.assertEqual(app.scope_options(scopes), {"institutions": [{"id": 1, "name": "CSI Pacific"}]})
        actor = app.actor_context({"id": 9, "email": "me@x.ca", "current_profile": {"id": 42, "institution": {"id": 1}, "first_name": "Me"}})
        self.assertEqual(actor["current_profile"], {"id": 42, "institution": {"id": 1}})
        self.assertNotIn("me@x.ca", json.dumps(actor))

    def test_history_summary_detects_renewed_people_with_expired_nominations(self):
        latest = pd.DataFrame([
            {"row_id": "p1", "role": "Coach", "sport": "Wrestling", "nomination_id": "n2", "nomination_fiscal_year": "2026-2027", "nomination_status": "ACTIVE"},
            {"row_id": "p2", "role": "Athlete", "sport": "Rowing", "nomination_id": "n3", "nomination_fiscal_year": "2025-2026", "nomination_status": "EXPIRED"},
        ])
        expired = pd.DataFrame([
            {"row_id": "p1", "role": "Coach", "sport": "Wrestling", "nomination_id": "n1", "nomination_fiscal_year": "2025-2026", "nomination_status": "EXPIRED"},
            {"row_id": "p2", "role": "Athlete", "sport": "Rowing", "nomination_id": "n3", "nomination_fiscal_year": "2025-2026", "nomination_status": "EXPIRED"},
            {"row_id": "p9", "role": "Athlete", "sport": "Judo", "nomination_id": "n9", "nomination_fiscal_year": "2024-2025", "nomination_status": "EXPIRED"},
        ])
        result = app.nomination_history_summary(latest, expired)
        self.assertEqual(result["expired_rows"], 3)
        self.assertEqual(result["expired_rows_missing_from_latest_fetch"], 1)
        self.assertEqual(result["missing_from_latest_by_sport"], {"Judo": 1})
        self.assertEqual(result["renewed_people_with_earlier_nomination"], 1)
        self.assertEqual(result["renewed_by_fiscal_years"], {"2025-2026 -> 2026-2027": 1})
        self.assertEqual(result["renewed_by_sport"], {"Wrestling": 1})

if __name__ == "__main__":
    unittest.main()
