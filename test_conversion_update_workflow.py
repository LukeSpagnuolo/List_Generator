import base64
import io
import unittest
import zipfile

import pandas as pd

import conversion_update_workflow as cu


def dataset(rows):
    frame = pd.DataFrame(rows)
    for column in cu.DATASET_COLUMNS:
        if column not in frame:
            frame[column] = ""
    frame = frame[cu.DATASET_COLUMNS].fillna("").astype(str)
    frame["Profile_ID"] = ""
    return frame


def ds_row(first, last, sport, year, program, dob, key=None, gender="F", css="NO"):
    return {
        "Sport": sport, "First Name": first, "Last Name": last, "Gender": gender, "Date of Birth": dob,
        "Year": str(year), "Program": program, "Full_Name": key or cu.norm(first) + cu.norm(last),
        "CSS": css, "Years Targeted": "", "Convert Year": "N",
    }


def nomination(first, last, sport, fiscal, carding, dob, kind="nomination", role="Athlete", profile_id="",
               organization="Sport BC", approved="True", sex="Female"):
    return {
        "row_kind": kind, "role": role, "first_name": first, "last_name": last, "sport": sport,
        "nomination_sport_name": sport, "nomination_fiscal_year": fiscal, "athlete_carding": carding,
        "dob": dob, "sex_of_competition": sex, "nomination_organization": organization,
        "nomination_approved": approved, "profile_id": profile_id,
        "row_id": f"p:{profile_id}" if profile_id else "n:1",
    }


def run(ds, noms, decisions=None, years=None, **kwargs):
    candidates, excluded = cu.prepare_nominations(pd.DataFrame(noms), years)
    plan = cu.match_nominations(candidates, ds)
    return plan, excluded, cu.apply_update(ds, plan, decisions, **kwargs)


class PrepareNominationsTests(unittest.TestCase):
    def test_exclusions_and_program_mapping(self):
        noms = [
            nomination("Ava", "Coach", "Rowing", "2026-2027", "", "1980-01-01", role="Coach"),
            nomination("Ben", "Test", "Cinderball (TEST)", "2026-2027", "SR", "2008-01-01"),
            nomination("Cal", "Retired", "Rowing", "2026-2027", "GamePlan - Retired", "1990-01-01"),
            nomination("Dee", "Pending", "Rowing", "2026-2027", "SR", "2005-01-01", approved="False"),
            nomination("Eve", "Elite", "Rowing", "2026-2027", "SR2 ", "2003-01-01"),
            nomination("Fay", "Affiliated", "Hockey", "2026-27", "NSO Affiliated (Uncarded)", "2009-01-01"),
        ]
        candidates, excluded = cu.prepare_nominations(pd.DataFrame(noms))
        self.assertEqual(
            candidates[["First Name", "Sport", "Year", "Program"]].values.tolist(),
            [["Fay", "Ice Hockey", "2027", "Uncarded"], ["Eve", "Rowing", "2027", "SC Carded"]],
        )
        reasons = dict(zip(excluded["Name"], excluded["Reason"]))
        self.assertNotIn("Ava Coach", reasons)  # coaches are silently out of scope
        self.assertEqual(reasons["Ben Test"], "Test sport")
        self.assertIn("GamePlan - Retired", reasons["Cal Retired"])
        self.assertEqual(reasons["Dee Pending"], "Nomination not approved")

    def test_profile_and_nomination_rows_both_count_and_highest_program_wins(self):
        noms = [
            nomination("Gia", "Two", "Rowing", "2026-2027", "Prov Dev 2", "2008-02-02"),
            nomination("Gia", "Two", "Rowing", "2026-2027", "Prov Dev 1", "2008-02-02", kind="profile", profile_id="77"),
        ]
        candidates, _ = cu.prepare_nominations(pd.DataFrame(noms))
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates.iloc[0]["Program"], "Prov Dev 1")
        self.assertEqual(candidates.iloc[0]["Profile ID"], "77")

    def test_fiscal_year_filter_and_accents(self):
        noms = [
            nomination("Zoé", "Écrin", "Judo", "2025-2026", "Prov Dev 2", "2012-01-01"),
            nomination("Zoé", "Écrin", "Judo", "2026-2027", "Prov Dev 2", "2012-01-01"),
        ]
        candidates, _ = cu.prepare_nominations(pd.DataFrame(noms), ["2026-2027"])
        self.assertEqual(candidates["Year"].tolist(), ["2027"])
        self.assertEqual(candidates.iloc[0]["own"], "zoeecrin")
        self.assertEqual(candidates.iloc[0][["First Name", "Last Name"]].tolist(), ["Zoe", "Ecrin"])
        self.assertEqual(cu.root_letters("Marie-Ève O’Brien"), "Marie-Eve O’Brien")

    def test_level_order(self):
        order = sorted(cu.LEVEL, key=cu.LEVEL.get)
        self.assertEqual(order, ["Prov Dev 3", "Prov Dev 2", "Prov Dev 1", "Uncarded", "SC Carded"])
        self.assertEqual(cu.CARDING_TO_PROGRAM["c1i"], "SC Carded")


class MatchAndApplyTests(unittest.TestCase):
    def test_new_returning_upgrade_and_up_to_date(self):
        ds = dataset([
            ds_row("Ana", "Row", "Rowing", 2026, "Prov Dev 2", "2008-01-01"),
            ds_row("Bo", "Swim", "Swimming", 2027, "Prov Dev 2", "2009-01-01"),
            ds_row("Cy", "Run", "Athletics", 2027, "SC Carded", "2001-01-01"),
        ])
        noms = [
            nomination("Ana", "Row", "Rowing", "2026-2027", "Prov Dev 1", "2008-01-01"),
            nomination("Bo", "Swim", "Para Swimming", "2026-2027", "SR", "2009-01-01"),
            nomination("Cy", "Run", "Athletics", "2026-2027", "Prov Dev 1", "2001-01-01"),
            nomination("Di", "New", "Judo", "2026-2027", "Prov Dev 3", "2011-01-01"),
        ]
        plan, _, result = run(ds, noms)
        actions = dict(zip(plan["First Name"], plan["Action"]))
        self.assertEqual(actions, {"Ana": cu.ACTION_ADD, "Bo": cu.ACTION_UPGRADE, "Cy": cu.ACTION_UP_TO_DATE, "Di": cu.ACTION_ADD})
        out = result.dataset
        ana = out[out["Full_Name"] == "anarow"].sort_values("Year")
        self.assertEqual(ana["Convert Year"].tolist(), ["N", "Y"])  # Prov Dev 2 -> Prov Dev 1
        self.assertEqual(ana["Years Targeted"].tolist(), ["2", "2"])
        bo = out[out["Full_Name"] == "boswim"]
        self.assertEqual(bo[["Sport", "Program"]].values.tolist(), [["Swimming", "SC Carded"]])  # label kept
        cy = out[out["Full_Name"] == "cyrun"]
        self.assertEqual(cy["Program"].tolist(), ["SC Carded"])  # never downgraded
        self.assertEqual(len(out), 5)

    def test_rerunning_the_same_nominations_changes_nothing(self):
        ds = dataset([ds_row("Ana", "Row", "Rowing", 2026, "Prov Dev 2", "2008-01-01")])
        noms = [
            nomination("Ana", "Row", "Rowing", "2026-2027", "Prov Dev 1", "2008-01-01"),
            nomination("Di", "New", "Judo", "2026-2027", "Prov Dev 3", "2011-01-01"),
        ]
        _, _, first = run(ds, noms)
        plan, _, second = run(first.dataset, noms)
        self.assertTrue((plan["Action"] == cu.ACTION_UP_TO_DATE).all())
        pd.testing.assert_frame_equal(first.dataset, second.dataset)

    def test_same_name_different_sport_and_dob_is_a_different_person(self):
        ds = dataset([ds_row("Sam", "Lee", "Soccer", 2026, "Prov Dev 2", "2005-05-05")])
        plan, _, result = run(ds, [nomination("Sam", "Lee", "Tennis", "2026-2027", "Prov Dev 2", "2010-10-10")])
        self.assertEqual(plan.iloc[0]["Status"], cu.STATUS_NEW)
        self.assertEqual(plan.iloc[0]["Dataset key"], "samlee_2010")
        self.assertEqual(set(result.dataset["Full_Name"]), {"samlee", "samlee_2010"})

    def test_same_name_and_dob_in_new_sport_is_a_multisport_athlete(self):
        ds = dataset([ds_row("Sam", "Lee", "Soccer", 2026, "Prov Dev 2", "2005-05-05")])
        plan, _, _ = run(ds, [nomination("Sam", "Lee", "Tennis", "2026-2027", "Prov Dev 2", "2005-05-05")])
        self.assertEqual(plan.iloc[0]["Status"], cu.STATUS_NEW_SPORT)
        self.assertEqual(plan.iloc[0]["Dataset key"], "samlee")

    def test_conflicts_go_to_review_and_decisions_apply(self):
        ds = dataset([
            ds_row("Kim", "Diff", "Rowing", 2026, "Prov Dev 2", "2004-01-01"),
            ds_row("Lou", "Blank", "Rowing", 2026, "Prov Dev 2", ""),
            ds_row("Bohdin", "Morris", "Rugby", 2026, "Prov Dev 2", "2008-02-24"),
        ])
        noms = [
            nomination("Kim", "Diff", "Rowing", "2026-2027", "Prov Dev 1", "2006-07-09"),
            nomination("Lou", "Blank", "Rowing", "2026-2027", "Prov Dev 1", "2007-07-07"),
            nomination("Bohdan", "Morris", "Rugby", "2026-2027", "Prov Dev 2", "2008-02-24"),
        ]
        plan, _, held = run(ds, noms)
        self.assertEqual(
            sorted(plan["Status"]),
            sorted([cu.REVIEW_DOB_DIFFERS, cu.REVIEW_NO_DATASET_DOB, cu.REVIEW_NAME_SPELLING]),
        )
        self.assertEqual(held.counts["held"], 3)
        self.assertEqual(held.counts["rows_added"], 0)
        ids = dict(zip(plan["First Name"], plan["Review ID"]))
        decisions = {ids["Kim"]: cu.DIFFERENT, ids["Lou"]: cu.SAME, ids["Bohdan"]: cu.SAME}
        result = cu.apply_update(ds, plan, decisions)
        out = result.dataset
        self.assertEqual(set(out.loc[out["First Name"] == "Kim", "Full_Name"]), {"kimdiff", "kimdiff_2006"})
        self.assertEqual(set(out.loc[out["Full_Name"] == "loublank", "Date of Birth"]), {"2007-07-07"})
        bohdin = out[out["Full_Name"] == "bohdinmorris"].sort_values("Year")
        self.assertEqual(bohdin["Year"].tolist(), ["2026", "2027"])
        # The spelling filed under that athlete matches directly next time.
        replan = cu.match_nominations(cu.prepare_nominations(pd.DataFrame(noms[2:]))[0], out)
        self.assertEqual(replan.iloc[0]["Action"], cu.ACTION_UP_TO_DATE)

    def test_dob_typo_uses_newest_nomination_unless_implausible(self):
        ds = dataset([
            ds_row("Tia", "Typo", "Rowing", 2026, "Prov Dev 2", "2005-03-04"),
            ds_row("Eli", "Eade", "Volleyball", 2026, "Prov Dev 2", "2002-06-11"),
        ])
        noms = [
            nomination("Tia", "Typo", "Rowing", "2026-2027", "Prov Dev 2", "2005-04-03"),
            nomination("Eli", "Eade", "Volleyball", "2026-2027", "Prov Dev 2", "2020-06-11"),
        ]
        plan, _, result = run(ds, noms)
        self.assertEqual(set(plan["Status"]), {cu.STATUS_MATCHED_TYPO})
        out = result.dataset
        self.assertEqual(set(out.loc[out["Full_Name"] == "tiatypo", "Date of Birth"]), {"2005-04-03"})
        self.assertEqual(set(out.loc[out["Full_Name"] == "elieade", "Date of Birth"]), {"2002-06-11"})
        kept = cu.apply_update(ds, plan, apply_typo_dobs=False).dataset
        self.assertEqual(set(kept.loc[kept["Full_Name"] == "tiatypo", "Date of Birth"]), {"2005-03-04"})

    def test_older_year_nomination_with_other_dob_is_already_filed(self):
        ds = dataset([
            ds_row("Meg", "Au", "Judo", 2026, "Prov Dev 2", "2010-05-22"),
            ds_row("Meg", "Au", "Judo", 2027, "Prov Dev 3", "2010-05-22"),
        ])
        plan, _, _ = run(ds, [nomination("Meg", "Au", "Judo", "2025-2026", "Prov Dev 2", "2010-07-15")])
        self.assertEqual(plan.iloc[0]["Action"], cu.ACTION_UP_TO_DATE)

    def test_profile_id_is_stored_and_then_matches_a_renamed_athlete(self):
        ds = dataset([ds_row("Jo", "Smith", "Rowing", 2026, "Prov Dev 2", "2008-01-01")])
        _, _, first = run(ds, [nomination("Jo", "Smith", "Rowing", "2026-2027", "Prov Dev 2", "2008-01-01",
                                          kind="profile", profile_id="501")])
        self.assertEqual(set(first.dataset["Profile_ID"]), {"501"})
        plan, _, _ = run(first.dataset, [nomination("Jo", "Smith-Jones", "Rowing", "2027-2028", "Prov Dev 1",
                                                    "2008-01-01", kind="profile", profile_id="501")])
        self.assertEqual(plan.iloc[0]["Status"], cu.STATUS_MATCHED_ID)
        self.assertEqual(plan.iloc[0]["Dataset key"], "josmith")

    def test_legacy_accent_keys_still_match(self):
        ds = dataset([ds_row("Josué", "Luma", "Soccer", 2026, "Prov Dev 3", "2011-07-15", key="josuluma")])
        plan, _, _ = run(ds, [nomination("Josué", "Luma", "Soccer", "2025-2026", "Prov Dev 3", "2011-07-15")])
        self.assertEqual(plan.iloc[0]["Action"], cu.ACTION_UP_TO_DATE)

    def test_css_multisport_rule(self):
        ds = dataset([ds_row("Ky", "Css", "Multisport", 2027, "Prov Dev 3", "2011-01-01", css="YES")])
        noms = [
            nomination("Ky", "Css", "Soccer", "2026-2027", "Prov Dev 2", "2011-01-01"),
            nomination("Ri", "School", "Multisport", "2026-2027", "Prov Dev 3", "2012-01-01",
                       organization="Canadian Sport School"),
            nomination("Ri", "School", "Karate", "2026-2027", "Prov Dev 2", "2012-01-01"),
        ]
        plan, _, result = run(ds, noms)
        self.assertEqual(plan.set_index(["First Name", "Sport"]).loc[("Ri", "Multisport"), "Action"], cu.ACTION_CSS_ONLY)
        out = result.dataset.set_index(["Full_Name", "Sport"])
        self.assertNotIn(("kycss", "Multisport"), out.index)
        self.assertEqual(out.loc[("kycss", "Soccer"), "CSS"], "YES")
        self.assertEqual(out.loc[("rischool", "Karate"), "CSS"], "YES")
        self.assertNotIn(("rischool", "Multisport"), out.index)
        self.assertEqual(result.counts["multisport_removed"], 1)

    def test_upload_round_trip_and_zip(self):
        ds = dataset([ds_row("Ana", "Row", "Rowing", 2026, "Prov Dev 2", "2008-01-01")]).drop(columns="Profile_ID")
        encoded = "data:text/csv;base64," + base64.b64encode(ds.to_csv(index=False).encode()).decode()
        loaded = cu.read_conversion_csv(encoded, "Conversion_Data.csv")
        self.assertIn("Profile_ID", loaded)
        with self.assertRaises(ValueError):
            cu.read_conversion_csv("data:text/csv;base64," + base64.b64encode(b"a,b\n1,2\n").decode(), "x.csv")
        plan, excluded, result = run(loaded, [nomination("Ana", "Row", "Rowing", "2026-2027", "Prov Dev 1", "2008-01-01")])
        archive = zipfile.ZipFile(io.BytesIO(cu.build_update_zip(result, excluded, "Conversion_Data.csv")))
        self.assertEqual(
            sorted(archive.namelist()),
            sorted(["Conversion_Data.csv", "conversion_update_changes.csv",
                    "conversion_update_held_for_review.csv", "conversion_update_excluded.csv",
                    cu.DECISIONS_FILENAME]),
        )
        written = pd.read_csv(archive.open("Conversion_Data.csv"), dtype=str, keep_default_na=False)
        self.assertEqual(list(written.columns), cu.DATASET_COLUMNS + ["Profile_ID"])
        self.assertEqual(len(written), 2)


class SavedDecisionTests(unittest.TestCase):
    DS = [
        ds_row("Kim", "Diff", "Rowing", 2026, "Prov Dev 2", "2004-01-01"),
        ds_row("Lou", "Blank", "Rowing", 2026, "Prov Dev 2", ""),
        ds_row("Nia", "Nodob", "Judo", 2026, "Prov Dev 2", "2009-09-09"),
        ds_row("Nia", "Nodob", "Judo", 2026, "Prov Dev 2", "2003-03-03", key="nianodob_2003"),
    ]
    NOMS = [
        nomination("Kim", "Diff", "Rowing", "2026-2027", "Prov Dev 1", "2006-07-09"),
        nomination("Lou", "Blank", "Rowing", "2026-2027", "Prov Dev 1", "2007-07-07"),
        nomination("Nia", "Nodob", "Judo", "2026-2027", "Prov Dev 1", ""),
    ]

    def decide(self):
        ds = dataset(self.DS)
        plan, _, _ = run(ds, self.NOMS)
        ids = dict(zip(plan["First Name"], plan["Review ID"]))
        decisions = {ids["Kim"]: cu.DIFFERENT, ids["Lou"]: cu.SAME, ids["Nia"]: cu.SAME}
        return ds, plan, cu.apply_update(ds, plan, decisions)

    def test_decisions_are_recorded_with_the_final_key(self):
        _, _, result = self.decide()
        recorded = result.decisions.set_index("First Name")
        self.assertEqual(recorded.loc["Kim", "Dataset key"], "kimdiff_2006")
        self.assertEqual(recorded.loc["Lou", ["Decision", "Dataset key"]].tolist(), [cu.SAME, "loublank"])
        self.assertEqual(recorded.loc["Nia", "DOB"], "")

    def test_rerun_on_the_same_base_reuses_saved_decisions(self):
        ds, _, first = self.decide()
        saved = cu.parse_decisions_csv(first.decisions.to_csv(index=False).encode())
        candidates, _ = cu.prepare_nominations(pd.DataFrame(self.NOMS))
        plan = cu.match_nominations(candidates, ds, saved)
        self.assertFalse((plan["Action"] == cu.ACTION_REVIEW).any())
        self.assertEqual(
            dict(zip(plan["First Name"], plan["Status"])),
            {"Kim": cu.STATUS_SAVED_DIFFERENT, "Lou": cu.STATUS_SAVED_SAME, "Nia": cu.STATUS_SAVED_SAME},
        )
        again = cu.apply_update(ds, plan)
        pd.testing.assert_frame_equal(first.dataset, again.dataset)

    def test_rerun_on_the_updated_dataset_needs_no_review(self):
        _, _, first = self.decide()
        plan, _, second = run(first.dataset, self.NOMS)
        self.assertTrue((plan["Action"] == cu.ACTION_UP_TO_DATE).all(), plan[["First Name", "Status"]])
        self.assertEqual(second.changes["Change"].tolist(), [])

    def test_merge_keeps_the_latest_decision_per_nomination(self):
        _, _, first = self.decide()
        changed = first.decisions.copy()
        changed.loc[changed["First Name"] == "Kim", "Decision"] = cu.SAME
        merged = cu.merge_decisions(first.decisions, changed[changed["First Name"] == "Kim"])
        self.assertEqual(len(merged), 3)
        self.assertEqual(merged.set_index("First Name").loc["Kim", "Decision"], cu.SAME)
        self.assertTrue(cu.is_decisions_csv(merged.to_csv(index=False).encode()))
        self.assertFalse(cu.is_decisions_csv(dataset(self.DS).to_csv(index=False).encode()))

    def test_no_dob_nomination_already_filed_this_year_matches(self):
        ds = dataset([
            ds_row("Nia", "Nodob", "Judo", 2027, "Prov Dev 2", "2009-09-09"),
            ds_row("Nia", "Nodob", "Judo", 2026, "Prov Dev 2", "2003-03-03", key="nianodob_2003"),
        ])
        plan, _, _ = run(ds, [nomination("Nia", "Nodob", "Judo", "2026-2027", "Prov Dev 2", "")])
        self.assertEqual(plan.iloc[0]["Status"], cu.STATUS_FILED_NO_DOB)
        self.assertEqual(plan.iloc[0]["Dataset key"], "nianodob")


class DobMergeTests(unittest.TestCase):
    def test_profile_id_match_applies_the_nomination_dob(self):
        ds = dataset([ds_row("Jo", "Smith", "Rowing", 2026, "Prov Dev 2", "2008-01-01")])
        ds["Profile_ID"] = "501"
        _, _, result = run(ds, [nomination("Jo", "Smith", "Rowing", "2026-2027", "Prov Dev 2", "2007-12-15",
                                           kind="profile", profile_id="501")])
        self.assertEqual(set(result.dataset["Date of Birth"]), {"2007-12-15"})
        self.assertEqual(result.counts["dob_changes"], 1)

    def test_blank_dataset_dobs_are_filled_from_matched_nominations(self):
        ds = dataset([
            ds_row("Meg", "Au", "Judo", 2026, "Prov Dev 2", ""),
            ds_row("Meg", "Au", "Judo", 2027, "Prov Dev 3", "2010-05-22"),
        ])
        _, _, result = run(ds, [nomination("Meg", "Au", "Judo", "2027-2028", "Prov Dev 3", "2010-05-22")])
        self.assertEqual(set(result.dataset["Date of Birth"]), {"2010-05-22"})
        self.assertEqual(result.counts["dob_filled"], 1)

    def test_older_nomination_fills_blanks_but_never_overwrites(self):
        ds = dataset([
            ds_row("Meg", "Au", "Judo", 2026, "Prov Dev 2", ""),
            ds_row("Meg", "Au", "Judo", 2027, "Prov Dev 3", ""),
        ])
        plan, _, result = run(ds, [nomination("Meg", "Au", "Judo", "2025-2026", "Prov Dev 2", "2010-07-15")])
        self.assertEqual(plan.iloc[0]["Action"], cu.ACTION_UP_TO_DATE)
        self.assertEqual(set(result.dataset["Date of Birth"]), {"2010-07-15"})
        ds.loc[1, "Date of Birth"] = "2010-05-22"
        _, _, kept = run(ds, [nomination("Meg", "Au", "Judo", "2025-2026", "Prov Dev 2", "2010-07-15")])
        self.assertEqual(kept.dataset["Date of Birth"].tolist(), ["2010-05-22", "2010-05-22"])

    def test_newest_nomination_dob_wins(self):
        ds = dataset([ds_row("Lou", "Blank", "Rowing", 2025, "Prov Dev 2", "")])
        _, _, result = run(ds, [
            nomination("Lou", "Blank", "Rowing", "2025-2026", "Prov Dev 2", "2007-07-17"),
            nomination("Lou", "Blank", "Rowing", "2026-2027", "Prov Dev 2", "2007-07-07"),
        ], decisions=None)
        # Both nominations go to review (dataset has no DOB) and are held: nothing changes.
        self.assertEqual(set(result.dataset["Date of Birth"]), {""})
        plan, _, _ = run(ds, [
            nomination("Lou", "Blank", "Rowing", "2025-2026", "Prov Dev 2", "2007-07-17"),
            nomination("Lou", "Blank", "Rowing", "2026-2027", "Prov Dev 2", "2007-07-07"),
        ])
        decided = cu.apply_update(ds, plan, {rid: cu.SAME for rid in plan["Review ID"] if rid})
        self.assertEqual(set(decided.dataset["Date of Birth"]), {"2007-07-07"})


    def test_same_year_conflicting_dobs_settle_on_the_profile_dob(self):
        ds = dataset([ds_row("Owen", "Marc", "Rowing", 2026, "Prov Dev 1", "2004-02-22")])
        noms = [
            nomination("Owen", "Marc", "Rowing", "2026-2027", "SR", "2004-02-22"),
            nomination("Owen", "Marc", "Rowing", "2026-2027", "Prov Dev 1", "2004-01-22", kind="profile", profile_id="77"),
        ]
        current = ds
        for _ in range(3):
            _, _, result = run(current, noms)
            current = result.dataset
        self.assertEqual(set(current["Date of Birth"]), {"2004-01-22"})
        _, _, last = run(current, noms)
        self.assertEqual(last.counts["data_changes"], 0)

    def test_two_same_year_nominations_of_one_athlete_settle(self):
        ds = dataset([ds_row("Mad", "Louis", "Lacrosse", 2026, "Prov Dev 2", "2009-06-10")])
        noms = [
            nomination("Mad", "Louis", "Lacrosse", "2025-2026", "Prov Dev 2", "2009-06-10"),
            nomination("Mad", "Louis", "Lacrosse", "2025-2026", "Prov Dev 2", "2009-11-13"),
        ]
        plan, _, _ = run(ds, noms)
        rid = plan.loc[plan["Review ID"] != "", "Review ID"].iloc[0]
        current = cu.apply_update(ds, plan, {rid: cu.SAME})
        saved = current.decisions
        for _ in range(3):
            candidates, _ = cu.prepare_nominations(pd.DataFrame(noms))
            replan = cu.match_nominations(candidates, current.dataset, saved)
            self.assertFalse((replan["Action"] == cu.ACTION_REVIEW).any())
            current = cu.apply_update(current.dataset, replan)
            saved = cu.merge_decisions(saved, current.decisions)
        self.assertEqual(current.counts["data_changes"], 0)

    def test_dob_typo_still_links_a_new_sport(self):
        ds = dataset([ds_row("Preston", "Holt", "Judo", 2027, "Prov Dev 2", "2010-07-08")])
        plan, _, _ = run(ds, [nomination("Preston", "Holt", "Multisport", "2026-2027", "Prov Dev 3", "2010-08-07",
                                         organization="Canadian Sport School")])
        self.assertEqual(plan.iloc[0]["Dataset key"], "prestonholt")
        self.assertEqual(plan.iloc[0]["Action"], cu.ACTION_CSS_ONLY)


class FakeResponse:
    def __init__(self, status_code, payload=None, content=b""):
        self.status_code, self._payload, self.content = status_code, payload, content

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, *responses):
        self.responses, self.calls = list(responses), []

    def _call(self, method, url, **kwargs):
        self.calls.append((url, kwargs) if method == "get" else (method, url, kwargs))
        return self.responses.pop(0)

    def get(self, url, **kwargs):
        return self._call("get", url, **kwargs)

    def post(self, url, **kwargs):
        return self._call("post", url, **kwargs)

    def patch(self, url, **kwargs):
        return self._call("patch", url, **kwargs)


class GitHubSourceTests(unittest.TestCase):
    COMMIT = [{"sha": "abc1234def", "html_url": "https://github.com/x/y/commit/abc1234def",
               "commit": {"message": "Add 2027 nominations\n\nbody", "committer": {"date": "2026-11-06T21:30:07Z"}}}]

    def csv_bytes(self):
        return dataset([ds_row("Ana", "Row", "Rowing", 2026, "Prov Dev 2", "2008-01-01")]).to_csv(index=False).encode()

    def test_reads_the_file_at_the_latest_commit(self):
        session = FakeSession(FakeResponse(200, self.COMMIT), FakeResponse(200, content=self.csv_bytes()))
        frame, info = cu.fetch_conversion_from_github(cu.GitHubSource(), session=session)
        self.assertEqual(len(frame), 1)
        self.assertEqual(info["sha"], "abc1234def")
        self.assertEqual(info["message"], "Add 2027 nominations")
        self.assertIn("/abc1234def/Conversion_Data.csv", session.calls[1][0])
        self.assertEqual(session.calls[0][1]["params"]["path"], "Conversion_Data.csv")
        self.assertNotIn("Authorization", session.calls[0][1]["headers"])

    def test_token_is_sent_when_set(self):
        session = FakeSession(FakeResponse(200, self.COMMIT), FakeResponse(200, content=self.csv_bytes()))
        cu.fetch_conversion_from_github(cu.GitHubSource(token="t0k"), session=session)
        self.assertEqual(session.calls[0][1]["headers"]["Authorization"], "Bearer t0k")
        self.assertEqual(session.calls[1][1]["headers"]["Authorization"], "Bearer t0k")

    def test_errors_are_explained(self):
        for status, text in ((403, "GITHUB_TOKEN"), (404, "couldn't find"), (500, "HTTP 500")):
            with self.assertRaisesRegex(ValueError, text):
                cu.fetch_conversion_from_github(cu.GitHubSource(), session=FakeSession(FakeResponse(status, {})))
        with self.assertRaisesRegex(ValueError, "No commits"):
            cu.fetch_conversion_from_github(cu.GitHubSource(), session=FakeSession(FakeResponse(200, [])))
        bad = FakeSession(FakeResponse(200, self.COMMIT), FakeResponse(200, content=b"a,b\n1,2\n"))
        with self.assertRaisesRegex(ValueError, "missing columns"):
            cu.fetch_conversion_from_github(cu.GitHubSource(), session=bad)


class GitHubPushTests(unittest.TestCase):
    SOURCE = cu.GitHubSource(repo="me/Conversion", token="t0k")

    def ok_session(self, latest="loaded123"):
        return FakeSession(
            FakeResponse(200, [{"sha": latest}]),                       # latest commit touching the file
            FakeResponse(200, {"object": {"sha": "head456"}}),          # branch head
            FakeResponse(200, {"tree": {"sha": "tree789"}}),            # head commit
            FakeResponse(201, {"sha": "blob111"}),
            FakeResponse(201, {"sha": "tree222"}),
            FakeResponse(201, {"sha": "new333", "html_url": "https://github.com/me/Conversion/commit/new333"}),
            FakeResponse(200, {"object": {"sha": "new333"}}),
        )

    def test_push_commits_on_top_of_the_branch_without_force(self):
        session = self.ok_session()
        pushed = cu.push_conversion_to_github(self.SOURCE, b"a,b\n", "loaded123", "msg", session=session)
        self.assertEqual(pushed["sha"], "new333")
        tree_call = session.calls[4]
        self.assertEqual(tree_call[2]["json"]["base_tree"], "tree789")
        self.assertEqual(tree_call[2]["json"]["tree"][0]["path"], "Conversion_Data.csv")
        commit_call = session.calls[5]
        self.assertEqual(commit_call[2]["json"]["parents"], ["head456"])
        patch_call = session.calls[6]
        self.assertEqual(patch_call[0], "patch")
        self.assertFalse(patch_call[2]["json"]["force"])
        self.assertEqual(base64.b64decode(session.calls[3][2]["json"]["content"]), b"a,b\n")

    def test_push_can_include_the_decisions_file_in_the_same_commit(self):
        session = self.ok_session()
        session.responses.insert(4, FakeResponse(201, {"sha": "blob112"}))
        cu.push_conversion_to_github(self.SOURCE, b"a,b\n", "loaded123", "msg", session=session,
                                     extra_files={self.SOURCE.decisions_path: b"Decision,Dataset key\n"})
        tree = session.calls[5][2]["json"]["tree"]
        self.assertEqual([entry["path"] for entry in tree],
                         ["Conversion_Data.csv", "conversion_review_decisions.csv"])
        self.assertEqual(cu.GitHubSource(path="data/x.csv").decisions_path, "data/conversion_review_decisions.csv")

    def test_missing_decisions_file_is_empty(self):
        frame = cu.fetch_decisions_from_github(self.SOURCE, "abc", session=FakeSession(FakeResponse(404)))
        self.assertTrue(frame.empty)
        self.assertEqual(list(frame.columns), cu.DECISION_COLUMNS)

    def test_refuses_when_file_changed_since_load(self):
        with self.assertRaisesRegex(ValueError, "changed on GitHub since you loaded it"):
            cu.push_conversion_to_github(self.SOURCE, b"x", "loaded123", "msg", session=self.ok_session(latest="other999"))

    def test_rejected_fast_forward_is_explained(self):
        session = self.ok_session()
        session.responses[-1] = FakeResponse(422, {})
        with self.assertRaisesRegex(ValueError, "Someone pushed"):
            cu.push_conversion_to_github(self.SOURCE, b"x", "loaded123", "msg", session=session)

    def test_needs_a_token_and_permission(self):
        with self.assertRaisesRegex(ValueError, "GITHUB_TOKEN"):
            cu.push_conversion_to_github(cu.GitHubSource(), b"x", "sha", "msg", session=FakeSession())
        session = FakeSession(FakeResponse(200, [{"sha": "loaded123"}]), FakeResponse(404, {}))
        with self.assertRaisesRegex(ValueError, "Read and write"):
            cu.push_conversion_to_github(self.SOURCE, b"x", "loaded123", "msg", session=session)

    def test_commit_message_summarises_the_update(self):
        ds = dataset([ds_row("Ana", "Row", "Rowing", 2026, "Prov Dev 2", "2008-01-01")])
        _, _, result = run(ds, [nomination("Ana", "Row", "Rowing", "2026-2027", "Prov Dev 1", "2008-01-01")])
        message = cu.commit_message(result, ["2026-2027"])
        self.assertTrue(message.startswith("Conversion update: +1 rows, 0 program upgrades (2026-2027)"))
        self.assertIn("Rows 1 -> 2", message)


if __name__ == "__main__":
    unittest.main()
