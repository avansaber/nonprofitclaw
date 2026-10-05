"""Tests for NonprofitClaw volunteers, campaigns, pledges, and compliance.

Actions tested:
  Volunteers:
  - nonprofit-add-volunteer
  - nonprofit-update-volunteer
  - nonprofit-list-volunteers
  - nonprofit-get-volunteer
  - nonprofit-add-volunteer-shift
  - nonprofit-list-volunteer-shifts
  - nonprofit-complete-volunteer-shift
  - nonprofit-volunteer-hours-report

  Campaigns & Pledges:
  - nonprofit-add-campaign
  - nonprofit-update-campaign
  - nonprofit-list-campaigns
  - nonprofit-get-campaign
  - nonprofit-activate-campaign
  - nonprofit-close-campaign
  - nonprofit-add-pledge
  - nonprofit-list-pledges
  - nonprofit-get-pledge
  - nonprofit-fulfill-pledge
  - nonprofit-cancel-pledge

  Compliance:
  - nonprofit-generate-tax-receipt
  - nonprofit-list-tax-receipts
  - nonprofit-donor-summary
  - status (module_status)
"""
import pytest
from decimal import Decimal
from nonprofit_helpers import (
    call_action, ns, is_error, is_ok, load_db_query,
    get_conn, init_all_tables,
    seed_company, seed_volunteer, seed_program, seed_campaign, seed_donor,
    seed_donation, seed_fund, seed_naming_series, snapshot_tables, _uuid,
)

mod = load_db_query()


# ─────────────────────────────────────────────────────────────────────────────
# Volunteers
# ─────────────────────────────────────────────────────────────────────────────

class TestAddVolunteer:
    def test_create_volunteer(self, conn, env):
        import volunteers as vol_mod
        result = call_action(vol_mod.add_volunteer, conn, ns(
            company_id=env["company_id"],
            name="Sarah Wilson",
            email="sarah@example.com",
            phone="555-9876",
            skills="teaching,counseling",
            availability="weekends",
            start_date="2026-01-15",
        ))
        assert is_ok(result), result
        assert result["name"] == "Sarah Wilson"
        assert "id" in result

    def test_missing_name_fails(self, conn, env):
        import volunteers as vol_mod
        result = call_action(vol_mod.add_volunteer, conn, ns(
            company_id=env["company_id"],
            name=None,
            email=None, phone=None, skills=None,
            availability=None, start_date=None,
        ))
        assert is_error(result)


class TestUpdateVolunteer:
    def test_update_skills(self, conn, env):
        import volunteers as vol_mod
        result = call_action(vol_mod.update_volunteer, conn, ns(
            id=env["volunteer_id"],
            name=None, email=None, phone=None,
            skills="mentoring,driving,cooking",
            availability=None, start_date=None, is_active=None,
        ))
        assert is_ok(result), result
        assert result["updated"] is True

    def test_deactivate_volunteer(self, conn, env):
        import volunteers as vol_mod
        result = call_action(vol_mod.update_volunteer, conn, ns(
            id=env["volunteer_id"],
            name=None, email=None, phone=None,
            skills=None, availability=None, start_date=None,
            is_active="0",
        ))
        assert is_ok(result), result

        row = conn.execute(
            "SELECT is_active FROM nonprofitclaw_volunteer WHERE id=?",
            (env["volunteer_id"],)
        ).fetchone()
        assert row["is_active"] == 0


class TestListVolunteers:
    def test_list_all(self, conn, env):
        import volunteers as vol_mod
        result = call_action(vol_mod.list_volunteers, conn, ns(
            company_id=env["company_id"],
            is_active=None, search=None,
            limit="50", offset="0",
        ))
        assert is_ok(result), result
        assert result["total"] >= 1


class TestGetVolunteer:
    def test_get_existing(self, conn, env):
        import volunteers as vol_mod
        result = call_action(vol_mod.get_volunteer, conn, ns(
            id=env["volunteer_id"],
        ))
        assert is_ok(result), result
        assert result["volunteer"]["id"] == env["volunteer_id"]
        assert "recent_shifts" in result["volunteer"]


# ─────────────────────────────────────────────────────────────────────────────
# Volunteer Shifts
# ─────────────────────────────────────────────────────────────────────────────

class TestAddVolunteerShift:
    def test_create_shift(self, conn, env):
        import volunteers as vol_mod
        result = call_action(vol_mod.add_volunteer_shift, conn, ns(
            company_id=env["company_id"],
            volunteer_id=env["volunteer_id"],
            program_id=env["program_id"],
            shift_date="2026-03-15",
            hours="4.5",
            description="Morning tutoring session",
        ))
        assert is_ok(result), result
        assert result["hours"] == "4.50"

    def test_missing_hours_fails(self, conn, env):
        import volunteers as vol_mod
        result = call_action(vol_mod.add_volunteer_shift, conn, ns(
            company_id=env["company_id"],
            volunteer_id=env["volunteer_id"],
            program_id=None,
            shift_date=None,
            hours=None,
            description=None,
        ))
        assert is_error(result)

    def test_bad_volunteer_fails(self, conn, env):
        import volunteers as vol_mod
        result = call_action(vol_mod.add_volunteer_shift, conn, ns(
            company_id=env["company_id"],
            volunteer_id="nonexistent",
            program_id=None,
            shift_date=None,
            hours="3",
            description=None,
        ))
        assert is_error(result)


class TestCompleteVolunteerShift:
    def test_complete_shift_updates_totals(self, conn, env):
        import volunteers as vol_mod

        # Create a shift
        add_result = call_action(vol_mod.add_volunteer_shift, conn, ns(
            company_id=env["company_id"],
            volunteer_id=env["volunteer_id"],
            program_id=None,
            shift_date="2026-03-10",
            hours="3.00",
            description="Event setup",
        ))
        assert is_ok(add_result), add_result
        shift_id = add_result["id"]

        # Complete it
        result = call_action(vol_mod.complete_volunteer_shift, conn, ns(
            id=shift_id,
            hours=None,
        ))
        assert is_ok(result), result
        assert result["completed"] is True
        assert Decimal(result["hours"]) == Decimal("3.00")
        assert int(result["volunteer_shift_count"]) >= 1

    def test_complete_with_updated_hours(self, conn, env):
        import volunteers as vol_mod

        add_result = call_action(vol_mod.add_volunteer_shift, conn, ns(
            company_id=env["company_id"],
            volunteer_id=env["volunteer_id"],
            program_id=None,
            shift_date="2026-03-11",
            hours="4.00",
            description=None,
        ))
        assert is_ok(add_result)

        result = call_action(vol_mod.complete_volunteer_shift, conn, ns(
            id=add_result["id"],
            hours="5.50",
        ))
        assert is_ok(result), result
        assert result["hours"] == "5.50"

    def test_complete_already_completed_fails(self, conn, env):
        import volunteers as vol_mod

        add_result = call_action(vol_mod.add_volunteer_shift, conn, ns(
            company_id=env["company_id"],
            volunteer_id=env["volunteer_id"],
            program_id=None,
            shift_date="2026-03-12",
            hours="2.00",
            description=None,
        ))
        assert is_ok(add_result)

        # Complete once
        call_action(vol_mod.complete_volunteer_shift, conn, ns(
            id=add_result["id"], hours=None,
        ))

        # Try again
        result = call_action(vol_mod.complete_volunteer_shift, conn, ns(
            id=add_result["id"], hours=None,
        ))
        assert is_error(result)


class TestListVolunteerShifts:
    """Behaviour of nonprofit-list-volunteer-shifts, read back from the database.

    The shifts below are created through nonprofit-add-volunteer-shift, then
    the stored nonprofitclaw_volunteer_shift rows are read back and compared
    field-for-field with what the list response returns. Completing one shift
    through nonprofit-complete-volunteer-shift must be reflected in the
    status filter.

    nonprofit-list-volunteer-shifts never posts to the general ledger (no
    gl_entry write exists in volunteers.py), so this asserts stored rows,
    never ledger legs; a later reader must not add a balanced-legs assertion
    here.
    """

    def test_list_shifts(self, conn, env):
        import volunteers as vol_mod
        first = call_action(vol_mod.add_volunteer_shift, conn, ns(
            company_id=env["company_id"],
            volunteer_id=env["volunteer_id"],
            program_id=env["program_id"],
            shift_date="2026-03-15",
            hours="4.50",
            description="Morning tutoring",
        ))
        assert is_ok(first), first
        second = call_action(vol_mod.add_volunteer_shift, conn, ns(
            company_id=env["company_id"],
            volunteer_id=env["volunteer_id"],
            program_id=None,
            shift_date="2026-04-10",
            hours="2.25",
            description="Event setup",
        ))
        assert is_ok(second), second

        stored = {
            row["id"]: dict(row) for row in conn.execute(
                "SELECT * FROM nonprofitclaw_volunteer_shift").fetchall()
        }
        assert stored[first["id"]]["hours"] == "4.50"
        assert stored[second["id"]]["hours"] == "2.25"
        assert stored[first["id"]]["volunteer_id"] == env["volunteer_id"]
        assert stored[first["id"]]["program_id"] == env["program_id"]
        assert stored[second["id"]]["program_id"] is None
        assert stored[first["id"]]["shift_date"] == "2026-03-15"
        assert stored[first["id"]]["description"] == "Morning tutoring"
        assert stored[first["id"]]["status"] == "scheduled"

        assert is_ok(call_action(vol_mod.complete_volunteer_shift, conn, ns(
            id=first["id"], hours=None,
        )))
        assert conn.execute(
            "SELECT status FROM nonprofitclaw_volunteer_shift WHERE id=?",
            (first["id"],)).fetchone()["status"] == "completed"

        result = call_action(vol_mod.list_volunteer_shifts, conn, ns(
            company_id=env["company_id"],
            volunteer_id=None, program_id=None, status=None,
            from_date=None, to_date=None,
            limit="50", offset="0",
        ))
        assert is_ok(result), result
        assert result["total"] == 2
        listed = {entry["id"]: entry for entry in result["volunteer_shifts"]}
        assert set(listed) == {first["id"], second["id"]}
        for shift_id in (first["id"], second["id"]):
            row = conn.execute(
                "SELECT * FROM nonprofitclaw_volunteer_shift WHERE id=?",
                (shift_id,)).fetchone()
            entry = listed[shift_id]
            assert entry["hours"] == row["hours"]
            assert Decimal(entry["hours"]) == Decimal(row["hours"])
            assert entry["volunteer_id"] == row["volunteer_id"]
            assert entry["program_id"] == row["program_id"]
            assert entry["shift_date"] == row["shift_date"]
            assert entry["description"] == row["description"]
            assert entry["status"] == row["status"]
            assert entry["naming_series"] == row["naming_series"]
        assert listed[first["id"]]["status"] == "completed"
        assert listed[second["id"]]["status"] == "scheduled"
        assert listed[first["id"]]["volunteer_name"] == "Bob Helper"
        assert listed[first["id"]]["program_name"] == "Youth Outreach"

        by_volunteer = call_action(vol_mod.list_volunteer_shifts, conn, ns(
            company_id=env["company_id"],
            volunteer_id=env["volunteer_id"], program_id=None, status=None,
            from_date=None, to_date=None,
            limit="50", offset="0",
        ))
        assert is_ok(by_volunteer), by_volunteer
        assert by_volunteer["total"] == 2

        by_program = call_action(vol_mod.list_volunteer_shifts, conn, ns(
            company_id=env["company_id"],
            volunteer_id=None, program_id=env["program_id"], status=None,
            from_date=None, to_date=None,
            limit="50", offset="0",
        ))
        assert is_ok(by_program), by_program
        assert (by_program["total"],
                [entry["id"] for entry in by_program["volunteer_shifts"]]) == (
            1, [first["id"]])

        completed = call_action(vol_mod.list_volunteer_shifts, conn, ns(
            company_id=env["company_id"],
            volunteer_id=None, program_id=None, status="completed",
            from_date=None, to_date=None,
            limit="50", offset="0",
        ))
        assert is_ok(completed), completed
        assert (completed["total"],
                [entry["id"] for entry in completed["volunteer_shifts"]]) == (
            1, [first["id"]])

        in_range = call_action(vol_mod.list_volunteer_shifts, conn, ns(
            company_id=env["company_id"],
            volunteer_id=None, program_id=None, status=None,
            from_date="2026-04-01", to_date="2026-04-30",
            limit="50", offset="0",
        ))
        assert is_ok(in_range), in_range
        assert (in_range["total"],
                [entry["id"] for entry in in_range["volunteer_shifts"]]) == (
            1, [second["id"]])

        other_company = seed_company(conn)
        seed_naming_series(conn, other_company)
        other_volunteer = seed_volunteer(conn, other_company, "Far Helper",
                                         "far@example.com")
        assert is_ok(call_action(vol_mod.add_volunteer_shift, conn, ns(
            company_id=other_company,
            volunteer_id=other_volunteer,
            program_id=None,
            shift_date="2026-05-01",
            hours="1.00",
            description="Elsewhere",
        )))
        reseeded = call_action(vol_mod.list_volunteer_shifts, conn, ns(
            company_id=env["company_id"],
            volunteer_id=None, program_id=None, status=None,
            from_date=None, to_date=None,
            limit="50", offset="0",
        ))
        assert is_ok(reseeded), reseeded
        assert reseeded["total"] == 2

    def test_list_shifts_missing_company_refuses_without_writes(self, conn, env):
        import volunteers as vol_mod
        before = snapshot_tables(conn, ["nonprofitclaw_volunteer_shift",
                                        "audit_log"])
        result = call_action(vol_mod.list_volunteer_shifts, conn, ns(
            company_id=None,
            volunteer_id=None, program_id=None, status=None,
            from_date=None, to_date=None,
            limit="50", offset="0",
        ))
        assert is_error(result)
        assert result["message"] == "--company-id is required"
        assert snapshot_tables(conn, ["nonprofitclaw_volunteer_shift",
                                      "audit_log"]) == before


class TestVolunteerHoursReport:
    def test_hours_report(self, conn, env):
        import volunteers as vol_mod

        # Create and complete a shift first
        add_result = call_action(vol_mod.add_volunteer_shift, conn, ns(
            company_id=env["company_id"],
            volunteer_id=env["volunteer_id"],
            program_id=env["program_id"],
            shift_date="2026-03-01",
            hours="6.00",
            description="Full day",
        ))
        assert is_ok(add_result)
        call_action(vol_mod.complete_volunteer_shift, conn, ns(
            id=add_result["id"], hours=None,
        ))

        result = call_action(vol_mod.volunteer_hours_report, conn, ns(
            company_id=env["company_id"],
            from_date=None, to_date=None,
        ))
        assert is_ok(result), result
        assert "volunteers" in result
        assert "total_hours" in result
        assert "total_shifts" in result
        assert int(result["total_shifts"]) >= 1


# ─────────────────────────────────────────────────────────────────────────────
# Campaigns
# ─────────────────────────────────────────────────────────────────────────────

class TestAddCampaign:
    def test_create_campaign(self, conn, env):
        import campaigns as camp_mod
        result = call_action(camp_mod.add_campaign, conn, ns(
            company_id=env["company_id"],
            name="Year-End Appeal",
            description="Annual year-end fundraising",
            fund_id=env["fund_id"],
            goal_amount="25000",
            start_date="2026-11-01",
            end_date="2026-12-31",
        ))
        assert is_ok(result), result
        assert result["name"] == "Year-End Appeal"

    def test_missing_name_fails(self, conn, env):
        import campaigns as camp_mod
        result = call_action(camp_mod.add_campaign, conn, ns(
            company_id=env["company_id"],
            name=None,
            description=None,
            fund_id=None,
            goal_amount=None,
            start_date=None,
            end_date=None,
        ))
        assert is_error(result)


class TestUpdateCampaign:
    def test_update_goal(self, conn, env):
        import campaigns as camp_mod
        result = call_action(camp_mod.update_campaign, conn, ns(
            id=env["campaign_id"],
            name=None, description=None,
            start_date=None, end_date=None,
            goal_amount="50000.00",
            fund_id=None,
        ))
        assert is_ok(result), result
        assert result["updated"] is True

    def test_update_completed_campaign_fails(self, conn, env):
        import campaigns as camp_mod
        cid = seed_campaign(conn, env["company_id"], "Done", status="completed")
        result = call_action(camp_mod.update_campaign, conn, ns(
            id=cid,
            name="Try Update", description=None,
            start_date=None, end_date=None,
            goal_amount=None, fund_id=None,
        ))
        assert is_error(result)


class TestListCampaigns:
    def test_list_all(self, conn, env):
        import campaigns as camp_mod
        result = call_action(camp_mod.list_campaigns, conn, ns(
            company_id=env["company_id"],
            status=None, search=None,
            limit="50", offset="0",
        ))
        assert is_ok(result), result
        assert result["total"] >= 1


class TestGetCampaign:
    def test_get_existing(self, conn, env):
        import campaigns as camp_mod
        result = call_action(camp_mod.get_campaign, conn, ns(
            id=env["campaign_id"],
        ))
        assert is_ok(result), result
        assert result["campaign"]["id"] == env["campaign_id"]
        assert "pledge_count" in result["campaign"]
        assert "donation_count" in result["campaign"]


class TestActivateCampaign:
    def test_activate_draft(self, conn, env):
        import campaigns as camp_mod
        cid = seed_campaign(conn, env["company_id"], "Draft Camp",
                            status="draft")
        result = call_action(camp_mod.activate_campaign, conn, ns(id=cid))
        assert is_ok(result), result
        assert result["campaign_status"] == "active"

    def test_activate_non_draft_fails(self, conn, env):
        import campaigns as camp_mod
        # env["campaign_id"] is already active
        result = call_action(camp_mod.activate_campaign, conn, ns(
            id=env["campaign_id"],
        ))
        assert is_error(result)


class TestCloseCampaign:
    def test_close_active_campaign(self, conn, env):
        import campaigns as camp_mod
        result = call_action(camp_mod.close_campaign, conn, ns(
            id=env["campaign_id"],
        ))
        assert is_ok(result), result
        assert result["campaign_status"] == "completed"

    def test_close_already_completed_fails(self, conn, env):
        import campaigns as camp_mod
        cid = seed_campaign(conn, env["company_id"], "Already Done",
                            status="completed")
        result = call_action(camp_mod.close_campaign, conn, ns(id=cid))
        assert is_error(result)


# ─────────────────────────────────────────────────────────────────────────────
# Pledges
# ─────────────────────────────────────────────────────────────────────────────

class TestAddPledge:
    def test_create_pledge(self, conn, env):
        import campaigns as camp_mod
        result = call_action(camp_mod.add_pledge, conn, ns(
            company_id=env["company_id"],
            donor_id=env["donor_id"],
            campaign_id=env["campaign_id"],
            fund_id=None,
            amount="5000",
            pledge_date="2026-03-01",
            frequency="monthly",
            next_due_date="2026-04-01",
            end_date="2026-12-31",
            notes="Monthly pledge",
        ))
        assert is_ok(result), result
        assert result["amount"] == "5000.00"

    def test_pledge_missing_donor_fails(self, conn, env):
        import campaigns as camp_mod
        result = call_action(camp_mod.add_pledge, conn, ns(
            company_id=env["company_id"],
            donor_id=None,
            campaign_id=None,
            fund_id=None,
            amount="100",
            pledge_date=None,
            frequency=None,
            next_due_date=None,
            end_date=None,
            notes=None,
        ))
        assert is_error(result)

    def test_pledge_to_inactive_campaign_fails(self, conn, env):
        import campaigns as camp_mod
        cid = seed_campaign(conn, env["company_id"], "Draft Camp",
                            status="draft")
        result = call_action(camp_mod.add_pledge, conn, ns(
            company_id=env["company_id"],
            donor_id=env["donor_id"],
            campaign_id=cid,
            fund_id=None,
            amount="100",
            pledge_date=None,
            frequency=None,
            next_due_date=None,
            end_date=None,
            notes=None,
        ))
        assert is_error(result)


class TestListPledges:
    """Behaviour of nonprofit-list-pledges, read back from the database.

    The pledges below are created through nonprofit-add-pledge, then the
    stored nonprofitclaw_pledge rows are read back and compared field-for-
    field with what the list response returns. A partial fulfillment through
    nonprofit-fulfill-pledge must be reflected in the remaining and
    percent_fulfilled the list computes.

    nonprofit-list-pledges never posts to the general ledger (no gl_entry
    write exists in campaigns.py), so this asserts stored rows, never ledger
    legs; a later reader must not add a balanced-legs assertion here.
    """

    def test_list_pledges(self, conn, env):
        import campaigns as camp_mod
        first = call_action(camp_mod.add_pledge, conn, ns(
            company_id=env["company_id"],
            donor_id=env["donor_id"],
            campaign_id=env["campaign_id"],
            fund_id=None,
            amount="1000.00",
            pledge_date="2026-03-01",
            frequency="one_time",
            next_due_date=None,
            end_date=None,
            notes=None,
        ))
        assert is_ok(first), first
        second = call_action(camp_mod.add_pledge, conn, ns(
            company_id=env["company_id"],
            donor_id=env["donor_id"],
            campaign_id=env["campaign_id"],
            fund_id=None,
            amount="250.50",
            pledge_date="2026-04-01",
            frequency="monthly",
            next_due_date=None,
            end_date=None,
            notes="Monthly gift",
        ))
        assert is_ok(second), second

        stored = {
            row["id"]: dict(row) for row in conn.execute(
                "SELECT * FROM nonprofitclaw_pledge").fetchall()
        }
        assert stored[first["id"]]["amount"] == "1000.00"
        assert stored[second["id"]]["amount"] == "250.50"
        assert stored[first["id"]]["donor_id"] == env["donor_id"]
        assert stored[first["id"]]["campaign_id"] == env["campaign_id"]
        assert stored[first["id"]]["pledge_date"] == "2026-03-01"
        assert stored[second["id"]]["frequency"] == "monthly"
        assert stored[second["id"]]["notes"] == "Monthly gift"
        assert stored[first["id"]]["status"] == "active"

        partial = call_action(camp_mod.fulfill_pledge, conn, ns(
            pledge_id=first["id"], id=None, amount="400.00",
        ))
        assert is_ok(partial), partial
        assert partial["remaining"] == "600.00"
        row = conn.execute(
            "SELECT * FROM nonprofitclaw_pledge WHERE id=?",
            (first["id"],)).fetchone()
        assert row["fulfilled_amount"] == "400.00"
        assert row["status"] == "partially_fulfilled"

        result = call_action(camp_mod.list_pledges, conn, ns(
            company_id=env["company_id"],
            donor_id=None, campaign_id=None, status=None,
            limit="50", offset="0",
        ))
        assert is_ok(result), result
        assert result["total"] == 2
        listed = {entry["id"]: entry for entry in result["pledges"]}
        assert set(listed) == {first["id"], second["id"]}
        for pledge_id in (first["id"], second["id"]):
            row = conn.execute(
                "SELECT * FROM nonprofitclaw_pledge WHERE id=?",
                (pledge_id,)).fetchone()
            entry = listed[pledge_id]
            assert entry["amount"] == row["amount"]
            assert Decimal(entry["amount"]) == Decimal(row["amount"])
            assert entry["fulfilled_amount"] == row["fulfilled_amount"]
            assert entry["donor_id"] == row["donor_id"]
            assert entry["campaign_id"] == row["campaign_id"]
            assert entry["pledge_date"] == row["pledge_date"]
            assert entry["frequency"] == row["frequency"]
            assert entry["status"] == row["status"]
            assert entry["naming_series"] == row["naming_series"]
        assert listed[first["id"]]["remaining"] == "600.00"
        assert listed[first["id"]]["percent_fulfilled"] == "40.00"
        assert listed[second["id"]]["remaining"] == "250.50"
        assert listed[first["id"]]["donor_name"] == "Alice Benefactor"
        assert listed[first["id"]]["campaign_name"] == "Annual Giving"

        by_donor = call_action(camp_mod.list_pledges, conn, ns(
            company_id=env["company_id"],
            donor_id=env["donor_id"], campaign_id=None, status=None,
            limit="50", offset="0",
        ))
        assert is_ok(by_donor), by_donor
        assert by_donor["total"] == 2

        by_campaign = call_action(camp_mod.list_pledges, conn, ns(
            company_id=env["company_id"],
            donor_id=None, campaign_id=env["campaign_id"], status=None,
            limit="50", offset="0",
        ))
        assert is_ok(by_campaign), by_campaign
        assert by_campaign["total"] == 2

        partial_list = call_action(camp_mod.list_pledges, conn, ns(
            company_id=env["company_id"],
            donor_id=None, campaign_id=None, status="partially_fulfilled",
            limit="50", offset="0",
        ))
        assert is_ok(partial_list), partial_list
        assert (partial_list["total"],
                [entry["id"] for entry in partial_list["pledges"]]) == (
            1, [first["id"]])

        other_company = seed_company(conn)
        seed_naming_series(conn, other_company)
        other_donor = seed_donor(conn, other_company, "Far Donor")
        other_campaign = seed_campaign(conn, other_company, "Far Campaign",
                                       "5000.00", "active")
        assert is_ok(call_action(camp_mod.add_pledge, conn, ns(
            company_id=other_company,
            donor_id=other_donor["donor_id"],
            campaign_id=other_campaign,
            fund_id=None,
            amount="10.00",
            pledge_date="2026-05-01",
            frequency="one_time",
            next_due_date=None,
            end_date=None,
            notes=None,
        )))
        reseeded = call_action(camp_mod.list_pledges, conn, ns(
            company_id=env["company_id"],
            donor_id=None, campaign_id=None, status=None,
            limit="50", offset="0",
        ))
        assert is_ok(reseeded), reseeded
        assert reseeded["total"] == 2

    def test_list_pledges_missing_company_refuses_without_writes(self, conn, env):
        import campaigns as camp_mod
        before = snapshot_tables(conn, ["nonprofitclaw_pledge", "audit_log"])
        result = call_action(camp_mod.list_pledges, conn, ns(
            company_id=None,
            donor_id=None, campaign_id=None, status=None,
            limit="50", offset="0",
        ))
        assert is_error(result)
        assert result["message"] == "--company-id is required"
        assert snapshot_tables(conn, ["nonprofitclaw_pledge",
                                      "audit_log"]) == before


class TestGetPledge:
    def _create_pledge(self, conn, env):
        import campaigns as camp_mod
        r = call_action(camp_mod.add_pledge, conn, ns(
            company_id=env["company_id"],
            donor_id=env["donor_id"],
            campaign_id=env["campaign_id"],
            fund_id=None,
            amount="1000",
            pledge_date="2026-03-01",
            frequency="one_time",
            next_due_date=None,
            end_date=None,
            notes=None,
        ))
        assert is_ok(r), r
        return r["id"]

    def test_get_existing_pledge(self, conn, env):
        import campaigns as camp_mod
        pid = self._create_pledge(conn, env)
        result = call_action(camp_mod.get_pledge, conn, ns(id=pid))
        assert is_ok(result), result
        assert result["pledge"]["id"] == pid
        assert "remaining" in result["pledge"]


class TestFulfillPledge:
    def _create_pledge(self, conn, env, amount="1000"):
        import campaigns as camp_mod
        r = call_action(camp_mod.add_pledge, conn, ns(
            company_id=env["company_id"],
            donor_id=env["donor_id"],
            campaign_id=env["campaign_id"],
            fund_id=None,
            amount=amount,
            pledge_date="2026-03-01",
            frequency="one_time",
            next_due_date=None,
            end_date=None,
            notes=None,
        ))
        assert is_ok(r), r
        return r["id"]

    def test_partial_fulfillment(self, conn, env):
        import campaigns as camp_mod
        pid = self._create_pledge(conn, env, "1000")
        result = call_action(camp_mod.fulfill_pledge, conn, ns(
            id=pid, pledge_id=None, amount="400",
        ))
        assert is_ok(result), result
        assert result["pledge_status"] == "partially_fulfilled"
        assert result["fulfilled_amount"] == "400.00"
        assert result["remaining"] == "600.00"

    def test_full_fulfillment(self, conn, env):
        import campaigns as camp_mod
        pid = self._create_pledge(conn, env, "500")
        result = call_action(camp_mod.fulfill_pledge, conn, ns(
            id=pid, pledge_id=None, amount="500",
        ))
        assert is_ok(result), result
        assert result["pledge_status"] == "fulfilled"
        assert result["remaining"] == "0.00"

    def test_over_fulfillment_fails(self, conn, env):
        import campaigns as camp_mod
        pid = self._create_pledge(conn, env, "200")
        result = call_action(camp_mod.fulfill_pledge, conn, ns(
            id=pid, pledge_id=None, amount="300",
        ))
        assert is_error(result)


class TestCancelPledge:
    def test_cancel_active_pledge(self, conn, env):
        import campaigns as camp_mod
        r = call_action(camp_mod.add_pledge, conn, ns(
            company_id=env["company_id"],
            donor_id=env["donor_id"],
            campaign_id=env["campaign_id"],
            fund_id=None,
            amount="500",
            pledge_date="2026-03-01",
            frequency="one_time",
            next_due_date=None,
            end_date=None,
            notes=None,
        ))
        assert is_ok(r)
        result = call_action(camp_mod.cancel_pledge, conn, ns(id=r["id"]))
        assert is_ok(result), result
        assert result["pledge_status"] == "cancelled"

    def test_cancel_fulfilled_pledge_fails(self, conn, env):
        import campaigns as camp_mod
        r = call_action(camp_mod.add_pledge, conn, ns(
            company_id=env["company_id"],
            donor_id=env["donor_id"],
            campaign_id=env["campaign_id"],
            fund_id=None,
            amount="100",
            pledge_date="2026-03-01",
            frequency="one_time",
            next_due_date=None,
            end_date=None,
            notes=None,
        ))
        assert is_ok(r)
        # Fulfill it
        call_action(camp_mod.fulfill_pledge, conn, ns(
            id=r["id"], pledge_id=None, amount="100",
        ))
        # Try to cancel
        result = call_action(camp_mod.cancel_pledge, conn, ns(id=r["id"]))
        assert is_error(result)


# ─────────────────────────────────────────────────────────────────────────────
# Compliance — Tax Receipts
# ─────────────────────────────────────────────────────────────────────────────

class TestGenerateTaxReceipt:
    def test_single_receipt(self, conn, env):
        import compliance as comp_mod
        donation_id = seed_donation(conn, env["company_id"], env["donor_id"],
                                    "500.00")
        result = call_action(comp_mod.generate_tax_receipt, conn, ns(
            company_id=env["company_id"],
            donor_id=env["donor_id"],
            tax_year="2026",
            receipt_type="single",
            donation_id=donation_id,
            sent_method="email",
        ))
        assert is_ok(result), result
        assert result["amount"] == "500.00"
        assert result["tax_year"] == "2026"
        assert result["receipt_type"] == "single"

    def test_duplicate_receipt_fails(self, conn, env):
        import compliance as comp_mod
        donation_id = seed_donation(conn, env["company_id"], env["donor_id"],
                                    "200.00")
        # First receipt
        r1 = call_action(comp_mod.generate_tax_receipt, conn, ns(
            company_id=env["company_id"],
            donor_id=env["donor_id"],
            tax_year="2026",
            receipt_type="single",
            donation_id=donation_id,
            sent_method=None,
        ))
        assert is_ok(r1)

        # Duplicate
        r2 = call_action(comp_mod.generate_tax_receipt, conn, ns(
            company_id=env["company_id"],
            donor_id=env["donor_id"],
            tax_year="2026",
            receipt_type="single",
            donation_id=donation_id,
            sent_method=None,
        ))
        assert is_error(r2)

    def test_receipt_for_refunded_donation_fails(self, conn, env):
        import compliance as comp_mod
        donation_id = seed_donation(conn, env["company_id"], env["donor_id"],
                                    "300.00", status="refunded")
        result = call_action(comp_mod.generate_tax_receipt, conn, ns(
            company_id=env["company_id"],
            donor_id=env["donor_id"],
            tax_year="2026",
            receipt_type="single",
            donation_id=donation_id,
            sent_method=None,
        ))
        assert is_error(result)

    def test_annual_summary_no_donations_fails(self, conn, env):
        """Annual summary with no deductible donations returns error."""
        import compliance as comp_mod
        result = call_action(comp_mod.generate_tax_receipt, conn, ns(
            company_id=env["company_id"],
            donor_id=env["donor_id"],
            tax_year="2025",
            receipt_type="annual_summary",
            donation_id=None,
            sent_method=None,
        ))
        assert is_error(result)
        assert "No tax-deductible donations" in result["message"]

    def test_annual_summary_missing_tax_year_fails(self, conn, env):
        """Annual summary without --tax-year returns error."""
        import compliance as comp_mod
        result = call_action(comp_mod.generate_tax_receipt, conn, ns(
            company_id=env["company_id"],
            donor_id=env["donor_id"],
            tax_year=None,
            receipt_type="annual_summary",
            donation_id=None,
            sent_method=None,
        ))
        assert is_error(result)


class TestListTaxReceipts:
    """Behaviour of nonprofit-list-tax-receipts, read back from the database.

    The receipt below is created through nonprofit-generate-tax-receipt, then
    the stored nonprofitclaw_tax_receipt row is read back and compared
    field-for-field with what the list response returns. Issuing the receipt
    must also flip the donation's receipt_sent flag from 0 to 1.

    nonprofit-list-tax-receipts never posts to the general ledger (no
    gl_entry write exists in compliance.py), so this asserts stored rows,
    never ledger legs; a later reader must not add a balanced-legs assertion
    here.
    """

    def test_list_receipts(self, conn, env):
        import compliance as comp_mod
        donation_id = seed_donation(conn, env["company_id"], env["donor_id"],
                                    "1500.10", fund_id=env["fund_id"],
                                    campaign_id=env["campaign_id"])
        assert conn.execute(
            "SELECT receipt_sent FROM nonprofitclaw_donation WHERE id=?",
            (donation_id,)).fetchone()["receipt_sent"] == 0

        created = call_action(comp_mod.generate_tax_receipt, conn, ns(
            company_id=env["company_id"],
            donor_id=env["donor_id"],
            tax_year="2026",
            receipt_type="single",
            donation_id=donation_id,
            sent_method="email",
        ))
        assert is_ok(created), created
        assert created["amount"] == "1500.10"

        row = conn.execute(
            "SELECT * FROM nonprofitclaw_tax_receipt WHERE id=?",
            (created["id"],)).fetchone()
        assert row["amount"] == "1500.10"
        assert Decimal(row["amount"]) == Decimal("1500.10")
        assert row["donor_id"] == env["donor_id"]
        assert row["donation_id"] == donation_id
        assert row["tax_year"] == "2026"
        assert row["receipt_type"] == "single"
        assert row["sent_method"] == "email"
        assert conn.execute(
            "SELECT receipt_sent FROM nonprofitclaw_donation WHERE id=?",
            (donation_id,)).fetchone()["receipt_sent"] == 1

        result = call_action(comp_mod.list_tax_receipts, conn, ns(
            company_id=env["company_id"],
            donor_id=None, tax_year=None, receipt_type=None,
            limit="50", offset="0",
        ))
        assert is_ok(result), result
        assert result["total"] == 1
        assert len(result["tax_receipts"]) == 1
        entry = result["tax_receipts"][0]
        assert entry["id"] == created["id"]
        assert entry["amount"] == row["amount"]
        assert Decimal(entry["amount"]) == Decimal("1500.10")
        assert entry["donor_id"] == row["donor_id"]
        assert entry["donation_id"] == row["donation_id"]
        assert entry["tax_year"] == row["tax_year"]
        assert entry["receipt_type"] == row["receipt_type"]
        assert entry["sent_method"] == row["sent_method"]
        assert entry["naming_series"] == row["naming_series"]
        assert entry["donor_name"] == "Alice Benefactor"

        by_year = call_action(comp_mod.list_tax_receipts, conn, ns(
            company_id=env["company_id"],
            donor_id=None, tax_year="2026", receipt_type=None,
            limit="50", offset="0",
        ))
        assert is_ok(by_year), by_year
        assert by_year["total"] == 1
        other_year = call_action(comp_mod.list_tax_receipts, conn, ns(
            company_id=env["company_id"],
            donor_id=None, tax_year="2025", receipt_type=None,
            limit="50", offset="0",
        ))
        assert is_ok(other_year), other_year
        assert (other_year["total"], other_year["tax_receipts"]) == (0, [])

        by_type = call_action(comp_mod.list_tax_receipts, conn, ns(
            company_id=env["company_id"],
            donor_id=None, tax_year=None, receipt_type="single",
            limit="50", offset="0",
        ))
        assert is_ok(by_type), by_type
        assert by_type["total"] == 1
        annual = call_action(comp_mod.list_tax_receipts, conn, ns(
            company_id=env["company_id"],
            donor_id=None, tax_year=None, receipt_type="annual_summary",
            limit="50", offset="0",
        ))
        assert is_ok(annual), annual
        assert (annual["total"], annual["tax_receipts"]) == (0, [])

        by_donor = call_action(comp_mod.list_tax_receipts, conn, ns(
            company_id=env["company_id"],
            donor_id=env["donor_id"], tax_year=None, receipt_type=None,
            limit="50", offset="0",
        ))
        assert is_ok(by_donor), by_donor
        assert by_donor["total"] == 1

        other_company = seed_company(conn)
        seed_naming_series(conn, other_company)
        other_donor = seed_donor(conn, other_company, "Far Donor")
        other_donation = seed_donation(conn, other_company,
                                       other_donor["donor_id"], "99.99")
        assert is_ok(call_action(comp_mod.generate_tax_receipt, conn, ns(
            company_id=other_company,
            donor_id=other_donor["donor_id"],
            tax_year="2026",
            receipt_type="single",
            donation_id=other_donation,
            sent_method=None,
        )))
        reseeded = call_action(comp_mod.list_tax_receipts, conn, ns(
            company_id=env["company_id"],
            donor_id=None, tax_year=None, receipt_type=None,
            limit="50", offset="0",
        ))
        assert is_ok(reseeded), reseeded
        assert reseeded["total"] == 1
        assert reseeded["tax_receipts"][0]["id"] == created["id"]

    def test_list_receipts_missing_company_refuses_without_writes(self, conn, env):
        import compliance as comp_mod
        before = snapshot_tables(conn, ["nonprofitclaw_tax_receipt",
                                        "nonprofitclaw_donation",
                                        "audit_log"])
        result = call_action(comp_mod.list_tax_receipts, conn, ns(
            company_id=None,
            donor_id=None, tax_year=None, receipt_type=None,
            limit="50", offset="0",
        ))
        assert is_error(result)
        assert result["message"] == "--company-id is required"
        assert snapshot_tables(conn, ["nonprofitclaw_tax_receipt",
                                      "nonprofitclaw_donation",
                                      "audit_log"]) == before


# ─────────────────────────────────────────────────────────────────────────────
# Compliance — Summary & Status
# ─────────────────────────────────────────────────────────────────────────────

class TestDonorSummary:
    def test_donor_summary(self, conn, env):
        import compliance as comp_mod
        # Add a donation so stats are non-zero
        seed_donation(conn, env["company_id"], env["donor_id"], "250.00")
        result = call_action(comp_mod.donor_summary, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert result["total_donors"] >= 1
        assert result["active_donors"] >= 1
        assert "donor_levels" in result
        assert "top_donors" in result
        assert "monthly_trend" in result


class TestModuleStatus:
    def test_module_status(self, conn, env):
        import compliance as comp_mod
        result = call_action(comp_mod.module_status, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert result["module"] == "nonprofitclaw"
        assert result["module_status"] == "operational"
        assert "record_counts" in result
        assert "donors" in result["record_counts"]
        assert "donations" in result["record_counts"]
        assert "funds" in result["record_counts"]

    def test_module_status_missing_company_fails(self, conn, env):
        import compliance as comp_mod
        result = call_action(comp_mod.module_status, conn, ns(
            company_id=None,
        ))
        assert is_error(result)


# ─────────────────────────────────────────────────────────────────────────────
# Donor substantiation and split receipts v1 (floor-o061)
# ─────────────────────────────────────────────────────────────────────────────

class TestDonorSubstantiationSplitReceipts:
    def _add_donation(self, conn, env, amount, goods_fv=None, goods_desc=None,
                      in_kind_fv=None):
        add = mod.ACTIONS["nonprofit-add-donation"]
        return call_action(add, conn, ns(
            company_id=env["company_id"],
            donor_id=env["donor_id"],
            amount=amount,
            in_kind_fair_value=in_kind_fv,
            goods_services_fair_value=goods_fv,
            goods_services_description=goods_desc,
        ))

    def _receipt(self, conn, env, donation_id, tax_year="2026"):
        gen = mod.ACTIONS["nonprofit-generate-tax-receipt"]
        return call_action(gen, conn, ns(
            company_id=env["company_id"],
            donor_id=env["donor_id"],
            tax_year=tax_year,
            receipt_type="single",
            donation_id=donation_id,
            sent_method=None,
        ))

    def test_split_receipt_deductible_and_both_statements(self, conn, env):
        created = self._add_donation(conn, env, "500.00", "125.25", "Gala dinner")
        assert is_ok(created), created
        don_row = conn.execute(
            "SELECT amount, goods_services_fair_value, goods_services_description,"
            " in_kind_fair_value FROM nonprofitclaw_donation WHERE id=?",
            (created["id"],)).fetchone()
        assert don_row["amount"] == "500.00"
        assert don_row["goods_services_fair_value"] == "125.25"
        assert don_row["goods_services_description"] == "Gala dinner"
        assert Decimal(don_row["goods_services_fair_value"]) == Decimal("125.25")

        result = self._receipt(conn, env, created["id"])
        assert is_ok(result), result
        assert result["deductible_amount"] == "374.75"
        assert Decimal(result["deductible_amount"]) == Decimal("374.75")
        assert result["goods_services_fair_value"] == "125.25"
        assert result["goods_services_description"] == "Gala dinner"
        assert result["goods_services_provided"] is True
        joined = " ".join(result["statements"]).lower()
        assert "quid pro quo" in joined
        assert "only the excess" in joined
        assert "deductible" in joined
        assert "acknowledgment" in joined

        row = conn.execute(
            "SELECT amount, deductible_amount, goods_services_fair_value,"
            " goods_services_description, statements FROM nonprofitclaw_tax_receipt"
            " WHERE id=?", (result["id"],)).fetchone()
        assert row["deductible_amount"] == "374.75"
        assert Decimal(row["deductible_amount"]) == Decimal("374.75")
        assert row["goods_services_fair_value"] == "125.25"
        assert row["goods_services_description"] == "Gala dinner"
        import json as _json
        stored = _json.loads(row["statements"])
        assert stored == result["statements"]
        assert len(stored) == 2

    def test_acknowledgment_none_provided(self, conn, env):
        created = self._add_donation(conn, env, "250.00")
        assert is_ok(created), created
        result = self._receipt(conn, env, created["id"])
        assert is_ok(result), result
        assert result["deductible_amount"] == "250.00"
        assert result["goods_services_provided"] is False
        assert len(result["statements"]) == 1
        assert "no goods or services were provided" in result["statements"][0].lower()
        row = conn.execute(
            "SELECT deductible_amount, statements FROM nonprofitclaw_tax_receipt WHERE id=?",
            (result["id"],)).fetchone()
        assert row["deductible_amount"] == "250.00"

    def test_75_does_not_cross_quid_threshold(self, conn, env):
        created = self._add_donation(conn, env, "75.00", "10.00", "Coffee mug")
        assert is_ok(created), created
        result = self._receipt(conn, env, created["id"])
        assert is_ok(result), result
        assert result["deductible_amount"] == "65.00"
        assert result["statements"] == []

    def test_negative_fair_value_refuses_without_writes(self, conn, env):
        before = snapshot_tables(conn, ["nonprofitclaw_donation",
                                        "nonprofitclaw_tax_receipt", "audit_log"])
        result = self._add_donation(conn, env, "500.00", "-5.00", "Bad value")
        assert is_error(result)
        assert snapshot_tables(conn, ["nonprofitclaw_donation",
                                      "nonprofitclaw_tax_receipt",
                                      "audit_log"]) == before

    def test_fair_value_above_amount_refuses_without_writes(self, conn, env):
        before = snapshot_tables(conn, ["nonprofitclaw_donation",
                                        "nonprofitclaw_tax_receipt", "audit_log"])
        result = self._add_donation(conn, env, "100.00", "125.25", "Too much")
        assert is_error(result)
        assert snapshot_tables(conn, ["nonprofitclaw_donation",
                                      "nonprofitclaw_tax_receipt",
                                      "audit_log"]) == before

    def test_negative_in_kind_refuses_without_writes(self, conn, env):
        before = snapshot_tables(conn, ["nonprofitclaw_donation",
                                        "nonprofitclaw_tax_receipt", "audit_log"])
        add = mod.ACTIONS["nonprofit-add-donation"]
        result = call_action(add, conn, ns(
            company_id=env["company_id"],
            donor_id=env["donor_id"],
            amount="100.00",
            in_kind_fair_value="-1.00",
            goods_services_fair_value=None,
            goods_services_description=None,
        ))
        assert is_error(result)
        assert snapshot_tables(conn, ["nonprofitclaw_donation",
                                      "nonprofitclaw_tax_receipt",
                                      "audit_log"]) == before

    def test_company_scope_and_duplicate_unchanged(self, conn, env):
        created = self._add_donation(conn, env, "300.00", "20.00", "Books")
        assert is_ok(created), created
        first = self._receipt(conn, env, created["id"])
        assert is_ok(first), first
        assert first["deductible_amount"] == "280.00"
        gen = mod.ACTIONS["nonprofit-generate-tax-receipt"]
        dup = call_action(gen, conn, ns(
            company_id=env["company_id"],
            donor_id=env["donor_id"],
            tax_year="2026",
            receipt_type="single",
            donation_id=created["id"],
            sent_method=None,
        ))
        assert is_error(dup)
        assert "already exists" in dup["message"]
        other_company = seed_company(conn)
        seed_naming_series(conn, other_company)
        scoped = call_action(gen, conn, ns(
            company_id=other_company,
            donor_id=env["donor_id"],
            tax_year="2026",
            receipt_type="single",
            donation_id=created["id"],
            sent_method=None,
        ))
        assert is_error(scoped)


# ─────────────────────────────────────────────────────────────────────────────
# Donor substantiation upgrade (migration 002, floor-o061 attempt 2)
# ─────────────────────────────────────────────────────────────────────────────

class TestDonorSubstantiationMigration002Upgrade:
    """Disposable upgrade proof: prior shape -> migration 002 twice.

    Rewinds a fresh disposable database to the pre-change shape (the six
    split-receipt columns absent), seeds one donation row and one tax receipt
    row, runs migration 002 twice, and proves all six columns appear while
    every value either row held is unchanged.
    """

    _DROP_DONATION_FV = (
        "ALTER TABLE nonprofitclaw_donation DROP COLUMN goods_services_fair_value")
    _DROP_DONATION_DESC = (
        "ALTER TABLE nonprofitclaw_donation DROP COLUMN goods_services_description")
    _DROP_RECEIPT_DED = (
        "ALTER TABLE nonprofitclaw_tax_receipt DROP COLUMN deductible_amount")
    _DROP_RECEIPT_FV = (
        "ALTER TABLE nonprofitclaw_tax_receipt DROP COLUMN goods_services_fair_value")
    _DROP_RECEIPT_DESC = (
        "ALTER TABLE nonprofitclaw_tax_receipt DROP COLUMN goods_services_description")
    _DROP_RECEIPT_STMTS = (
        "ALTER TABLE nonprofitclaw_tax_receipt DROP COLUMN statements")

    DONATION_ADDED = [
        "nonprofitclaw_donation.goods_services_fair_value",
        "nonprofitclaw_donation.goods_services_description",
    ]
    RECEIPT_ADDED = [
        "nonprofitclaw_tax_receipt.deductible_amount",
        "nonprofitclaw_tax_receipt.goods_services_fair_value",
        "nonprofitclaw_tax_receipt.goods_services_description",
        "nonprofitclaw_tax_receipt.statements",
    ]

    def _load_migration(self):
        import importlib.util
        import os
        path = os.path.join(
            os.path.normpath(os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "..", "..", "migrations")),
            "002_donor_substantiation_columns.py")
        spec = importlib.util.spec_from_file_location(
            "nonprofitclaw_migration_002", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def _rewind(self, db_path):
        import nonprofit_helpers  # noqa: F401, binds erpclaw_lib to the tree
        from erpclaw_lib import seam
        from erpclaw_lib.db import get_connection
        for stmt in (
                self._DROP_RECEIPT_STMTS, self._DROP_RECEIPT_DESC,
                self._DROP_RECEIPT_FV, self._DROP_RECEIPT_DED,
                self._DROP_DONATION_DESC, self._DROP_DONATION_FV):
            conn = get_connection(db_path)
            conn.execute(stmt)
            conn.commit()
            conn.close()
        assert "goods_services_fair_value" not in seam.column_names(
            "nonprofitclaw_donation", db_path)
        assert "goods_services_description" not in seam.column_names(
            "nonprofitclaw_donation", db_path)
        for col in ("deductible_amount", "goods_services_fair_value",
                    "goods_services_description", "statements"):
            assert col not in seam.column_names(
                "nonprofitclaw_tax_receipt", db_path)

    def test_prior_shape_upgrades_twice_and_keeps_row_values(self, tmp_path):
        import nonprofit_helpers  # noqa: F401, binds erpclaw_lib to the tree
        from erpclaw_lib import seam
        mig = self._load_migration()
        path = str(tmp_path / "prior.sqlite")
        init_all_tables(path)
        self._rewind(path)

        conn = get_conn(path)
        try:
            company_id = seed_company(conn)
            seed_naming_series(conn, company_id)
            donor = seed_donor(conn, company_id)
            donor_id = donor["donor_id"]
            donation_id = seed_donation(conn, company_id, donor_id, "500.00")
            receipt_id = _uuid()
            conn.execute(
                """INSERT INTO nonprofitclaw_tax_receipt
                   (id, naming_series, donor_id, donation_id, receipt_date,
                    amount, tax_year, receipt_type, company_id)
                   VALUES (?, 'TREC-0001', ?, ?, '2026-02-01', '500.00',
                    '2026', 'single', ?)""",
                (receipt_id, donor_id, donation_id, company_id))
            conn.commit()
            donation_before = dict(conn.execute(
                "SELECT * FROM nonprofitclaw_donation WHERE id = ?",
                (donation_id,)).fetchone())
            receipt_before = dict(conn.execute(
                "SELECT * FROM nonprofitclaw_tax_receipt WHERE id = ?",
                (receipt_id,)).fetchone())
        finally:
            conn.close()

        first = mig.run_migration(path)
        assert sorted(first["added"]) == sorted(
            self.DONATION_ADDED + self.RECEIPT_ADDED)

        donation_cols = seam.column_names("nonprofitclaw_donation", path)
        receipt_cols = seam.column_names("nonprofitclaw_tax_receipt", path)
        assert "goods_services_fair_value" in donation_cols
        assert "goods_services_description" in donation_cols
        for col in ("deductible_amount", "goods_services_fair_value",
                    "goods_services_description", "statements"):
            assert col in receipt_cols

        second = mig.run_migration(path)
        assert second["added"] == []
        assert second["tables"] == 0
        assert second["indexes"] == 0
        assert sorted(second["already_present"]) == sorted(
            self.DONATION_ADDED + self.RECEIPT_ADDED)

        conn = get_conn(path)
        try:
            donation_after = dict(conn.execute(
                "SELECT * FROM nonprofitclaw_donation WHERE id = ?",
                (donation_id,)).fetchone())
            receipt_after = dict(conn.execute(
                "SELECT * FROM nonprofitclaw_tax_receipt WHERE id = ?",
                (receipt_id,)).fetchone())
        finally:
            conn.close()
        for col in ("goods_services_fair_value", "goods_services_description"):
            assert donation_after.pop(col) is None
        assert donation_after == donation_before
        for col in ("deductible_amount", "goods_services_fair_value",
                    "goods_services_description", "statements"):
            assert receipt_after.pop(col) is None
        assert receipt_after == receipt_before

    def test_report_only_writes_nothing(self, tmp_path):
        import nonprofit_helpers  # noqa: F401, binds erpclaw_lib to the tree
        from erpclaw_lib import seam
        mig = self._load_migration()
        path = str(tmp_path / "prior_ro.sqlite")
        init_all_tables(path)
        self._rewind(path)
        result = mig.run_migration(path, report_only=True)
        assert result["report_only"] is True
        assert result["added"] == []
        assert "goods_services_fair_value" not in seam.column_names(
            "nonprofitclaw_donation", path)
        assert "deductible_amount" not in seam.column_names(
            "nonprofitclaw_tax_receipt", path)

    def test_no_op_without_nonprofitclaw(self, tmp_path):
        import importlib.util
        import nonprofit_helpers  # noqa: F401, binds erpclaw_lib to the tree
        from erpclaw_lib import seam
        from nonprofit_helpers import INIT_SCHEMA_PATH
        mig = self._load_migration()
        path = str(tmp_path / "foundation.sqlite")
        spec = importlib.util.spec_from_file_location("init_schema",
                                                      INIT_SCHEMA_PATH)
        foundation = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(foundation)
        foundation.init_db(path)
        result = mig.run_migration(path)
        assert result["tables"] == 0
        assert result["added"] == []
        assert [t for t in seam.table_names(path)
                if t.startswith("nonprofitclaw_")] == []

    def test_it_declares_that_it_changes_no_data(self):
        mig = self._load_migration()
        assert mig.MIGRATION_DATA_CLASS == "none"
