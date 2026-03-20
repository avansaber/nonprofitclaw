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
    seed_volunteer, seed_program, seed_campaign, seed_donor, seed_donation,
    seed_fund, _uuid,
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
    def test_list_shifts(self, conn, env):
        import volunteers as vol_mod
        result = call_action(vol_mod.list_volunteer_shifts, conn, ns(
            company_id=env["company_id"],
            volunteer_id=None, program_id=None, status=None,
            from_date=None, to_date=None,
            limit="50", offset="0",
        ))
        assert is_ok(result), result
        assert "volunteer_shifts" in result


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
    def test_list_pledges(self, conn, env):
        import campaigns as camp_mod
        result = call_action(camp_mod.list_pledges, conn, ns(
            company_id=env["company_id"],
            donor_id=None, campaign_id=None, status=None,
            limit="50", offset="0",
        ))
        assert is_ok(result), result
        assert "pledges" in result


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
    def test_list_receipts(self, conn, env):
        import compliance as comp_mod
        result = call_action(comp_mod.list_tax_receipts, conn, ns(
            company_id=env["company_id"],
            donor_id=None, tax_year=None, receipt_type=None,
            limit="50", offset="0",
        ))
        assert is_ok(result), result
        assert "tax_receipts" in result


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
