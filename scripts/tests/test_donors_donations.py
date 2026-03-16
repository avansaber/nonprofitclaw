"""Tests for NonprofitClaw donors & donations domain.

Actions tested:
  - nonprofit-add-donor (via mock of cross_skill.create_customer)
  - nonprofit-update-donor
  - nonprofit-list-donors
  - nonprofit-get-donor
  - nonprofit-donor-giving-history
  - nonprofit-merge-donors
  - nonprofit-add-donation
  - nonprofit-update-donation
  - nonprofit-list-donations
  - nonprofit-get-donation
  - nonprofit-refund-donation
"""
import pytest
from decimal import Decimal
from unittest.mock import patch, MagicMock
from nonprofit_helpers import (
    call_action, ns, is_error, is_ok, load_db_query,
    seed_donor, seed_fund, seed_campaign, seed_donation, seed_customer, _uuid,
)

mod = load_db_query()


# ─────────────────────────────────────────────────────────────────────────────
# Donor CRUD (add-donor requires mocking cross_skill.create_customer)
# ─────────────────────────────────────────────────────────────────────────────

class TestAddDonor:
    def _mock_create_customer(self, conn, company_id, name="Mock Customer"):
        """Create a real customer row and return a mock function that returns its ID."""
        customer_id = seed_customer(conn, company_id, name)
        return customer_id, lambda **kw: {"customer_id": customer_id}

    def test_create_individual_donor(self, conn, env):
        customer_id, mock_fn = self._mock_create_customer(
            conn, env["company_id"], "John Doe"
        )
        import donors as donors_mod
        original = donors_mod.create_customer
        try:
            donors_mod.create_customer = mock_fn
            result = call_action(donors_mod.add_donor, conn, ns(
                company_id=env["company_id"],
                name="John Doe",
                donor_type="individual",
                donor_level="gold",
                email="john@example.com",
                phone="555-1234",
                notes="Test donor",
            ))
        finally:
            donors_mod.create_customer = original

        assert is_ok(result), result
        assert "id" in result
        assert result["name"] == "John Doe"
        assert "naming_series" in result
        assert result["customer_id"] == customer_id

    def test_missing_company_fails(self, conn, env):
        import donors as donors_mod
        original = donors_mod.create_customer
        try:
            donors_mod.create_customer = lambda **kw: {"customer_id": _uuid()}
            result = call_action(donors_mod.add_donor, conn, ns(
                company_id=None,
                name="Test",
                donor_type=None,
                donor_level=None,
                email=None,
                phone=None,
                notes=None,
            ))
        finally:
            donors_mod.create_customer = original
        assert is_error(result)

    def test_missing_name_fails(self, conn, env):
        import donors as donors_mod
        original = donors_mod.create_customer
        try:
            donors_mod.create_customer = lambda **kw: {"customer_id": _uuid()}
            result = call_action(donors_mod.add_donor, conn, ns(
                company_id=env["company_id"],
                name=None,
                donor_type=None,
                donor_level=None,
                email=None,
                phone=None,
                notes=None,
            ))
        finally:
            donors_mod.create_customer = original
        assert is_error(result)


class TestUpdateDonor:
    def test_update_donor_level(self, conn, env):
        import donors as donors_mod
        result = call_action(donors_mod.update_donor, conn, ns(
            id=env["donor_id"],
            name=None,
            email=None,
            phone=None,
            address=None,
            tax_id=None,
            donor_type=None,
            donor_level="platinum",
            notes=None,
            is_active=None,
        ))
        assert is_ok(result), result
        assert result["updated"] is True

        # Verify updated
        row = conn.execute(
            "SELECT donor_level FROM nonprofitclaw_donor_ext WHERE id=?",
            (env["donor_id"],)
        ).fetchone()
        assert row["donor_level"] == "platinum"

    def test_update_donor_not_found(self, conn, env):
        import donors as donors_mod
        result = call_action(donors_mod.update_donor, conn, ns(
            id="nonexistent-id",
            name=None, email=None, phone=None, address=None, tax_id=None,
            donor_type=None, donor_level="gold", notes=None, is_active=None,
        ))
        assert is_error(result)

    def test_update_no_fields_fails(self, conn, env):
        import donors as donors_mod
        result = call_action(donors_mod.update_donor, conn, ns(
            id=env["donor_id"],
            name=None, email=None, phone=None, address=None, tax_id=None,
            donor_type=None, donor_level=None, notes=None, is_active=None,
        ))
        assert is_error(result)


class TestListDonors:
    def test_list_all_donors(self, conn, env):
        import donors as donors_mod
        result = call_action(donors_mod.list_donors, conn, ns(
            company_id=env["company_id"],
            search=None, donor_type=None, donor_level=None,
            is_active=None, limit="50", offset="0",
        ))
        assert is_ok(result), result
        assert result["total"] >= 1
        assert len(result["donors"]) >= 1

    def test_list_by_type_filter(self, conn, env):
        import donors as donors_mod
        result = call_action(donors_mod.list_donors, conn, ns(
            company_id=env["company_id"],
            search=None, donor_type="individual", donor_level=None,
            is_active=None, limit="50", offset="0",
        ))
        assert is_ok(result), result

    def test_list_missing_company_fails(self, conn, env):
        import donors as donors_mod
        result = call_action(donors_mod.list_donors, conn, ns(
            company_id=None,
            search=None, donor_type=None, donor_level=None,
            is_active=None, limit="50", offset="0",
        ))
        assert is_error(result)


class TestGetDonor:
    def test_get_existing_donor(self, conn, env):
        import donors as donors_mod
        result = call_action(donors_mod.get_donor, conn, ns(id=env["donor_id"]))
        assert is_ok(result), result
        assert result["donor"]["id"] == env["donor_id"]
        assert "name" in result["donor"]

    def test_get_nonexistent_donor(self, conn, env):
        import donors as donors_mod
        result = call_action(donors_mod.get_donor, conn, ns(id="bad-id"))
        assert is_error(result)


# ─────────────────────────────────────────────────────────────────────────────
# Donations
# ─────────────────────────────────────────────────────────────────────────────

class TestAddDonation:
    def test_create_basic_donation(self, conn, env):
        import donors as donors_mod
        result = call_action(donors_mod.add_donation, conn, ns(
            company_id=env["company_id"],
            donor_id=env["donor_id"],
            amount="250.00",
            payment_method="check",
            donation_date="2026-03-01",
            reference="CHK-1001",
            fund_id=None,
            campaign_id=None,
            is_recurring=None,
            recurrence_freq=None,
            notes="Annual gift",
            cash_account_id=None,
            revenue_account_id=None,
            cost_center_id=None,
        ))
        assert is_ok(result), result
        assert result["amount"] == "250.00"
        assert "id" in result
        assert "naming_series" in result

    def test_donation_to_fund(self, conn, env):
        import donors as donors_mod
        result = call_action(donors_mod.add_donation, conn, ns(
            company_id=env["company_id"],
            donor_id=env["donor_id"],
            amount="500.00",
            payment_method="credit_card",
            donation_date="2026-03-01",
            reference=None,
            fund_id=env["fund_id"],
            campaign_id=None,
            is_recurring=None,
            recurrence_freq=None,
            notes=None,
            cash_account_id=None,
            revenue_account_id=None,
            cost_center_id=None,
        ))
        assert is_ok(result), result

        # Verify fund balance was updated
        fund = conn.execute(
            "SELECT current_balance FROM nonprofitclaw_fund WHERE id=?",
            (env["fund_id"],)
        ).fetchone()
        assert Decimal(fund["current_balance"]) > Decimal("0")

    def test_donation_to_campaign(self, conn, env):
        import donors as donors_mod
        result = call_action(donors_mod.add_donation, conn, ns(
            company_id=env["company_id"],
            donor_id=env["donor_id"],
            amount="100.00",
            payment_method="online",
            donation_date="2026-03-01",
            reference=None,
            fund_id=None,
            campaign_id=env["campaign_id"],
            is_recurring=None,
            recurrence_freq=None,
            notes=None,
            cash_account_id=None,
            revenue_account_id=None,
            cost_center_id=None,
        ))
        assert is_ok(result), result

        # Verify campaign raised_amount updated
        camp = conn.execute(
            "SELECT raised_amount, donor_count FROM nonprofitclaw_campaign WHERE id=?",
            (env["campaign_id"],)
        ).fetchone()
        assert Decimal(camp["raised_amount"]) >= Decimal("100")

    def test_donation_missing_amount_fails(self, conn, env):
        import donors as donors_mod
        result = call_action(donors_mod.add_donation, conn, ns(
            company_id=env["company_id"],
            donor_id=env["donor_id"],
            amount=None,
            payment_method="check",
            donation_date=None,
            reference=None,
            fund_id=None,
            campaign_id=None,
            is_recurring=None,
            recurrence_freq=None,
            notes=None,
            cash_account_id=None,
            revenue_account_id=None,
            cost_center_id=None,
        ))
        assert is_error(result)

    def test_donation_zero_amount_fails(self, conn, env):
        import donors as donors_mod
        result = call_action(donors_mod.add_donation, conn, ns(
            company_id=env["company_id"],
            donor_id=env["donor_id"],
            amount="0",
            payment_method="check",
            donation_date=None,
            reference=None,
            fund_id=None,
            campaign_id=None,
            is_recurring=None,
            recurrence_freq=None,
            notes=None,
            cash_account_id=None,
            revenue_account_id=None,
            cost_center_id=None,
        ))
        assert is_error(result)

    def test_donation_bad_donor_fails(self, conn, env):
        import donors as donors_mod
        result = call_action(donors_mod.add_donation, conn, ns(
            company_id=env["company_id"],
            donor_id="nonexistent",
            amount="100",
            payment_method="check",
            donation_date=None,
            reference=None,
            fund_id=None,
            campaign_id=None,
            is_recurring=None,
            recurrence_freq=None,
            notes=None,
            cash_account_id=None,
            revenue_account_id=None,
            cost_center_id=None,
        ))
        assert is_error(result)

    def test_donation_updates_donor_stats(self, conn, env):
        import donors as donors_mod
        call_action(donors_mod.add_donation, conn, ns(
            company_id=env["company_id"],
            donor_id=env["donor_id"],
            amount="300.00",
            payment_method="bank_transfer",
            donation_date="2026-02-15",
            reference=None,
            fund_id=None,
            campaign_id=None,
            is_recurring=None,
            recurrence_freq=None,
            notes=None,
            cash_account_id=None,
            revenue_account_id=None,
            cost_center_id=None,
        ))
        # Check donor ext stats
        donor = conn.execute(
            "SELECT total_donated, donation_count FROM nonprofitclaw_donor_ext WHERE id=?",
            (env["donor_id"],)
        ).fetchone()
        assert int(donor["donation_count"]) >= 1
        assert Decimal(donor["total_donated"]) >= Decimal("300")


class TestUpdateDonation:
    def test_update_payment_method(self, conn, env):
        import donors as donors_mod
        donation_id = seed_donation(conn, env["company_id"], env["donor_id"], "100.00")
        result = call_action(donors_mod.update_donation, conn, ns(
            id=donation_id,
            payment_method="credit_card",
            reference="CC-999",
            notes=None,
            donation_date=None,
            is_recurring=None,
            recurrence_freq=None,
        ))
        assert is_ok(result), result
        assert result["updated"] is True

    def test_update_no_fields_fails(self, conn, env):
        import donors as donors_mod
        donation_id = seed_donation(conn, env["company_id"], env["donor_id"])
        result = call_action(donors_mod.update_donation, conn, ns(
            id=donation_id,
            payment_method=None,
            reference=None,
            notes=None,
            donation_date=None,
            is_recurring=None,
            recurrence_freq=None,
        ))
        assert is_error(result)


class TestListDonations:
    def test_list_company_donations(self, conn, env):
        import donors as donors_mod
        seed_donation(conn, env["company_id"], env["donor_id"], "100.00")
        seed_donation(conn, env["company_id"], env["donor_id"], "200.00")
        result = call_action(donors_mod.list_donations, conn, ns(
            company_id=env["company_id"],
            donor_id=None, fund_id=None, campaign_id=None,
            status=None, from_date=None, to_date=None,
            limit="50", offset="0",
        ))
        assert is_ok(result), result
        assert result["total"] >= 2

    def test_list_by_donor(self, conn, env):
        import donors as donors_mod
        seed_donation(conn, env["company_id"], env["donor_id"], "150.00")
        result = call_action(donors_mod.list_donations, conn, ns(
            company_id=env["company_id"],
            donor_id=env["donor_id"], fund_id=None, campaign_id=None,
            status=None, from_date=None, to_date=None,
            limit="50", offset="0",
        ))
        assert is_ok(result), result
        assert result["total"] >= 1


class TestGetDonation:
    def test_get_existing_donation(self, conn, env):
        import donors as donors_mod
        donation_id = seed_donation(conn, env["company_id"], env["donor_id"], "100.00")
        result = call_action(donors_mod.get_donation, conn, ns(id=donation_id))
        assert is_ok(result), result
        assert result["donation"]["id"] == donation_id
        assert result["donation"]["amount"] == "100.00"

    def test_get_nonexistent_donation(self, conn, env):
        import donors as donors_mod
        result = call_action(donors_mod.get_donation, conn, ns(id="bad-id"))
        assert is_error(result)


class TestRefundDonation:
    def test_refund_received_donation(self, conn, env):
        import donors as donors_mod
        donation_id = seed_donation(conn, env["company_id"], env["donor_id"], "200.00")
        result = call_action(donors_mod.refund_donation, conn, ns(
            id=donation_id, donation_id=None,
        ))
        assert is_ok(result), result
        assert result["refunded"] is True
        assert result["amount"] == "200.00"

        # Verify status
        row = conn.execute("SELECT status FROM nonprofitclaw_donation WHERE id=?",
                           (donation_id,)).fetchone()
        assert row["status"] == "refunded"

    def test_refund_already_refunded_fails(self, conn, env):
        import donors as donors_mod
        donation_id = seed_donation(conn, env["company_id"], env["donor_id"],
                                    "100.00", status="refunded")
        result = call_action(donors_mod.refund_donation, conn, ns(
            id=donation_id, donation_id=None,
        ))
        assert is_error(result)

    def test_refund_updates_donor_stats(self, conn, env):
        import donors as donors_mod
        # First add a donation via the action to set stats
        seed_donation(conn, env["company_id"], env["donor_id"], "500.00")
        donation_id = seed_donation(conn, env["company_id"], env["donor_id"], "300.00")

        call_action(donors_mod.refund_donation, conn, ns(
            id=donation_id, donation_id=None,
        ))

        donor = conn.execute(
            "SELECT total_donated, donation_count FROM nonprofitclaw_donor_ext WHERE id=?",
            (env["donor_id"],)
        ).fetchone()
        # After refund, stats should reflect only the non-refunded donations
        assert int(donor["donation_count"]) >= 1


class TestDonorGivingHistory:
    def test_giving_history_with_donations(self, conn, env):
        import donors as donors_mod
        seed_donation(conn, env["company_id"], env["donor_id"], "100.00")
        seed_donation(conn, env["company_id"], env["donor_id"], "200.00")
        result = call_action(donors_mod.donor_giving_history, conn, ns(
            donor_id=env["donor_id"],
        ))
        assert is_ok(result), result
        assert result["total"] >= 2
        assert len(result["donations"]) >= 2

    def test_giving_history_not_found(self, conn, env):
        import donors as donors_mod
        result = call_action(donors_mod.donor_giving_history, conn, ns(
            donor_id="bad-id",
        ))
        assert is_error(result)


class TestMergeDonors:
    def test_merge_two_donors(self, conn, env):
        import donors as donors_mod
        donor2 = seed_donor(conn, env["company_id"], "Second Donor")
        d2_id = donor2["donor_id"]

        # Give each a donation
        seed_donation(conn, env["company_id"], env["donor_id"], "100.00")
        seed_donation(conn, env["company_id"], d2_id, "200.00")

        result = call_action(donors_mod.merge_donors, conn, ns(
            source_donor_id=d2_id,
            target_donor_id=env["donor_id"],
        ))
        assert is_ok(result), result
        assert result["merged"] is True
        assert result["new_donation_count"] >= 2

        # Source donor should be deleted
        row = conn.execute(
            "SELECT id FROM nonprofitclaw_donor_ext WHERE id=?", (d2_id,)
        ).fetchone()
        assert row is None

    def test_merge_same_donor_fails(self, conn, env):
        import donors as donors_mod
        result = call_action(donors_mod.merge_donors, conn, ns(
            source_donor_id=env["donor_id"],
            target_donor_id=env["donor_id"],
        ))
        assert is_error(result)
