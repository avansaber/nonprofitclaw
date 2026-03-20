"""Tests for NonprofitClaw funds domain.

Actions tested:
  - nonprofit-add-fund
  - nonprofit-update-fund
  - nonprofit-list-funds
  - nonprofit-get-fund
  - nonprofit-add-fund-transfer
  - nonprofit-list-fund-transfers
  - nonprofit-approve-fund-transfer
  - nonprofit-fund-balance-report
"""
import pytest
from decimal import Decimal
from nonprofit_helpers import (
    call_action, ns, is_error, is_ok, load_db_query,
    seed_fund,
)

mod = load_db_query()


# ─────────────────────────────────────────────────────────────────────────────
# Fund CRUD
# ─────────────────────────────────────────────────────────────────────────────

class TestAddFund:
    def test_create_unrestricted_fund(self, conn, env):
        import funds as funds_mod
        result = call_action(funds_mod.add_fund, conn, ns(
            company_id=env["company_id"],
            name="Operating Fund",
            fund_type="unrestricted",
            description="General operating expenses",
            target_amount="50000",
            start_date="2026-01-01",
            end_date="2026-12-31",
        ))
        assert is_ok(result), result
        assert result["name"] == "Operating Fund"
        assert "id" in result
        assert "naming_series" in result

    def test_create_restricted_fund(self, conn, env):
        import funds as funds_mod
        result = call_action(funds_mod.add_fund, conn, ns(
            company_id=env["company_id"],
            name="Building Fund",
            fund_type="temporarily_restricted",
            description="Capital campaign",
            target_amount="100000",
            start_date=None,
            end_date=None,
        ))
        assert is_ok(result), result

    def test_missing_name_fails(self, conn, env):
        import funds as funds_mod
        result = call_action(funds_mod.add_fund, conn, ns(
            company_id=env["company_id"],
            name=None,
            fund_type="unrestricted",
            description=None,
            target_amount=None,
            start_date=None,
            end_date=None,
        ))
        assert is_error(result)

    def test_missing_company_fails(self, conn, env):
        import funds as funds_mod
        result = call_action(funds_mod.add_fund, conn, ns(
            company_id=None,
            name="Test Fund",
            fund_type="unrestricted",
            description=None,
            target_amount=None,
            start_date=None,
            end_date=None,
        ))
        assert is_error(result)


class TestUpdateFund:
    def test_update_fund_name(self, conn, env):
        import funds as funds_mod
        result = call_action(funds_mod.update_fund, conn, ns(
            id=env["fund_id"],
            name="Renamed Fund",
            fund_type=None,
            description=None,
            target_amount=None,
            start_date=None,
            end_date=None,
            is_active=None,
        ))
        assert is_ok(result), result
        assert result["updated"] is True

    def test_update_target_amount(self, conn, env):
        import funds as funds_mod
        result = call_action(funds_mod.update_fund, conn, ns(
            id=env["fund_id"],
            name=None,
            fund_type=None,
            description=None,
            target_amount="75000.00",
            start_date=None,
            end_date=None,
            is_active=None,
        ))
        assert is_ok(result), result

        row = conn.execute(
            "SELECT target_amount FROM nonprofitclaw_fund WHERE id=?",
            (env["fund_id"],)
        ).fetchone()
        assert Decimal(row["target_amount"]) == Decimal("75000.00")

    def test_update_not_found_fails(self, conn, env):
        import funds as funds_mod
        result = call_action(funds_mod.update_fund, conn, ns(
            id="bad-id",
            name="X",
            fund_type=None,
            description=None,
            target_amount=None,
            start_date=None,
            end_date=None,
            is_active=None,
        ))
        assert is_error(result)

    def test_update_no_fields_fails(self, conn, env):
        import funds as funds_mod
        result = call_action(funds_mod.update_fund, conn, ns(
            id=env["fund_id"],
            name=None,
            fund_type=None,
            description=None,
            target_amount=None,
            start_date=None,
            end_date=None,
            is_active=None,
        ))
        assert is_error(result)


class TestListFunds:
    def test_list_all_funds(self, conn, env):
        import funds as funds_mod
        result = call_action(funds_mod.list_funds, conn, ns(
            company_id=env["company_id"],
            fund_type=None, is_active=None, search=None,
            limit="50", offset="0",
        ))
        assert is_ok(result), result
        assert result["total"] >= 1

    def test_list_by_type(self, conn, env):
        import funds as funds_mod
        result = call_action(funds_mod.list_funds, conn, ns(
            company_id=env["company_id"],
            fund_type="unrestricted", is_active=None, search=None,
            limit="50", offset="0",
        ))
        assert is_ok(result), result


class TestGetFund:
    def test_get_existing(self, conn, env):
        import funds as funds_mod
        result = call_action(funds_mod.get_fund, conn, ns(id=env["fund_id"]))
        assert is_ok(result), result
        assert result["fund"]["id"] == env["fund_id"]

    def test_get_nonexistent(self, conn, env):
        import funds as funds_mod
        result = call_action(funds_mod.get_fund, conn, ns(id="bad"))
        assert is_error(result)


# ─────────────────────────────────────────────────────────────────────────────
# Fund Transfers
# ─────────────────────────────────────────────────────────────────────────────

class TestAddFundTransfer:
    def test_create_transfer(self, conn, env):
        import funds as funds_mod
        fund2 = seed_fund(conn, env["company_id"], "Restricted Fund",
                          "temporarily_restricted")
        result = call_action(funds_mod.add_fund_transfer, conn, ns(
            company_id=env["company_id"],
            from_fund_id=env["fund_id"],
            to_fund_id=fund2,
            amount="1000.00",
            transfer_date="2026-03-01",
            reason="Reallocation",
            approved_by=None,
        ))
        assert is_ok(result), result
        assert result["amount"] == "1000.00"
        assert "id" in result

    def test_transfer_same_fund_fails(self, conn, env):
        import funds as funds_mod
        result = call_action(funds_mod.add_fund_transfer, conn, ns(
            company_id=env["company_id"],
            from_fund_id=env["fund_id"],
            to_fund_id=env["fund_id"],
            amount="100",
            transfer_date=None,
            reason=None,
            approved_by=None,
        ))
        assert is_error(result)

    def test_transfer_missing_amount_fails(self, conn, env):
        import funds as funds_mod
        fund2 = seed_fund(conn, env["company_id"], "Fund B")
        result = call_action(funds_mod.add_fund_transfer, conn, ns(
            company_id=env["company_id"],
            from_fund_id=env["fund_id"],
            to_fund_id=fund2,
            amount=None,
            transfer_date=None,
            reason=None,
            approved_by=None,
        ))
        assert is_error(result)


class TestApproveFundTransfer:
    def test_approve_transfer_moves_balance(self, conn, env):
        import funds as funds_mod

        # Give source fund a balance
        conn.execute(
            "UPDATE nonprofitclaw_fund SET current_balance='5000' WHERE id=?",
            (env["fund_id"],)
        )
        conn.commit()

        fund2 = seed_fund(conn, env["company_id"], "Target Fund")

        # Create transfer
        create_result = call_action(funds_mod.add_fund_transfer, conn, ns(
            company_id=env["company_id"],
            from_fund_id=env["fund_id"],
            to_fund_id=fund2,
            amount="2000.00",
            transfer_date="2026-03-01",
            reason="Reallocation",
            approved_by=None,
        ))
        assert is_ok(create_result), create_result
        transfer_id = create_result["id"]

        # Approve
        result = call_action(funds_mod.approve_fund_transfer, conn, ns(
            id=transfer_id,
            approved_by="Admin",
        ))
        assert is_ok(result), result
        assert result["approved"] is True

        # Verify balances
        source = conn.execute(
            "SELECT current_balance FROM nonprofitclaw_fund WHERE id=?",
            (env["fund_id"],)
        ).fetchone()
        target = conn.execute(
            "SELECT current_balance FROM nonprofitclaw_fund WHERE id=?",
            (fund2,)
        ).fetchone()
        assert Decimal(source["current_balance"]) == Decimal("3000.0")
        assert Decimal(target["current_balance"]) == Decimal("2000.0")

    def test_approve_insufficient_balance_fails(self, conn, env):
        import funds as funds_mod
        # Fund starts with 0 balance
        fund2 = seed_fund(conn, env["company_id"], "Target Fund 2")
        create_result = call_action(funds_mod.add_fund_transfer, conn, ns(
            company_id=env["company_id"],
            from_fund_id=env["fund_id"],
            to_fund_id=fund2,
            amount="99999.00",
            transfer_date=None,
            reason=None,
            approved_by=None,
        ))
        assert is_ok(create_result)
        result = call_action(funds_mod.approve_fund_transfer, conn, ns(
            id=create_result["id"],
            approved_by=None,
        ))
        assert is_error(result)


class TestListFundTransfers:
    def test_list_transfers(self, conn, env):
        import funds as funds_mod
        result = call_action(funds_mod.list_fund_transfers, conn, ns(
            company_id=env["company_id"],
            status=None, fund_id=None,
            limit="50", offset="0",
        ))
        assert is_ok(result), result
        assert "fund_transfers" in result


class TestFundBalanceReport:
    def test_balance_report(self, conn, env):
        import funds as funds_mod
        result = call_action(funds_mod.fund_balance_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert "funds" in result
        assert "total_balance" in result
        assert "fund_count" in result
