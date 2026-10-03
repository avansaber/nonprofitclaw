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
    seed_company, seed_fund, seed_naming_series, snapshot_tables,
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
    """Behaviour of nonprofit-list-fund-transfers, read back from the database.

    The transfers below are created through nonprofit-add-fund-transfer, then
    the stored nonprofitclaw_fund_transfer rows are read back and compared
    field-for-field with what the list response returns. Draft transfers move
    no money, so both fund balances must be unchanged afterwards.

    nonprofit-list-fund-transfers posts nothing to the general ledger, so this
    asserts stored rows, never ledger legs; a later reader must not add a
    balanced-legs assertion here.
    """

    def _make_transfer(self, conn, env, to_fund, amount, transfer_date, reason):
        import funds as funds_mod
        result = call_action(funds_mod.add_fund_transfer, conn, ns(
            company_id=env["company_id"],
            from_fund_id=env["fund_id"],
            to_fund_id=to_fund,
            amount=amount,
            transfer_date=transfer_date,
            reason=reason,
            approved_by=None,
        ))
        assert is_ok(result), result
        return result["id"]

    def test_list_transfers(self, conn, env):
        import funds as funds_mod
        target = seed_fund(conn, env["company_id"], "Target Fund",
                           "temporarily_restricted")
        first_id = self._make_transfer(conn, env, target, "1500.10",
                                       "2026-03-01", "Reallocation")
        second_id = self._make_transfer(conn, env, target, "250.25",
                                        "2026-04-15", "Top-up")

        stored = {
            row["id"]: dict(row) for row in conn.execute(
                "SELECT * FROM nonprofitclaw_fund_transfer").fetchall()
        }
        assert stored[first_id]["amount"] == "1500.10"
        assert stored[second_id]["amount"] == "250.25"
        assert stored[first_id]["from_fund_id"] == env["fund_id"]
        assert stored[first_id]["to_fund_id"] == target
        assert stored[first_id]["transfer_date"] == "2026-03-01"
        assert stored[first_id]["reason"] == "Reallocation"
        assert stored[first_id]["status"] == "draft"
        assert stored[second_id]["transfer_date"] == "2026-04-15"
        assert stored[second_id]["status"] == "draft"

        balances_before = {
            row["id"]: row["current_balance"] for row in conn.execute(
                "SELECT id, current_balance FROM nonprofitclaw_fund").fetchall()
        }

        result = call_action(funds_mod.list_fund_transfers, conn, ns(
            company_id=env["company_id"],
            status=None, fund_id=None,
            limit="50", offset="0",
        ))
        assert is_ok(result), result
        assert result["total"] == 2
        listed = {entry["id"]: entry for entry in result["fund_transfers"]}
        assert set(listed) == {first_id, second_id}
        for transfer_id, row in stored.items():
            entry = listed[transfer_id]
            assert entry["amount"] == row["amount"]
            assert Decimal(entry["amount"]) == Decimal(row["amount"])
            assert entry["from_fund_id"] == row["from_fund_id"]
            assert entry["to_fund_id"] == row["to_fund_id"]
            assert entry["transfer_date"] == row["transfer_date"]
            assert entry["reason"] == row["reason"]
            assert entry["status"] == row["status"]
            assert entry["naming_series"] == row["naming_series"]
        assert listed[first_id]["from_fund_name"] == "General Fund"
        assert listed[first_id]["to_fund_name"] == "Target Fund"

        by_fund = call_action(funds_mod.list_fund_transfers, conn, ns(
            company_id=env["company_id"],
            status=None, fund_id=env["fund_id"],
            limit="50", offset="0",
        ))
        assert is_ok(by_fund), by_fund
        assert by_fund["total"] == 2

        drafts = call_action(funds_mod.list_fund_transfers, conn, ns(
            company_id=env["company_id"],
            status="draft", fund_id=None,
            limit="50", offset="0",
        ))
        assert is_ok(drafts), drafts
        assert drafts["total"] == 2
        settled = call_action(funds_mod.list_fund_transfers, conn, ns(
            company_id=env["company_id"],
            status="completed", fund_id=None,
            limit="50", offset="0",
        ))
        assert is_ok(settled), settled
        assert (settled["total"], settled["fund_transfers"]) == (0, [])

        other_company = seed_company(conn)
        seed_naming_series(conn, other_company)
        other_from = seed_fund(conn, other_company, "Other Source")
        other_to = seed_fund(conn, other_company, "Other Target")
        other = call_action(funds_mod.add_fund_transfer, conn, ns(
            company_id=other_company,
            from_fund_id=other_from,
            to_fund_id=other_to,
            amount="99.99",
            transfer_date="2026-05-01",
            reason="Elsewhere",
            approved_by=None,
        ))
        assert is_ok(other), other
        reseeded = call_action(funds_mod.list_fund_transfers, conn, ns(
            company_id=env["company_id"],
            status=None, fund_id=None,
            limit="50", offset="0",
        ))
        assert is_ok(reseeded), reseeded
        assert reseeded["total"] == 2
        assert other["id"] not in {entry["id"]
                                   for entry in reseeded["fund_transfers"]}

        balances_after = {
            row["id"]: row["current_balance"] for row in conn.execute(
                "SELECT id, current_balance FROM nonprofitclaw_fund").fetchall()
        }
        assert balances_after[env["fund_id"]] == balances_before[env["fund_id"]]
        assert balances_after[target] == balances_before[target]

    def test_list_transfers_missing_company_refuses_without_writes(self, conn, env):
        import funds as funds_mod
        before = snapshot_tables(conn, ["nonprofitclaw_fund_transfer",
                                        "audit_log"])
        result = call_action(funds_mod.list_fund_transfers, conn, ns(
            company_id=None,
            status=None, fund_id=None,
            limit="50", offset="0",
        ))
        assert is_error(result)
        assert result["message"] == "--company-id is required"
        assert snapshot_tables(conn, ["nonprofitclaw_fund_transfer",
                                      "audit_log"]) == before


class TestFundBalanceReport:
    """Behaviour of nonprofit-fund-balance-report, read back from the database.

    The report is a read: it must return one entry per active fund of the
    company with the exact stored balance, a total that is the exact sum, and
    must exclude inactive funds and other companies' funds without changing
    any stored row.

    nonprofit-fund-balance-report posts nothing to the general ledger, so this
    asserts stored rows, never ledger legs; a later reader must not add a
    balanced-legs assertion here.
    """

    def test_balance_report(self, conn, env):
        import funds as funds_mod
        operating = seed_fund(conn, env["company_id"], "Operating Reserve",
                              "unrestricted", "1500.10")
        building = seed_fund(conn, env["company_id"], "Building Fund",
                             "temporarily_restricted", "250.25")
        assert is_ok(call_action(funds_mod.update_fund, conn, ns(
            id=building,
            name=None, fund_type=None, description=None,
            target_amount="1001.00",
            start_date=None, end_date=None, is_active=None,
        )))
        dormant = seed_fund(conn, env["company_id"], "Dormant Fund",
                            "unrestricted", "9999.99")
        assert is_ok(call_action(funds_mod.update_fund, conn, ns(
            id=dormant,
            name=None, fund_type=None, description=None, target_amount=None,
            start_date=None, end_date=None, is_active="0",
        )))
        other_company = seed_company(conn)
        seed_naming_series(conn, other_company)
        seed_fund(conn, other_company, "Foreign Fund", "unrestricted",
                  "777.77")

        balances_before = {
            row["id"]: row["current_balance"] for row in conn.execute(
                "SELECT id, current_balance FROM nonprofitclaw_fund").fetchall()
        }

        result = call_action(funds_mod.fund_balance_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert result["fund_count"] == 3
        assert result["total_balance"] == "1750.35"
        assert Decimal(result["total_balance"]) == Decimal("1750.35")

        by_id = {entry["id"]: entry for entry in result["funds"]}
        assert set(by_id) == {env["fund_id"], operating, building}
        assert by_id[operating]["current_balance"] == "1500.10"
        assert by_id[building]["current_balance"] == "250.25"
        assert by_id[env["fund_id"]]["current_balance"] ==             balances_before[env["fund_id"]]
        assert by_id[building]["percent_of_target"] == "25.00"
        names = [entry["name"] for entry in result["funds"]]
        assert names == sorted(names)

        stored_total = sum(
            (Decimal(balance) for fund_id, balance in balances_before.items()
             if fund_id in by_id),
            Decimal("0"),
        )
        assert Decimal(result["total_balance"]) == stored_total

        balances_after = {
            row["id"]: row["current_balance"] for row in conn.execute(
                "SELECT id, current_balance FROM nonprofitclaw_fund").fetchall()
        }
        assert balances_after == balances_before

    def test_balance_report_missing_company_refuses_without_writes(self, conn, env):
        import funds as funds_mod
        before = snapshot_tables(conn, ["nonprofitclaw_fund", "audit_log"])
        result = call_action(funds_mod.fund_balance_report, conn, ns(
            company_id=None,
        ))
        assert is_error(result)
        assert result["message"] == "--company-id is required"
        assert snapshot_tables(conn, ["nonprofitclaw_fund",
                                      "audit_log"]) == before
