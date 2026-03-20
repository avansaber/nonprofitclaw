"""Tests for NonprofitClaw grants and programs domains.

Actions tested:
  Grants:
  - nonprofit-add-grant
  - nonprofit-update-grant
  - nonprofit-list-grants
  - nonprofit-get-grant
  - nonprofit-activate-grant
  - nonprofit-add-grant-expense
  - nonprofit-list-grant-expenses
  - nonprofit-approve-grant-expense
  - nonprofit-close-grant
  - nonprofit-grant-status-report

  Programs:
  - nonprofit-add-program
  - nonprofit-update-program
  - nonprofit-list-programs
  - nonprofit-get-program
  - nonprofit-update-program-outcomes
"""
import pytest
from decimal import Decimal
from nonprofit_helpers import (
    call_action, ns, is_error, is_ok, load_db_query,
    seed_grant, seed_fund, seed_program,
)

mod = load_db_query()


# ─────────────────────────────────────────────────────────────────────────────
# Grant CRUD
# ─────────────────────────────────────────────────────────────────────────────

class TestAddGrant:
    def test_create_basic_grant(self, conn, env):
        import grants as grants_mod
        result = call_action(grants_mod.add_grant, conn, ns(
            company_id=env["company_id"],
            name="Education Initiative",
            grantor_name="Gates Foundation",
            grantor_type="foundation",
            grant_type="project",
            amount="100000",
            fund_id=None,
            start_date="2026-01-01",
            end_date="2026-12-31",
            reporting_freq="quarterly",
            notes="Multi-year grant",
        ))
        assert is_ok(result), result
        assert result["name"] == "Education Initiative"
        assert result["amount"] == "100000.00"
        assert "id" in result

    def test_create_grant_with_fund(self, conn, env):
        import grants as grants_mod
        result = call_action(grants_mod.add_grant, conn, ns(
            company_id=env["company_id"],
            name="Capital Grant",
            grantor_name="Local Gov",
            grantor_type="government",
            grant_type="capital",
            amount="25000",
            fund_id=env["fund_id"],
            start_date=None,
            end_date=None,
            reporting_freq="annual",
            notes=None,
        ))
        assert is_ok(result), result

    def test_missing_grantor_fails(self, conn, env):
        import grants as grants_mod
        result = call_action(grants_mod.add_grant, conn, ns(
            company_id=env["company_id"],
            name="Test Grant",
            grantor_name=None,
            grantor_type=None,
            grant_type=None,
            amount="5000",
            fund_id=None,
            start_date=None,
            end_date=None,
            reporting_freq=None,
            notes=None,
        ))
        assert is_error(result)

    def test_missing_amount_fails(self, conn, env):
        import grants as grants_mod
        result = call_action(grants_mod.add_grant, conn, ns(
            company_id=env["company_id"],
            name="Test Grant",
            grantor_name="Some Foundation",
            grantor_type=None,
            grant_type=None,
            amount=None,
            fund_id=None,
            start_date=None,
            end_date=None,
            reporting_freq=None,
            notes=None,
        ))
        assert is_error(result)


class TestUpdateGrant:
    def test_update_grant_name(self, conn, env):
        import grants as grants_mod
        result = call_action(grants_mod.update_grant, conn, ns(
            id=env["grant_id"],
            name="Renamed Grant",
            grantor_name=None,
            grantor_type=None,
            grant_type=None,
            reporting_freq=None,
            start_date=None,
            end_date=None,
            notes=None,
            fund_id=None,
        ))
        assert is_ok(result), result
        assert result["updated"] is True

    def test_update_closed_grant_fails(self, conn, env):
        import grants as grants_mod
        # Create a closed grant
        gid = seed_grant(conn, env["company_id"], "Closed Grant", status="closed")
        result = call_action(grants_mod.update_grant, conn, ns(
            id=gid,
            name="Try Update",
            grantor_name=None,
            grantor_type=None,
            grant_type=None,
            reporting_freq=None,
            start_date=None,
            end_date=None,
            notes=None,
            fund_id=None,
        ))
        assert is_error(result)


class TestListGrants:
    def test_list_all_grants(self, conn, env):
        import grants as grants_mod
        result = call_action(grants_mod.list_grants, conn, ns(
            company_id=env["company_id"],
            status=None, grantor_type=None, grant_type=None, search=None,
            limit="50", offset="0",
        ))
        assert is_ok(result), result
        assert result["total"] >= 1


class TestGetGrant:
    def test_get_existing(self, conn, env):
        import grants as grants_mod
        result = call_action(grants_mod.get_grant, conn, ns(id=env["grant_id"]))
        assert is_ok(result), result
        assert result["grant"]["id"] == env["grant_id"]
        assert "expense_summary" in result["grant"]

    def test_get_nonexistent(self, conn, env):
        import grants as grants_mod
        result = call_action(grants_mod.get_grant, conn, ns(id="bad-id"))
        assert is_error(result)


class TestActivateGrant:
    def test_activate_applied_grant(self, conn, env):
        import grants as grants_mod
        result = call_action(grants_mod.activate_grant, conn, ns(
            id=env["grant_id"],
            amount=None,
        ))
        assert is_ok(result), result
        assert result["grant_status"] == "active"
        assert result["received_amount"] == "50000.00"

    def test_activate_with_partial_amount(self, conn, env):
        import grants as grants_mod
        gid = seed_grant(conn, env["company_id"], "Partial Grant",
                         amount="10000", status="awarded")
        result = call_action(grants_mod.activate_grant, conn, ns(
            id=gid,
            amount="7500",
        ))
        assert is_ok(result), result
        assert result["received_amount"] == "7500.00"

    def test_activate_already_active_fails(self, conn, env):
        import grants as grants_mod
        gid = seed_grant(conn, env["company_id"], "Active Grant", status="active")
        result = call_action(grants_mod.activate_grant, conn, ns(
            id=gid, amount=None,
        ))
        assert is_error(result)


# ─────────────────────────────────────────────────────────────────────────────
# Grant Expenses
# ─────────────────────────────────────────────────────────────────────────────

class TestAddGrantExpense:
    def test_add_expense_to_active_grant(self, conn, env):
        import grants as grants_mod
        gid = seed_grant(conn, env["company_id"], "Active Grant",
                         amount="20000", status="active")
        result = call_action(grants_mod.add_grant_expense, conn, ns(
            company_id=env["company_id"],
            grant_id=gid,
            amount="1500.00",
            category="program",
            description="Training materials",
            expense_date="2026-03-01",
            receipt_reference="REC-001",
        ))
        assert is_ok(result), result
        assert result["amount"] == "1500.00"

    def test_add_expense_to_non_active_fails(self, conn, env):
        import grants as grants_mod
        result = call_action(grants_mod.add_grant_expense, conn, ns(
            company_id=env["company_id"],
            grant_id=env["grant_id"],  # status=applied
            amount="100",
            category="program",
            description=None,
            expense_date=None,
            receipt_reference=None,
        ))
        assert is_error(result)


class TestApproveGrantExpense:
    def test_approve_expense_updates_grant(self, conn, env):
        import grants as grants_mod
        gid = seed_grant(conn, env["company_id"], "Active Grant 2",
                         amount="10000", status="active")

        # Add expense
        add_result = call_action(grants_mod.add_grant_expense, conn, ns(
            company_id=env["company_id"],
            grant_id=gid,
            amount="2000.00",
            category="personnel",
            description="Staff salary",
            expense_date="2026-03-01",
            receipt_reference=None,
        ))
        assert is_ok(add_result), add_result
        expense_id = add_result["id"]

        # Approve it
        result = call_action(grants_mod.approve_grant_expense, conn, ns(
            id=expense_id,
            expense_account_id=None,
            cash_account_id=None,
            cost_center_id=None,
        ))
        assert is_ok(result), result
        assert result["approved"] is True
        assert result["grant_spent"] == "2000.00"
        assert result["grant_remaining"] == "8000.00"

    def test_approve_exceeds_remaining_fails(self, conn, env):
        import grants as grants_mod
        gid = seed_grant(conn, env["company_id"], "Small Grant",
                         amount="500", status="active")

        add_result = call_action(grants_mod.add_grant_expense, conn, ns(
            company_id=env["company_id"],
            grant_id=gid,
            amount="999.00",
            category="supplies",
            description=None,
            expense_date=None,
            receipt_reference=None,
        ))
        assert is_ok(add_result)

        result = call_action(grants_mod.approve_grant_expense, conn, ns(
            id=add_result["id"],
            expense_account_id=None,
            cash_account_id=None,
            cost_center_id=None,
        ))
        assert is_error(result)


class TestListGrantExpenses:
    def test_list_expenses(self, conn, env):
        import grants as grants_mod
        result = call_action(grants_mod.list_grant_expenses, conn, ns(
            company_id=env["company_id"],
            grant_id=None, status=None, category=None,
            from_date=None, to_date=None,
            limit="50", offset="0",
        ))
        assert is_ok(result), result
        assert "grant_expenses" in result


class TestCloseGrant:
    def test_close_active_grant(self, conn, env):
        import grants as grants_mod
        gid = seed_grant(conn, env["company_id"], "To Close",
                         amount="5000", status="active")
        result = call_action(grants_mod.close_grant, conn, ns(id=gid))
        assert is_ok(result), result
        assert result["grant_status"] == "completed"

    def test_close_already_closed_fails(self, conn, env):
        import grants as grants_mod
        gid = seed_grant(conn, env["company_id"], "Already Closed", status="closed")
        result = call_action(grants_mod.close_grant, conn, ns(id=gid))
        assert is_error(result)

    def test_close_with_pending_expenses_fails(self, conn, env):
        import grants as grants_mod
        gid = seed_grant(conn, env["company_id"], "Has Pending",
                         amount="10000", status="active")
        # Add a draft expense
        call_action(grants_mod.add_grant_expense, conn, ns(
            company_id=env["company_id"],
            grant_id=gid,
            amount="100",
            category="program",
            description=None,
            expense_date=None,
            receipt_reference=None,
        ))
        result = call_action(grants_mod.close_grant, conn, ns(id=gid))
        assert is_error(result)


class TestGrantStatusReport:
    def test_status_report(self, conn, env):
        import grants as grants_mod
        # Activate the env grant so it shows in report
        seed_grant(conn, env["company_id"], "Active Report Grant",
                   amount="30000", status="active")
        result = call_action(grants_mod.grant_status_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert "grants" in result
        assert "total_awarded" in result
        assert "active_grant_count" in result
        assert result["active_grant_count"] >= 1


# ─────────────────────────────────────────────────────────────────────────────
# Programs
# ─────────────────────────────────────────────────────────────────────────────

class TestAddProgram:
    def test_create_basic_program(self, conn, env):
        import programs as programs_mod
        result = call_action(programs_mod.add_program, conn, ns(
            company_id=env["company_id"],
            name="After School",
            description="After school tutoring",
            fund_id=None,
            budget="8000",
            beneficiary_count="50",
            start_date="2026-01-01",
            end_date="2026-06-30",
            outcome_metrics=None,
        ))
        assert is_ok(result), result
        assert result["name"] == "After School"

    def test_create_with_fund(self, conn, env):
        import programs as programs_mod
        result = call_action(programs_mod.add_program, conn, ns(
            company_id=env["company_id"],
            name="Summer Camp",
            description=None,
            fund_id=env["fund_id"],
            budget="15000",
            beneficiary_count=None,
            start_date=None,
            end_date=None,
            outcome_metrics=None,
        ))
        assert is_ok(result), result

    def test_missing_name_fails(self, conn, env):
        import programs as programs_mod
        result = call_action(programs_mod.add_program, conn, ns(
            company_id=env["company_id"],
            name=None,
            description=None,
            fund_id=None,
            budget=None,
            beneficiary_count=None,
            start_date=None,
            end_date=None,
            outcome_metrics=None,
        ))
        assert is_error(result)


class TestUpdateProgram:
    def test_update_budget(self, conn, env):
        import programs as programs_mod
        result = call_action(programs_mod.update_program, conn, ns(
            id=env["program_id"],
            name=None,
            description=None,
            start_date=None,
            end_date=None,
            outcome_metrics=None,
            budget="12000",
            fund_id=None,
            is_active=None,
            beneficiary_count=None,
        ))
        assert is_ok(result), result
        assert result["updated"] is True

    def test_update_not_found_fails(self, conn, env):
        import programs as programs_mod
        result = call_action(programs_mod.update_program, conn, ns(
            id="bad-id",
            name="X",
            description=None,
            start_date=None,
            end_date=None,
            outcome_metrics=None,
            budget=None,
            fund_id=None,
            is_active=None,
            beneficiary_count=None,
        ))
        assert is_error(result)


class TestListPrograms:
    def test_list_all(self, conn, env):
        import programs as programs_mod
        result = call_action(programs_mod.list_programs, conn, ns(
            company_id=env["company_id"],
            is_active=None, search=None, fund_id=None,
            limit="50", offset="0",
        ))
        assert is_ok(result), result
        assert result["total"] >= 1


class TestGetProgram:
    def test_get_existing(self, conn, env):
        import programs as programs_mod
        result = call_action(programs_mod.get_program, conn, ns(
            id=env["program_id"],
        ))
        assert is_ok(result), result
        assert result["program"]["id"] == env["program_id"]
        assert "volunteer_shifts" in result["program"]
        assert "volunteer_hours" in result["program"]


class TestUpdateProgramOutcomes:
    def test_update_outcomes(self, conn, env):
        import programs as programs_mod
        result = call_action(programs_mod.update_program_outcomes, conn, ns(
            id=env["program_id"],
            beneficiary_count="150",
            outcome_metrics='{"students_served": 150, "graduation_rate": "85%"}',
        ))
        assert is_ok(result), result
        assert result["outcomes_updated"] is True

    def test_no_fields_fails(self, conn, env):
        import programs as programs_mod
        result = call_action(programs_mod.update_program_outcomes, conn, ns(
            id=env["program_id"],
            beneficiary_count=None,
            outcome_metrics=None,
        ))
        assert is_error(result)
