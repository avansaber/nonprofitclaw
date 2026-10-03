"""Approval of a grant expense requires its ledger posting.

Approving a grant expense spends grant money, so the ledger posting must go
with it. Without the posting accounts (or the posting library) approval is
refused before any write, naming what is missing.
"""
import json
from decimal import Decimal

from nonprofit_helpers import (
    call_action, is_error, is_ok, load_db_query, ns,
    seed_account, seed_company,
)

load_db_query()  # puts the module's scripts directory on sys.path


def _setup(conn, env):
    import funds as funds_mod
    import grants as grants_mod
    fund = call_action(funds_mod.add_fund, conn, ns(
        company_id=env["company_id"], name="Ledger Grant Fund",
        fund_type="unrestricted", description=None, target_amount=None,
        start_date=None, end_date=None))
    assert is_ok(fund), fund
    fund_id = fund["id"]
    grant = call_action(grants_mod.add_grant, conn, ns(
        company_id=env["company_id"], name="Ledger Grant",
        grantor_name="Ledger Foundation", grantor_type="foundation",
        grant_type="project", amount="50000.00", fund_id=fund_id,
        start_date="2026-01-01", end_date="2026-12-31",
        reporting_freq="quarterly", notes=None))
    assert is_ok(grant), grant
    grant_id = grant["id"]
    activated = call_action(grants_mod.activate_grant, conn, ns(
        id=grant_id, amount="50000.00"))
    assert is_ok(activated), activated
    expense = call_action(grants_mod.add_grant_expense, conn, ns(
        company_id=env["company_id"], grant_id=grant_id, amount="40000.00",
        category="program", description="Program spend",
        expense_date="2026-03-05", receipt_reference=None))
    assert is_ok(expense), expense
    return fund_id, grant_id, expense["id"]


def _snapshot(conn, expense_id, grant_id, fund_id):
    expense_row = dict(conn.execute(
        "SELECT * FROM nonprofitclaw_grant_expense WHERE id = ?",
        (expense_id,)).fetchone())
    grant_row = conn.execute(
        "SELECT spent_amount, remaining_amount FROM nonprofitclaw_grant "
        "WHERE id = ?", (grant_id,)).fetchone()
    fund_row = conn.execute(
        "SELECT current_balance FROM nonprofitclaw_fund WHERE id = ?",
        (fund_id,)).fetchone()
    gl_count = conn.execute("SELECT COUNT(*) FROM gl_entry").fetchone()[0]
    audit_count = conn.execute("SELECT COUNT(*) FROM audit_log").fetchone()[0]
    return {
        "expense_row": {k: ("" if v is None else str(v)) for k, v in expense_row.items()},
        "spent": grant_row["spent_amount"],
        "remaining": grant_row["remaining_amount"],
        "balance": fund_row["current_balance"],
        "gl_count": gl_count,
        "audit_count": audit_count,
    }


def _assert_unchanged(conn, before, expense_id, grant_id, fund_id):
    assert _snapshot(conn, expense_id, grant_id, fund_id) == before


def test_no_accounts_refused(conn, env):
    import grants as grants_mod
    fund_id, grant_id, expense_id = _setup(conn, env)
    before = _snapshot(conn, expense_id, grant_id, fund_id)
    result = call_action(grants_mod.approve_grant_expense, conn, ns(
        id=expense_id, expense_account_id=None, cash_account_id=None,
        cost_center_id=None))
    assert is_error(result), result
    assert result["message"] == (
        f"Approving grant expense {expense_id} posts it to the ledger; "
        "missing: --expense-account-id, --cash-account-id"), result
    _assert_unchanged(conn, before, expense_id, grant_id, fund_id)


def test_expense_account_only_refused(conn, env):
    import grants as grants_mod
    fund_id, grant_id, expense_id = _setup(conn, env)
    before = _snapshot(conn, expense_id, grant_id, fund_id)
    result = call_action(grants_mod.approve_grant_expense, conn, ns(
        id=expense_id, expense_account_id=env["expense_acct"],
        cash_account_id=None, cost_center_id=env["cc_id"]))
    assert is_error(result), result
    assert result["message"] == (
        f"Approving grant expense {expense_id} posts it to the ledger; "
        "missing: --cash-account-id"), result
    _assert_unchanged(conn, before, expense_id, grant_id, fund_id)


def test_cash_account_only_refused(conn, env):
    import grants as grants_mod
    fund_id, grant_id, expense_id = _setup(conn, env)
    before = _snapshot(conn, expense_id, grant_id, fund_id)
    result = call_action(grants_mod.approve_grant_expense, conn, ns(
        id=expense_id, expense_account_id=None,
        cash_account_id=env["cash_acct"], cost_center_id=env["cc_id"]))
    assert is_error(result), result
    assert result["message"] == (
        f"Approving grant expense {expense_id} posts it to the ledger; "
        "missing: --expense-account-id"), result
    _assert_unchanged(conn, before, expense_id, grant_id, fund_id)


def test_no_ledger_library_refused(conn, env, monkeypatch):
    import grants as grants_mod
    fund_id, grant_id, expense_id = _setup(conn, env)
    before = _snapshot(conn, expense_id, grant_id, fund_id)
    monkeypatch.setattr(grants_mod, "HAS_GL", False)
    result = call_action(grants_mod.approve_grant_expense, conn, ns(
        id=expense_id, expense_account_id=env["expense_acct"],
        cash_account_id=env["cash_acct"], cost_center_id=env["cc_id"]))
    assert is_error(result), result
    assert result["message"] == (
        f"GL posting is not available; grant expense {expense_id} "
        "cannot be approved"), result
    _assert_unchanged(conn, before, expense_id, grant_id, fund_id)


def _assert_posts_and_spends(conn, env, expense_id, grant_id, fund_id, result):
    assert is_ok(result), result
    rows = conn.execute(
        "SELECT id, account_id, debit, credit FROM gl_entry "
        "WHERE voucher_id = ?", (expense_id,)).fetchall()
    assert len(rows) == 2, rows
    by_account = {r["account_id"]: (Decimal(str(r["debit"])), Decimal(str(r["credit"]))) for r in rows}
    assert by_account == {
        env["expense_acct"]: (Decimal("40000.00"), Decimal("0")),
        env["cash_acct"]: (Decimal("0"), Decimal("40000.00")),
    }, by_account
    grant_row = conn.execute(
        "SELECT spent_amount, remaining_amount FROM nonprofitclaw_grant "
        "WHERE id = ?", (grant_id,)).fetchone()
    assert grant_row["spent_amount"] == "40000.00", dict(grant_row)
    assert grant_row["remaining_amount"] == "10000.00", dict(grant_row)
    fund_row = conn.execute(
        "SELECT current_balance FROM nonprofitclaw_fund WHERE id = ?",
        (fund_id,)).fetchone()
    assert fund_row["current_balance"] == "10000.00", dict(fund_row)
    stored = conn.execute(
        "SELECT gl_entry_ids FROM nonprofitclaw_grant_expense WHERE id = ?",
        (expense_id,)).fetchone()["gl_entry_ids"]
    assert sorted(json.loads(stored)) == sorted([r["id"] for r in rows])


def test_with_accounts_posts_and_spends(conn, env):
    import grants as grants_mod
    fund_id, grant_id, expense_id = _setup(conn, env)
    result = call_action(grants_mod.approve_grant_expense, conn, ns(
        id=expense_id, expense_account_id=env["expense_acct"],
        cash_account_id=env["cash_acct"], cost_center_id=env["cc_id"]))
    _assert_posts_and_spends(conn, env, expense_id, grant_id, fund_id, result)


def test_refused_expense_can_then_be_approved(conn, env):
    import grants as grants_mod
    fund_id, grant_id, expense_id = _setup(conn, env)
    refused = call_action(grants_mod.approve_grant_expense, conn, ns(
        id=expense_id, expense_account_id=None, cash_account_id=None,
        cost_center_id=None))
    assert is_error(refused), refused
    assert refused["message"] == (
        f"Approving grant expense {expense_id} posts it to the ledger; "
        "missing: --expense-account-id, --cash-account-id"), refused
    result = call_action(grants_mod.approve_grant_expense, conn, ns(
        id=expense_id, expense_account_id=env["expense_acct"],
        cash_account_id=env["cash_acct"], cost_center_id=env["cc_id"]))
    _assert_posts_and_spends(conn, env, expense_id, grant_id, fund_id, result)


def test_status_refusal_comes_first(conn, env):
    import grants as grants_mod
    fund_id, grant_id, expense_id = _setup(conn, env)
    first = call_action(grants_mod.approve_grant_expense, conn, ns(
        id=expense_id, expense_account_id=env["expense_acct"],
        cash_account_id=env["cash_acct"], cost_center_id=env["cc_id"]))
    assert is_ok(first), first
    again = call_action(grants_mod.approve_grant_expense, conn, ns(
        id=expense_id, expense_account_id=None, cash_account_id=None,
        cost_center_id=None))
    assert is_error(again), again
    assert again["message"] == (
        "Expense must be in 'draft' or 'submitted' status to approve, "
        "currently 'approved'"), again


def test_missing_cost_center_refused_with_rollback(conn, env):
    import grants as grants_mod
    fund_id, grant_id, expense_id = _setup(conn, env)
    # Persist the fixture: the setup's last audit row is still pending, and
    # the refused approval rolls back its whole transaction (product rule).
    conn.commit()
    before = _snapshot(conn, expense_id, grant_id, fund_id)
    result = call_action(grants_mod.approve_grant_expense, conn, ns(
        id=expense_id, expense_account_id=env["expense_acct"],
        cash_account_id=env["cash_acct"], cost_center_id=None))
    assert is_error(result), result
    assert result["message"].startswith(
        f"GL posting failed for grant expense {expense_id}:"), result
    assert "Step 6" in result["message"], result
    _assert_unchanged(conn, before, expense_id, grant_id, fund_id)


def test_foreign_expense_account_refused_with_rollback(conn, env):
    import grants as grants_mod
    fund_id, grant_id, expense_id = _setup(conn, env)
    # Persist the fixture (see above); the seed commits below would do
    # it anyway, but the snapshot must not depend on helper internals.
    conn.commit()
    other_company = seed_company(conn, name="Other Org", abbr="OTH")
    foreign_acct = seed_account(conn, other_company, "Foreign Expense",
                                "expense", "expense")
    before = _snapshot(conn, expense_id, grant_id, fund_id)
    result = call_action(grants_mod.approve_grant_expense, conn, ns(
        id=expense_id, expense_account_id=foreign_acct,
        cash_account_id=env["cash_acct"], cost_center_id=env["cc_id"]))
    assert is_error(result), result
    assert result["message"].startswith(
        f"GL posting failed for grant expense {expense_id}:"), result
    assert "Step 3" in result["message"], result
    _assert_unchanged(conn, before, expense_id, grant_id, fund_id)
