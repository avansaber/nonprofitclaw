"""Tests for nonprofit-reject-grant-expense.

A draft grant expense that can never be approved (over budget) must be
rejectable so it stops blocking nonprofit-close-grant. Rejection writes
no GL entries and leaves the grant and fund totals untouched.
"""
import json

from erpclaw_lib.query import Q, P, Table
from nonprofit_helpers import (
    call_action, ns, is_error, is_ok, load_db_query,
    seed_grant, snapshot_tables,
)

mod = load_db_query()

_expense = Table("nonprofitclaw_grant_expense")
_grant = Table("nonprofitclaw_grant")
_audit = Table("audit_log")


def _add_expense(conn, company_id, grant_id, amount):
    import grants as grants_mod
    result = call_action(grants_mod.add_grant_expense, conn, ns(
        company_id=company_id,
        grant_id=grant_id,
        amount=amount,
        category="program",
        description=None,
        expense_date="2026-03-05",
        receipt_reference=None,
    ))
    assert is_ok(result), result
    return result["id"]


def _approve(conn, env, expense_id):
    import grants as grants_mod
    return call_action(grants_mod.approve_grant_expense, conn, ns(
        id=expense_id,
        expense_account_id=env["expense_acct"],
        cash_account_id=env["cash_acct"],
        cost_center_id=env["cc_id"],
    ))


def _expense_row(conn, expense_id):
    q = Q.from_(_expense).select(_expense.star).where(_expense.id == P())
    return conn.execute(q.get_sql(), (expense_id,)).fetchone()


def _grant_row(conn, grant_id):
    q = Q.from_(_grant).select(_grant.star).where(_grant.id == P())
    return conn.execute(q.get_sql(), (grant_id,)).fetchone()


def _gl_entry_count(conn):
    return conn.execute("SELECT COUNT(*) FROM gl_entry").fetchone()[0]


class TestRejectGrantExpense:
    def test_over_budget_draft_rejected_then_grant_closes(self, conn, env):
        import grants as grants_mod
        gid = seed_grant(conn, env["company_id"], "Rejectable Grant",
                         amount="10000.00", status="active")

        first_id = _add_expense(conn, env["company_id"], gid, "8000.00")
        approved = _approve(conn, env, first_id)
        assert is_ok(approved), approved

        draft_id = _add_expense(conn, env["company_id"], gid, "12000.50")
        refused = _approve(conn, env, draft_id)
        assert is_error(refused)
        assert refused["message"] == (
            "Expense amount (12000.50) exceeds grant remaining (2000.00)")

        gl_before = _gl_entry_count(conn)
        result = call_action(grants_mod.reject_grant_expense, conn, ns(
            id=draft_id, reason="over budget",
        ))
        assert is_ok(result), result
        assert result["id"] == draft_id
        assert result.get("document_status", result.get("status")) == "rejected"
        assert result["amount"] == "12000.50"
        assert result["grant_id"] == gid

        assert _expense_row(conn, draft_id)["status"] == "rejected"

        grant = _grant_row(conn, gid)
        assert grant["spent_amount"] == "8000.00"
        assert grant["remaining_amount"] == "2000.00"
        assert _gl_entry_count(conn) == gl_before

        closed = call_action(grants_mod.close_grant, conn, ns(id=gid))
        assert is_ok(closed), closed
        assert closed["grant_status"] == "completed"

    def test_reject_audit_row(self, conn, env):
        import grants as grants_mod
        gid = seed_grant(conn, env["company_id"], "Audited Grant",
                         amount="10000.00", status="active")
        draft_id = _add_expense(conn, env["company_id"], gid, "12000.50")

        result = call_action(grants_mod.reject_grant_expense, conn, ns(
            id=draft_id, reason="over budget",
        ))
        assert is_ok(result), result

        q = (
            Q.from_(_audit)
            .select(_audit.star)
            .where(_audit.action == "nonprofit-reject-grant-expense")
        )
        rows = conn.execute(q.get_sql()).fetchall()
        assert len(rows) == 1
        entry = dict(rows[0])
        assert entry["entity_id"] == draft_id
        assert entry["entity_type"] == "nonprofitclaw_grant_expense"
        old_values = json.loads(entry["old_values"])
        new_values = json.loads(entry["new_values"])
        assert old_values["status"] == "draft"
        assert new_values["status"] == "rejected"
        assert new_values["reason"] == "over budget"

    def test_reject_no_id_refuses_without_writes(self, conn, env):
        import grants as grants_mod
        gid = seed_grant(conn, env["company_id"], "No Id Grant",
                         amount="10000.00", status="active")
        _add_expense(conn, env["company_id"], gid, "100.00")
        before = snapshot_tables(conn, ["nonprofitclaw_grant_expense",
                                        "nonprofitclaw_grant",
                                        "audit_log"])
        result = call_action(grants_mod.reject_grant_expense, conn, ns(
            id=None, reason=None,
        ))
        assert is_error(result)
        assert result["message"] == "--id is required"
        assert snapshot_tables(conn, ["nonprofitclaw_grant_expense",
                                      "nonprofitclaw_grant",
                                      "audit_log"]) == before

    def test_reject_unknown_id_refuses_without_writes(self, conn, env):
        import grants as grants_mod
        gid = seed_grant(conn, env["company_id"], "Unknown Id Grant",
                         amount="10000.00", status="active")
        _add_expense(conn, env["company_id"], gid, "100.00")
        before = snapshot_tables(conn, ["nonprofitclaw_grant_expense",
                                        "nonprofitclaw_grant",
                                        "audit_log"])
        result = call_action(grants_mod.reject_grant_expense, conn, ns(
            id="grant-expense-missing", reason=None,
        ))
        assert is_error(result)
        assert result["message"] == "Grant expense grant-expense-missing not found"
        assert snapshot_tables(conn, ["nonprofitclaw_grant_expense",
                                      "nonprofitclaw_grant",
                                      "audit_log"]) == before

    def test_reject_approved_expense_refuses_without_writes(self, conn, env):
        import grants as grants_mod
        gid = seed_grant(conn, env["company_id"], "Approved Grant",
                         amount="10000.00", status="active")
        expense_id = _add_expense(conn, env["company_id"], gid, "8000.00")
        approved = _approve(conn, env, expense_id)
        assert is_ok(approved), approved
        before = snapshot_tables(conn, ["nonprofitclaw_grant_expense",
                                        "nonprofitclaw_grant",
                                        "audit_log"])
        result = call_action(grants_mod.reject_grant_expense, conn, ns(
            id=expense_id, reason="over budget",
        ))
        assert is_error(result)
        assert result["message"] == (
            f"Grant expense {expense_id} is approved; "
            "an approved expense cannot be rejected")
        assert snapshot_tables(conn, ["nonprofitclaw_grant_expense",
                                      "nonprofitclaw_grant",
                                      "audit_log"]) == before

    def test_reject_already_rejected_refuses_without_writes(self, conn, env):
        import grants as grants_mod
        gid = seed_grant(conn, env["company_id"], "Twice Rejected Grant",
                         amount="10000.00", status="active")
        draft_id = _add_expense(conn, env["company_id"], gid, "12000.50")
        first = call_action(grants_mod.reject_grant_expense, conn, ns(
            id=draft_id, reason="over budget",
        ))
        assert is_ok(first), first
        before = snapshot_tables(conn, ["nonprofitclaw_grant_expense",
                                        "nonprofitclaw_grant",
                                        "audit_log"])
        result = call_action(grants_mod.reject_grant_expense, conn, ns(
            id=draft_id, reason="over budget",
        ))
        assert is_error(result)
        assert result["message"] == f"Grant expense {draft_id} is already rejected"
        assert snapshot_tables(conn, ["nonprofitclaw_grant_expense",
                                      "nonprofitclaw_grant",
                                      "audit_log"]) == before

    def test_rejected_listed_by_status(self, conn, env):
        import grants as grants_mod
        gid = seed_grant(conn, env["company_id"], "Listed Grant",
                         amount="10000.00", status="active")
        draft_id = _add_expense(conn, env["company_id"], gid, "12000.50")
        rejected = call_action(grants_mod.reject_grant_expense, conn, ns(
            id=draft_id, reason="over budget",
        ))
        assert is_ok(rejected), rejected
        listed = call_action(grants_mod.list_grant_expenses, conn, ns(
            company_id=env["company_id"],
            grant_id=None, status="rejected", category=None,
            from_date=None, to_date=None,
            limit="50", offset="0",
        ))
        assert is_ok(listed), listed
        by_id = {entry["id"]: entry for entry in listed["grant_expenses"]}
        assert draft_id in by_id
        assert by_id[draft_id]["amount"] == "12000.50"

    def test_routed_through_db_query(self):
        assert "nonprofit-reject-grant-expense" in mod.ACTIONS
