"""Money received on a grant is its own ledger-posted receipt, and cancelling one reverses it.

Recording a grant receipt posts DR cash / CR the credit account, raises the
grant's received and remaining amounts and its fund by exactly the receipt, and
is refused above the award less what is already received. Cancelling reverses
the ledger rows, marks the receipt cancelled, lowers the same three amounts,
and is refused when approved expenses would then exceed receipts.
"""
import json
import uuid

import pytest

from erpclaw_lib.query import Q, P, Table
from nonprofit_helpers import (
    call_action, is_error, is_ok, load_db_query, ns, seed_company,
    seed_fiscal_year, snapshot_tables,
)

load_db_query()  # puts the module's scripts directory on sys.path

_grant = Table("nonprofitclaw_grant")
_receipt = Table("nonprofitclaw_grant_receipt")
_fund = Table("nonprofitclaw_fund")
_gl = Table("gl_entry")
_audit = Table("audit_log")

SNAPSHOT_TABLES = [
    "nonprofitclaw_grant_receipt",
    "nonprofitclaw_grant",
    "nonprofitclaw_fund",
    "gl_entry",
    "audit_log",
    "naming_series",
]

RECEIPT_DATE = "2026-03-01"
REFERENCE = "ACH-7781"


def _setup(conn, env, activate_amount="0.00"):
    """A fund, a 50000.00 grant into it, activated (with 0.00 unless told).

    Committed: setup rows are durable before the behaviour under test, so a
    rollback inside a refused call can only wipe that call's own writes.
    """
    mod = load_db_query()
    fund = call_action(mod.ACTIONS["nonprofit-add-fund"], conn, ns(
        company_id=env["company_id"], name="Grant Fund",
        fund_type="temporarily_restricted", description=None,
        target_amount=None, start_date=None, end_date=None))
    assert is_ok(fund), fund
    grant = call_action(mod.ACTIONS["nonprofit-add-grant"], conn, ns(
        company_id=env["company_id"], name="Community Grant",
        grantor_name="Ford Foundation", grantor_type="foundation",
        grant_type="project", amount="50000.00", fund_id=fund["id"],
        start_date=None, end_date=None, reporting_freq="quarterly",
        notes=None))
    assert is_ok(grant), grant
    if activate_amount is not None:
        activated = call_action(mod.ACTIONS["nonprofit-activate-grant"], conn, ns(
            id=grant["id"], amount=activate_amount))
        assert is_ok(activated), activated
    conn.commit()
    return fund["id"], grant["id"]


def _record(conn, env, grant_id, amount="2500.00", receipt_date=RECEIPT_DATE,
            reference=REFERENCE, cash="default", revenue="default",
            cc="default"):
    """Record a receipt; 'default' resolves an account to the suite's own."""
    mod = load_db_query()
    return call_action(mod.ACTIONS["nonprofit-record-grant-receipt"], conn, ns(
        grant_id=grant_id,
        company_id=env["company_id"],
        amount=amount,
        receipt_date=receipt_date,
        cash_account_id=env["cash_acct"] if cash == "default" else cash,
        revenue_account_id=env["revenue_acct"] if revenue == "default" else revenue,
        cost_center_id=env["cc_id"] if cc == "default" else cc,
        reference=reference))


def _cancel(conn, receipt_id):
    mod = load_db_query()
    return call_action(mod.ACTIONS["nonprofit-cancel-grant-receipt"], conn, ns(
        id=receipt_id))


def _grant_amounts(conn, grant_id):
    q = (
        Q.from_(_grant)
        .select(_grant.received_amount, _grant.remaining_amount)
        .where(_grant.id == P())
    )
    row = conn.execute(q.get_sql(), (grant_id,)).fetchone()
    return row["received_amount"], row["remaining_amount"]


def _fund_balance(conn, fund_id):
    q = Q.from_(_fund).select(_fund.current_balance).where(_fund.id == P())
    return conn.execute(q.get_sql(), (fund_id,)).fetchone()["current_balance"]


def _receipt_row(conn, receipt_id):
    q = Q.from_(_receipt).select(_receipt.star).where(_receipt.id == P())
    return conn.execute(q.get_sql(), (receipt_id,)).fetchone()


def _legs(conn, voucher_id):
    q = Q.from_(_gl).select(_gl.star).where(_gl.voucher_id == P())
    return conn.execute(q.get_sql(), (voucher_id,)).fetchall()


def _assert_single_audit_row(conn, entity_id, action):
    q = (
        Q.from_(_audit)
        .select(_audit.skill, _audit.action, _audit.entity_type, _audit.entity_id)
        .where(_audit.entity_id == P())
    )
    rows = conn.execute(q.get_sql(), (entity_id,)).fetchall()
    matches = [
        row for row in rows
        if (row["skill"], row["action"], row["entity_type"], row["entity_id"])
        == ("nonprofitclaw", action, "nonprofitclaw_grant_receipt", entity_id)
    ]
    assert len(matches) == 1, (
        f"expected exactly one audit row "
        f"('nonprofitclaw', '{action}', 'nonprofitclaw_grant_receipt') "
        f"for {entity_id!r}, found {len(matches)}"
    )


def test_record_posts_and_raises_totals(conn, env):
    fund_id, grant_id = _setup(conn, env)
    result = _record(conn, env, grant_id)
    assert is_ok(result), result
    rid = result["id"]
    assert result["grant_id"] == grant_id
    assert result["amount"] == "2500.00"
    assert result["grant_received"] == "2500.00"
    assert result["grant_remaining"] == "2500.00"
    assert result["naming_series"].startswith("NGR-")

    legs = _legs(conn, rid)
    assert len(legs) == 2
    by_account = {row["account_id"]: row for row in legs}
    cash = by_account[env["cash_acct"]]
    assert cash["debit"] == "2500.00"
    assert cash["credit"] == "0.00"
    revenue = by_account[env["revenue_acct"]]
    assert revenue["debit"] == "0.00"
    assert revenue["credit"] == "2500.00"
    for row in legs:
        assert row["voucher_type"] == "journal_entry"
        assert row["posting_date"] == "2026-03-01"
        assert row["is_cancelled"] == 0

    stored = _receipt_row(conn, rid)
    assert stored["status"] == "received"
    assert stored["amount"] == "2500.00"
    assert stored["reference"] == "ACH-7781"
    assert stored["credit_account_id"] == env["revenue_acct"]
    assert stored["fund_id"] == fund_id
    assert stored["cancelled_at"] is None
    assert stored["naming_series"].startswith("NGR-")
    leg_ids = sorted(row["id"] for row in legs)
    assert sorted(json.loads(stored["gl_entry_ids"])) == leg_ids
    assert sorted(result["gl_entry_ids"]) == leg_ids

    assert _grant_amounts(conn, grant_id) == ("2500.00", "2500.00")
    assert _fund_balance(conn, fund_id) == "2500.00"
    _assert_single_audit_row(conn, rid, "nonprofit-record-grant-receipt")


def test_second_receipt_accumulates(conn, env):
    fund_id, grant_id = _setup(conn, env)
    first = _record(conn, env, grant_id, amount="2500.00")
    assert is_ok(first), first
    second = _record(conn, env, grant_id, amount="1000.00")
    assert is_ok(second), second
    assert _grant_amounts(conn, grant_id) == ("3500.00", "3500.00")
    assert _fund_balance(conn, fund_id) == "3500.00"


def test_receipt_above_award_less_received_refused(conn, env):
    fund_id, grant_id = _setup(conn, env)
    first = _record(conn, env, grant_id, amount="2500.00")
    assert is_ok(first), first
    before = snapshot_tables(conn, SNAPSHOT_TABLES)
    refused = _record(conn, env, grant_id, amount="47500.01")
    assert is_error(refused), refused
    assert refused["message"] == (
        "Receipt amount (47500.01) exceeds the grant's award less received "
        "(47500.00)"), refused
    assert snapshot_tables(conn, SNAPSHOT_TABLES) == before
    boundary = _record(conn, env, grant_id, amount="47500.00")
    assert is_ok(boundary), boundary
    assert _grant_amounts(conn, grant_id) == ("50000.00", "50000.00")


def test_cap_counts_money_recorded_by_activation(conn, env):
    fund_id, grant_id = _setup(conn, env, activate_amount="10000.00")
    before = snapshot_tables(conn, SNAPSHOT_TABLES)
    refused = _record(conn, env, grant_id, amount="40000.01")
    assert is_error(refused), refused
    assert refused["message"] == (
        "Receipt amount (40000.01) exceeds the grant's award less received "
        "(40000.00)"), refused
    assert snapshot_tables(conn, SNAPSHOT_TABLES) == before


def test_missing_accounts_refused(conn, env):
    fund_id, grant_id = _setup(conn, env)

    before = snapshot_tables(conn, SNAPSHOT_TABLES)
    neither = _record(conn, env, grant_id, cash=None, revenue=None)
    assert is_error(neither), neither
    assert neither["message"] == (
        "Recording a grant receipt posts it to the ledger; missing: "
        "--cash-account-id, --revenue-account-id"), neither
    assert snapshot_tables(conn, SNAPSHOT_TABLES) == before

    before = snapshot_tables(conn, SNAPSHOT_TABLES)
    cash_only = _record(conn, env, grant_id, revenue=None)
    assert is_error(cash_only), cash_only
    assert cash_only["message"] == (
        "Recording a grant receipt posts it to the ledger; missing: "
        "--revenue-account-id"), cash_only
    assert snapshot_tables(conn, SNAPSHOT_TABLES) == before

    before = snapshot_tables(conn, SNAPSHOT_TABLES)
    revenue_only = _record(conn, env, grant_id, cash=None)
    assert is_error(revenue_only), revenue_only
    assert revenue_only["message"] == (
        "Recording a grant receipt posts it to the ledger; missing: "
        "--cash-account-id"), revenue_only
    assert snapshot_tables(conn, SNAPSHOT_TABLES) == before


def test_no_ledger_library_refused(conn, env, monkeypatch):
    import grants as grants_mod
    fund_id, grant_id = _setup(conn, env)
    before = snapshot_tables(conn, SNAPSHOT_TABLES)
    monkeypatch.setattr(grants_mod, "HAS_GL", False)
    result = _record(conn, env, grant_id)
    assert is_error(result), result
    assert result["message"] == (
        "GL posting is not available; grant receipt cannot be recorded"), result
    assert snapshot_tables(conn, SNAPSHOT_TABLES) == before


def test_record_refused_on_grant_not_active(conn, env):
    mod = load_db_query()
    fund_id, grant_id = _setup(conn, env)
    second = call_action(mod.ACTIONS["nonprofit-add-grant"], conn, ns(
        company_id=env["company_id"], name="Second Grant",
        grantor_name="Ford Foundation", grantor_type="foundation",
        grant_type="project", amount="50000.00", fund_id=fund_id,
        start_date=None, end_date=None, reporting_freq="quarterly",
        notes=None))
    assert is_ok(second), second
    gid2 = second["id"]

    before = snapshot_tables(conn, SNAPSHOT_TABLES)
    refused = _record(conn, env, gid2)
    assert is_error(refused), refused
    assert refused["message"] == (
        "Grant must be 'active' or 'completed' to record a receipt, "
        "currently 'applied'"), refused
    assert snapshot_tables(conn, SNAPSHOT_TABLES) == before

    activated = call_action(mod.ACTIONS["nonprofit-activate-grant"], conn, ns(
        id=gid2, amount="0.00"))
    assert is_ok(activated), activated
    closed = call_action(mod.ACTIONS["nonprofit-close-grant"], conn, ns(id=gid2))
    assert is_ok(closed), closed
    assert closed["grant_status"] == "completed"
    recorded = _record(conn, env, gid2)
    assert is_ok(recorded), recorded
    assert _grant_amounts(conn, gid2) == ("2500.00", "2500.00")


def test_ledger_refusal_rolls_back(conn, env):
    fund_id, grant_id = _setup(conn, env)
    before = snapshot_tables(conn, SNAPSHOT_TABLES)
    result = _record(conn, env, grant_id, cc=None)
    assert is_error(result), result
    assert result["message"].startswith("GL posting failed for grant receipt"), result
    assert "requires a cost_center_id" in result["message"], result
    assert snapshot_tables(conn, SNAPSHOT_TABLES) == before


def test_cancel_reverses_and_restores(conn, env):
    fund_id, grant_id = _setup(conn, env)
    recorded = _record(conn, env, grant_id)
    assert is_ok(recorded), recorded
    rid = recorded["id"]
    original_ids = sorted(recorded["gl_entry_ids"])

    cancelled = _cancel(conn, rid)
    assert is_ok(cancelled), cancelled
    assert cancelled["receipt_status"] == "cancelled"
    assert cancelled["grant_received"] == "0.00"
    assert cancelled["grant_remaining"] == "0.00"

    legs = _legs(conn, rid)
    assert len(legs) == 4
    for row in legs:
        assert row["is_cancelled"] == 1
        assert row["posting_date"] == "2026-03-01"
    pairs = sorted((row["account_id"], row["debit"], row["credit"]) for row in legs)
    assert pairs == sorted([
        (env["cash_acct"], "2500.00", "0.00"),
        (env["cash_acct"], "0.00", "2500.00"),
        (env["revenue_acct"], "0.00", "2500.00"),
        (env["revenue_acct"], "2500.00", "0.00"),
    ])
    assert sorted(cancelled["reversal_gl_entry_ids"]) == sorted(
        set(row["id"] for row in legs) - set(original_ids))

    stored = _receipt_row(conn, rid)
    assert stored["status"] == "cancelled"
    assert stored["cancelled_at"] is not None
    assert stored["amount"] == "2500.00"
    assert sorted(json.loads(stored["gl_entry_ids"])) == original_ids

    assert _grant_amounts(conn, grant_id) == ("0.00", "0.00")
    assert _fund_balance(conn, fund_id) == "0.00"
    _assert_single_audit_row(conn, rid, "nonprofit-cancel-grant-receipt")


def test_cancel_refused_when_expenses_exceed_receipts(conn, env):
    mod = load_db_query()
    fund_id, grant_id = _setup(conn, env)
    first = _record(conn, env, grant_id, amount="2500.00")
    assert is_ok(first), first
    rid1 = first["id"]

    expense = call_action(mod.ACTIONS["nonprofit-add-grant-expense"], conn, ns(
        company_id=env["company_id"], grant_id=grant_id, amount="2000.00",
        category="program", expense_date="2026-03-05", description=None,
        receipt_reference=None))
    assert is_ok(expense), expense
    approved = call_action(mod.ACTIONS["nonprofit-approve-grant-expense"], conn, ns(
        id=expense["id"], expense_account_id=env["expense_acct"],
        cash_account_id=env["cash_acct"], cost_center_id=env["cc_id"]))
    assert is_ok(approved), approved

    before = snapshot_tables(conn, SNAPSHOT_TABLES)
    refused = _cancel(conn, rid1)
    assert is_error(refused), refused
    assert refused["message"] == (
        f"Cancelling grant receipt {rid1} would leave grant {grant_id} "
        f"with approved expenses (2000.00) above its receipts (0.00)"), refused
    assert snapshot_tables(conn, SNAPSHOT_TABLES) == before

    second = _record(conn, env, grant_id, amount="2000.00")
    assert is_ok(second), second
    cancelled = _cancel(conn, rid1)
    assert is_ok(cancelled), cancelled
    assert _grant_amounts(conn, grant_id) == ("2000.00", "0.00")
    assert _fund_balance(conn, fund_id) == "0.00"


def test_cancel_twice_refused(conn, env):
    fund_id, grant_id = _setup(conn, env)
    recorded = _record(conn, env, grant_id)
    assert is_ok(recorded), recorded
    rid = recorded["id"]
    cancelled = _cancel(conn, rid)
    assert is_ok(cancelled), cancelled
    before = snapshot_tables(conn, SNAPSHOT_TABLES)
    again = _cancel(conn, rid)
    assert is_error(again), again
    assert again["message"] == (
        f"Grant receipt {rid} is already cancelled"), again
    assert snapshot_tables(conn, SNAPSHOT_TABLES) == before


def test_cancel_without_ledger_library_refused(conn, env, monkeypatch):
    import grants as grants_mod
    fund_id, grant_id = _setup(conn, env)
    recorded = _record(conn, env, grant_id)
    assert is_ok(recorded), recorded
    rid = recorded["id"]
    before = snapshot_tables(conn, SNAPSHOT_TABLES)
    monkeypatch.setattr(grants_mod, "HAS_GL", False)
    refused = _cancel(conn, rid)
    assert is_error(refused), refused
    assert refused["message"] == (
        f"GL posting is not available; grant receipt {rid} cannot be "
        f"cancelled"), refused
    assert snapshot_tables(conn, SNAPSHOT_TABLES) == before

    unknown = str(uuid.uuid4())
    missing = _cancel(conn, unknown)
    assert is_error(missing), missing
    assert missing["message"] == f"Grant receipt {unknown} not found", missing


def test_cancel_refused_in_a_closed_fiscal_year(conn, env):
    fund_id, grant_id = _setup(conn, env)
    recorded = _record(conn, env, grant_id)
    assert is_ok(recorded), recorded
    rid = recorded["id"]
    before = snapshot_tables(conn, SNAPSHOT_TABLES)
    fiscal_year = Table("fiscal_year")
    conn.execute(
        Q.update(fiscal_year)
        .set(fiscal_year.is_closed, P())
        .where(fiscal_year.id == P())
        .get_sql(),
        (1, env["fiscal_year_id"]))
    conn.commit()
    refused = _cancel(conn, rid)
    assert is_error(refused), refused
    assert refused["message"] == (
        f"Cannot cancel grant receipt {rid}: no open fiscal year covers "
        f"its date 2026-03-01"), refused
    assert snapshot_tables(conn, SNAPSHOT_TABLES) == before


def test_guarded_received_refuses_a_stale_value(conn, env):
    import grants as grants_mod
    fund_id, grant_id = _setup(conn, env)
    before = dict(conn.execute(
        "SELECT * FROM nonprofitclaw_grant WHERE id = ?",
        (grant_id,)).fetchone())
    with pytest.raises(ValueError) as excinfo:
        grants_mod._guarded_grant_received(
            conn, grant_id, "1.00", "1.00", "2.00", "2.00")
    assert str(excinfo.value) == (
        f"Concurrent change to nonprofitclaw_grant {grant_id}: "
        f"received_amount is no longer 1.00; nothing was written")
    after = dict(conn.execute(
        "SELECT * FROM nonprofitclaw_grant WHERE id = ?",
        (grant_id,)).fetchone())
    assert after == before


def test_grant_receipt_cancel_uses_grant_company(conn, env):
    fund_id, grant_id = _setup(conn, env)
    recorded = _record(conn, env, grant_id)
    assert is_ok(recorded), recorded
    rid = recorded["id"]
    fiscal_year = Table("fiscal_year")
    conn.execute(
        Q.update(fiscal_year)
        .set(fiscal_year.is_closed, P())
        .where(fiscal_year.id == P())
        .get_sql(),
        (1, env["fiscal_year_id"]))
    other = seed_company(conn, name="Other Org", abbr="OO")
    seed_fiscal_year(conn, other, name="FY-OTHER-2026",
                     start="2026-01-01", end="2026-12-31")
    conn.commit()
    refused = _cancel(conn, rid)
    assert is_error(refused), refused
    assert refused["message"] == (
        f"Cannot cancel grant receipt {rid}: no open fiscal year covers "
        f"its date {RECEIPT_DATE}"), refused
    stored = _receipt_row(conn, rid)
    assert stored["status"] == "received"
    assert stored["amount"] == "2500.00"
    assert _grant_amounts(conn, grant_id) == ("2500.00", "2500.00")
    assert _fund_balance(conn, fund_id) == "2500.00"
    assert len(_legs(conn, rid)) == 2
    _assert_single_audit_row(conn, rid, "nonprofit-record-grant-receipt")
    cancel_rows = [
        row for row in conn.execute(
            Q.from_(_audit).select(_audit.action)
            .where(_audit.entity_id == P()).get_sql(), (rid,)).fetchall()
        if row["action"] == "nonprofit-cancel-grant-receipt"
    ]
    assert cancel_rows == []
