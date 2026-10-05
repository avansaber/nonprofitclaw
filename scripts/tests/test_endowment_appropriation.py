"""Endowment appropriation v1: board-approved spend from an endowment fund.

Covers the one appropriation action: an explicit positive amount is recorded
against an active permanently restricted endowment fund in the same company,
posts a balanced DR spendable-fund-balance / CR cash-or-investment GL movement
under journal_entry in the same transaction, lowers the tracked endowment
balance, and is idempotent by endowment plus decision reference. Version 1
reports the remaining corpus and offers no legal prudence conclusion and no
pool investment accounting.
"""
from decimal import Decimal

from nonprofit_helpers import (
    call_action, is_error, is_ok, load_db_query, ns, seed_account,
    seed_company, seed_fund, seed_naming_series, snapshot_tables,
)

load_db_query()  # puts the module's scripts directory on sys.path, as the sibling files do

import endowments as endowments_mod

SNAP = [
    "nonprofitclaw_endowment_appropriation",
    "nonprofitclaw_fund",
    "gl_entry",
    "audit_log",
    "naming_series",
]

DATE = "2026-04-15"
REF = "BOARD-2026-01"


def _endowment(conn, company_id, balance="1000.00"):
    return seed_fund(conn, company_id, "Permanent Endowment",
                     "permanently_restricted", balance)


def _spendable(conn, company_id):
    return seed_account(conn, company_id, "Spendable Fund Balance",
                        "equity", "equity", "3001")


def _appropriate(conn, company_id, fund_id, amount="500.03", date=DATE,
                 ref=REF, cash=None, spendable=None, **extra):
    args = dict(company_id=company_id, endowment_fund_id=fund_id,
                decision_date=date, amount=amount, decision_reference=ref,
                cash_account_id=cash, spendable_account_id=spendable,
                cost_center_id=None)
    args.update(extra)
    return call_action(endowments_mod.appropriate_endowment, conn, ns(**args))


def _balance(conn, fund_id):
    return conn.execute(
        "SELECT current_balance FROM nonprofitclaw_fund WHERE id = ?",
        (fund_id,)).fetchone()["current_balance"]


def _legs(conn, voucher_id):
    rows = conn.execute(
        "SELECT account_id, debit, credit FROM gl_entry WHERE voucher_id = ?",
        (voucher_id,)).fetchall()
    return sorted((r["account_id"], r["debit"], r["credit"]) for r in rows)


def _row(conn, appropriation_id):
    return conn.execute(
        "SELECT * FROM nonprofitclaw_endowment_appropriation WHERE id = ?",
        (appropriation_id,)).fetchone()


def test_records_exact_500_03_with_balanced_gl_and_remaining_balance(conn, env):
    fund_id = _endowment(conn, env["company_id"])
    spendable = _spendable(conn, env["company_id"])

    r = _appropriate(conn, env["company_id"], fund_id,
                     cash=env["cash_acct"], spendable=spendable)
    assert is_ok(r), r
    assert r["amount"] == "500.03"
    assert r["remaining_balance"] == "499.97"
    assert r["id"] and r["gl_entry_ids"] and len(r["gl_entry_ids"]) == 2

    stored = _row(conn, r["id"])
    assert stored["amount"] == "500.03"
    assert stored["decision_reference"] == REF
    assert stored["endowment_fund_id"] == fund_id

    assert _legs(conn, r["id"]) == sorted([
        (spendable, "500.03", "0.00"),
        (env["cash_acct"], "0.00", "500.03"),
    ])
    legs = _legs(conn, r["id"])
    debit = sum(Decimal(d) for _, d, _ in legs)
    credit = sum(Decimal(c) for _, _, c in legs)
    assert debit == credit == Decimal("500.03")

    assert _balance(conn, fund_id) == "499.97"

    audit = conn.execute(
        "SELECT * FROM audit_log WHERE entity_id = ?",
        (r["id"],)).fetchone()
    assert audit is not None
    assert audit["action"] == "nonprofit-appropriate-endowment"


def test_identical_retry_returns_same_record_without_new_writes(conn, env):
    fund_id = _endowment(conn, env["company_id"])
    spendable = _spendable(conn, env["company_id"])

    first = _appropriate(conn, env["company_id"], fund_id,
                         cash=env["cash_acct"], spendable=spendable)
    assert is_ok(first), first
    before = snapshot_tables(conn, SNAP)

    retry = _appropriate(conn, env["company_id"], fund_id,
                         cash=env["cash_acct"], spendable=spendable)
    assert is_ok(retry), retry
    assert retry["id"] == first["id"]
    assert retry["amount"] == "500.03"
    assert retry["remaining_balance"] == "499.97"
    assert retry["gl_entry_ids"] == first["gl_entry_ids"]
    assert snapshot_tables(conn, SNAP) == before
    assert _balance(conn, fund_id) == "499.97"


def test_conflicting_retry_on_same_reference_is_refused(conn, env):
    fund_id = _endowment(conn, env["company_id"])
    spendable = _spendable(conn, env["company_id"])

    first = _appropriate(conn, env["company_id"], fund_id,
                         cash=env["cash_acct"], spendable=spendable)
    assert is_ok(first), first
    before = snapshot_tables(conn, SNAP)

    clash = _appropriate(conn, env["company_id"], fund_id, amount="100.00",
                         cash=env["cash_acct"], spendable=spendable)
    assert is_error(clash), clash
    assert "already used" in clash.get("message", "")
    assert snapshot_tables(conn, SNAP) == before
    assert _balance(conn, fund_id) == "499.97"


def test_overdraw_is_refused_without_partial_writes(conn, env):
    fund_id = _endowment(conn, env["company_id"])
    spendable = _spendable(conn, env["company_id"])
    before = snapshot_tables(conn, SNAP)

    r = _appropriate(conn, env["company_id"], fund_id, amount="1000.01",
                     cash=env["cash_acct"], spendable=spendable)
    assert is_error(r), r
    assert "Available: 1000.00" in r.get("message", "")
    assert snapshot_tables(conn, SNAP) == before
    assert _balance(conn, fund_id) == "1000.00"
    assert conn.execute(
        "SELECT COUNT(*) FROM gl_entry").fetchone()[0] == 0


def test_missing_decision_reference_is_refused(conn, env):
    fund_id = _endowment(conn, env["company_id"])
    spendable = _spendable(conn, env["company_id"])
    before = snapshot_tables(conn, SNAP)

    for ref in (None, "", "   "):
        r = _appropriate(conn, env["company_id"], fund_id, ref=ref,
                         cash=env["cash_acct"], spendable=spendable)
        assert is_error(r), r
        assert "decision-reference" in r.get("message", "")
    assert snapshot_tables(conn, SNAP) == before
    assert _balance(conn, fund_id) == "1000.00"


def test_endowment_and_account_company_isolation(conn, env):
    fund_id = _endowment(conn, env["company_id"])
    spendable = _spendable(conn, env["company_id"])

    other_company = seed_company(conn, "Second Nonprofit", "SNP")
    seed_naming_series(conn, other_company)
    other_fund = seed_fund(conn, other_company, "Other Endowment",
                           "permanently_restricted", "5000.00")
    other_cash = seed_account(conn, other_company, "Other Cash",
                              "asset", "cash", "1002")
    before = snapshot_tables(conn, SNAP)

    r = _appropriate(conn, env["company_id"], other_fund,
                     cash=env["cash_acct"], spendable=spendable)
    assert is_error(r), r
    assert "does not belong to this company" in r.get("message", "")

    r = _appropriate(conn, env["company_id"], fund_id,
                     cash=other_cash, spendable=spendable)
    assert is_error(r), r
    assert "does not belong to this company" in r.get("message", "")

    assert snapshot_tables(conn, SNAP) == before
    assert _balance(conn, fund_id) == "1000.00"
    assert _balance(conn, other_fund) == "5000.00"


def test_group_disabled_and_wrong_type_accounts_are_refused(conn, env):
    fund_id = _endowment(conn, env["company_id"])
    spendable = _spendable(conn, env["company_id"])
    before = snapshot_tables(conn, SNAP)

    grouped = seed_account(conn, env["company_id"], "Grouped Cash",
                           "asset", "cash", "1003")
    conn.execute("UPDATE account SET is_group = 1 WHERE id = ?", (grouped,))
    conn.commit()
    r = _appropriate(conn, env["company_id"], fund_id,
                     cash=grouped, spendable=spendable)
    assert is_error(r), r
    assert "group account" in r.get("message", "")

    frozen = seed_account(conn, env["company_id"], "Frozen Cash",
                          "asset", "cash", "1004")
    conn.execute("UPDATE account SET disabled = 1 WHERE id = ?", (frozen,))
    conn.commit()
    r = _appropriate(conn, env["company_id"], fund_id,
                     cash=frozen, spendable=spendable)
    assert is_error(r), r
    assert "disabled" in r.get("message", "")

    r = _appropriate(conn, env["company_id"], fund_id,
                     cash=env["revenue_acct"], spendable=spendable)
    assert is_error(r), r
    assert "asset" in r.get("message", "")

    r = _appropriate(conn, env["company_id"], fund_id,
                     cash=env["cash_acct"], spendable=env["cash_acct"])
    assert is_error(r), r
    assert "equity" in r.get("message", "")

    assert snapshot_tables(conn, SNAP) == before
    assert _balance(conn, fund_id) == "1000.00"


def test_inactive_or_non_endowment_fund_is_refused(conn, env):
    spendable = _spendable(conn, env["company_id"])

    plain = seed_fund(conn, env["company_id"], "General",
                      "unrestricted", "5000.00")
    dormant = _endowment(conn, env["company_id"])
    conn.execute("UPDATE nonprofitclaw_fund SET is_active = 0 WHERE id = ?",
                 (dormant,))
    conn.commit()
    before = snapshot_tables(conn, SNAP)

    r = _appropriate(conn, env["company_id"], plain,
                     cash=env["cash_acct"], spendable=spendable)
    assert is_error(r), r
    assert "permanently restricted" in r.get("message", "")

    r = _appropriate(conn, env["company_id"], dormant,
                     cash=env["cash_acct"], spendable=spendable)
    assert is_error(r), r
    assert "not active" in r.get("message", "")

    assert snapshot_tables(conn, SNAP) == before


def test_gl_failure_writes_nothing(conn, env):
    fund_id = _endowment(conn, env["company_id"])
    spendable = _spendable(conn, env["company_id"])
    before = snapshot_tables(conn, SNAP)

    r = _appropriate(conn, env["company_id"], fund_id, date="2027-06-01",
                     ref="BOARD-2027-01",
                     cash=env["cash_acct"], spendable=spendable)
    assert is_error(r), r
    assert "GL posting failed" in r.get("message", "")
    assert snapshot_tables(conn, SNAP) == before
    assert _balance(conn, fund_id) == "1000.00"


def test_zero_and_negative_amounts_are_refused(conn, env):
    fund_id = _endowment(conn, env["company_id"])
    spendable = _spendable(conn, env["company_id"])
    before = snapshot_tables(conn, SNAP)

    for amount in ("0", "0.00", "-10.00"):
        r = _appropriate(conn, env["company_id"], fund_id, amount=amount,
                         cash=env["cash_acct"], spendable=spendable)
        assert is_error(r), r
        assert "positive" in r.get("message", "")
    assert snapshot_tables(conn, SNAP) == before


def test_alias_flags_reach_the_same_action(conn, env):
    fund_id = _endowment(conn, env["company_id"])
    spendable = _spendable(conn, env["company_id"])

    r = call_action(endowments_mod.appropriate_endowment, conn, ns(
        company_id=env["company_id"], fund_id=fund_id,
        decision_date=DATE, amount="500.03", reference=REF,
        cash_account_id=env["cash_acct"], spendable_account_id=spendable,
        cost_center_id=None))
    assert is_ok(r), r
    assert r["amount"] == "500.03"
    assert r["remaining_balance"] == "499.97"


def test_routing_and_parser_flags():
    mod = load_db_query()
    assert "nonprofit-appropriate-endowment" in mod.ACTIONS
    parser = mod.build_parser()
    args = parser.parse_args([
        "--action", "nonprofit-appropriate-endowment",
        "--company-id", "c1", "--endowment-fund-id", "f1",
        "--decision-date", DATE, "--amount", "500.03",
        "--decision-reference", REF, "--cash-account-id", "a1",
        "--spendable-account-id", "a2",
    ])
    assert args.endowment_fund_id == "f1"
    assert args.decision_date == DATE
    assert args.decision_reference == REF
    assert args.cash_account_id == "a1"
    assert args.spendable_account_id == "a2"
