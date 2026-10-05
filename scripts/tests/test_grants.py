"""Conditional contributions v1: explicit donor conditions choose the credit leg.

`nonprofit-classify-conditional-contribution` records one grant receipt at one
exact positive amount on an active grant. The caller supplies the company
accounts and states whether the donor condition was met; the action never
infers it. Met posts DR cash / CR contribution revenue, unmet posts DR cash /
CR the refundable advance liability. Company scope, account types, group and
disabled accounts, invalid Decimals and missing explicit conditions are all
refused with no writes.
"""
import json
from decimal import Decimal

from erpclaw_lib.query import Q, P, Table
from nonprofit_helpers import (
    call_action, is_error, is_ok, load_db_query, ns,
    seed_account, seed_company, seed_fiscal_year, snapshot_tables,
)

load_db_query()

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

AMOUNT = "500.03"
RECEIPT_DATE = "2026-03-01"
CONDITION = "Serve 200 meals by 2026-06-30"


def _advance_acct(conn, company_id):
    return seed_account(conn, company_id, "Refundable Advances",
                        "liability", None, "2100")


def _activate(conn, env):
    mod = load_db_query()
    r = call_action(mod.ACTIONS["nonprofit-activate-grant"], conn, ns(
        id=env["grant_id"], amount="0.00"))
    assert is_ok(r), r
    conn.commit()
    return env["grant_id"]


def _classify(conn, env, grant_id=None, amount=AMOUNT,
              receipt_date=RECEIPT_DATE, cash="default", revenue="default",
              advance=None, condition=CONDITION, met=True, cc="default",
              company=None):
    mod = load_db_query()
    return call_action(
        mod.ACTIONS["nonprofit-classify-conditional-contribution"], conn, ns(
            company_id=company or env["company_id"],
            grant_id=grant_id or env["grant_id"],
            amount=amount,
            receipt_date=receipt_date,
            cash_account_id=env["cash_acct"] if cash == "default" else cash,
            revenue_account_id=env["revenue_acct"] if revenue == "default" else revenue,
            refundable_advance_account_id=advance,
            condition_text=condition,
            condition_met=met,
            cost_center_id=env["cc_id"] if cc == "default" else cc,
            reference=None))


def _legs(conn, voucher_id):
    q = Q.from_(_gl).select(_gl.star).where(_gl.voucher_id == P())
    return conn.execute(q.get_sql(), (voucher_id,)).fetchall()


def _assert_single_audit_row(conn, entity_id):
    q = (
        Q.from_(_audit)
        .select(_audit.skill, _audit.action, _audit.entity_type, _audit.entity_id)
        .where(_audit.entity_id == P())
    )
    rows = conn.execute(q.get_sql(), (entity_id,)).fetchall()
    matches = [
        row for row in rows
        if (row["skill"], row["action"], row["entity_type"], row["entity_id"])
        == ("nonprofitclaw", "nonprofit-classify-conditional-contribution",
            "nonprofitclaw_grant_receipt", entity_id)
    ]
    assert len(matches) == 1, (
        "expected exactly one audit row "
        "('nonprofitclaw', 'nonprofit-classify-conditional-contribution', "
        "'nonprofitclaw_grant_receipt') for %r, found %d"
        % (entity_id, len(matches)))


def test_condition_met_posts_contribution_revenue(conn, env):
    grant_id = _activate(conn, env)
    advance = _advance_acct(conn, env["company_id"])
    result = _classify(conn, env, grant_id=grant_id, advance=advance, met=True)
    assert is_ok(result), result
    rid = result["receipt_id"] if "receipt_id" in result else result["id"]
    assert result["classification"] == "contribution_revenue"
    assert result["amount"] == "500.03"
    assert result["id"] == rid
    assert result["condition_met"] is True

    legs = _legs(conn, rid)
    assert len(legs) == 2
    by_account = {row["account_id"]: row for row in legs}
    assert by_account[env["cash_acct"]]["debit"] == "500.03"
    assert by_account[env["cash_acct"]]["credit"] == "0.00"
    assert by_account[env["revenue_acct"]]["debit"] == "0.00"
    assert by_account[env["revenue_acct"]]["credit"] == "500.03"
    assert by_account[env["cash_acct"]]["debit"] != by_account[env["cash_acct"]]["credit"]
    total_debit = sum((Decimal(r["debit"]) for r in legs), Decimal("0"))
    total_credit = sum((Decimal(r["credit"]) for r in legs), Decimal("0"))
    assert total_debit == total_credit == Decimal("500.03")
    for row in legs:
        assert row["voucher_type"] == "journal_entry"
        assert row["posting_date"] == RECEIPT_DATE
        assert row["is_cancelled"] == 0

    stored = conn.execute(
        Q.from_(_receipt).select(_receipt.star).where(_receipt.id == P()).get_sql(),
        (rid,)).fetchone()
    assert stored["amount"] == "500.03"
    assert stored["status"] == "received"
    assert stored["credit_account_id"] == env["revenue_acct"]
    assert stored["cash_account_id"] == env["cash_acct"]
    leg_ids = sorted(row["id"] for row in legs)
    assert sorted(json.loads(stored["gl_entry_ids"])) == leg_ids
    assert sorted(result["gl_entry_ids"]) == leg_ids

    grant = conn.execute(
        Q.from_(_grant).select(_grant.received_amount, _grant.remaining_amount).where(_grant.id == P()).get_sql(),
        (grant_id,)).fetchone()
    assert (grant["received_amount"], grant["remaining_amount"]) == ("500.03", "500.03")
    _assert_single_audit_row(conn, rid)


def test_condition_unmet_posts_refundable_advance(conn, env):
    grant_id = _activate(conn, env)
    advance = _advance_acct(conn, env["company_id"])
    result = _classify(conn, env, grant_id=grant_id, advance=advance, met=False)
    assert is_ok(result), result
    rid = result["receipt_id"] if "receipt_id" in result else result["id"]
    assert result["classification"] == "refundable_advance"
    assert result["amount"] == "500.03"
    assert result["condition_met"] is False

    legs = _legs(conn, rid)
    assert len(legs) == 2
    by_account = {row["account_id"]: row for row in legs}
    assert by_account[env["cash_acct"]]["debit"] == "500.03"
    assert by_account[env["cash_acct"]]["credit"] == "0.00"
    assert by_account[advance]["debit"] == "0.00"
    assert by_account[advance]["credit"] == "500.03"
    total_debit = sum((Decimal(r["debit"]) for r in legs), Decimal("0"))
    total_credit = sum((Decimal(r["credit"]) for r in legs), Decimal("0"))
    assert total_debit == total_credit == Decimal("500.03")

    stored = conn.execute(
        Q.from_(_receipt).select(_receipt.star).where(_receipt.id == P()).get_sql(),
        (rid,)).fetchone()
    assert stored["amount"] == "500.03"
    assert stored["credit_account_id"] == advance
    _assert_single_audit_row(conn, rid)


def test_condition_met_string_false_is_advance(conn, env):
    grant_id = _activate(conn, env)
    advance = _advance_acct(conn, env["company_id"])
    result = _classify(conn, env, grant_id=grant_id, advance=advance, met="false")
    assert is_ok(result), result
    assert result["classification"] == "refundable_advance"


def test_grant_company_isolation_refused_without_writes(conn, env):
    grant_id = _activate(conn, env)
    advance = _advance_acct(conn, env["company_id"])
    other = seed_company(conn, name="Other Org", abbr="OO")
    seed_fiscal_year(conn, other, name="FY-OTHER-2026",
                     start="2026-01-01", end="2026-12-31")
    conn.commit()
    before = snapshot_tables(conn, SNAPSHOT_TABLES)
    refused = _classify(conn, env, grant_id=grant_id, advance=advance,
                        company=other)
    assert is_error(refused), refused
    assert "does not belong" in refused["message"]
    assert snapshot_tables(conn, SNAPSHOT_TABLES) == before


def test_account_company_isolation_refused_without_writes(conn, env):
    grant_id = _activate(conn, env)
    other = seed_company(conn, name="Other Org", abbr="OO")
    seed_fiscal_year(conn, other, name="FY-OTHER-2026",
                     start="2026-01-01", end="2026-12-31")
    foreign_cash = seed_account(conn, other, "Foreign Cash", "asset", "cash", "1001")
    advance = _advance_acct(conn, env["company_id"])
    conn.commit()
    before = snapshot_tables(conn, SNAPSHOT_TABLES)
    refused = _classify(conn, env, grant_id=grant_id, advance=advance,
                        cash=foreign_cash)
    assert is_error(refused), refused
    assert "does not belong" in refused["message"]
    assert snapshot_tables(conn, SNAPSHOT_TABLES) == before


def test_wrong_account_types_refused_without_writes(conn, env):
    grant_id = _activate(conn, env)
    advance = _advance_acct(conn, env["company_id"])
    conn.commit()

    before = snapshot_tables(conn, SNAPSHOT_TABLES)
    refused = _classify(conn, env, grant_id=grant_id, advance=advance,
                        cash=env["revenue_acct"])
    assert is_error(refused), refused
    assert "asset" in refused["message"]
    assert snapshot_tables(conn, SNAPSHOT_TABLES) == before

    before = snapshot_tables(conn, SNAPSHOT_TABLES)
    refused = _classify(conn, env, grant_id=grant_id, advance=advance,
                        revenue=env["cash_acct"])
    assert is_error(refused), refused
    assert "income" in refused["message"]
    assert snapshot_tables(conn, SNAPSHOT_TABLES) == before

    before = snapshot_tables(conn, SNAPSHOT_TABLES)
    refused = _classify(conn, env, grant_id=grant_id, advance=env["revenue_acct"])
    assert is_error(refused), refused
    assert "liability" in refused["message"]
    assert snapshot_tables(conn, SNAPSHOT_TABLES) == before


def test_group_and_disabled_accounts_refused_without_writes(conn, env):
    grant_id = _activate(conn, env)
    advance = _advance_acct(conn, env["company_id"])
    grouped = seed_account(conn, env["company_id"], "Grouped Cash",
                           "asset", "cash", "1010")
    disabled = seed_account(conn, env["company_id"], "Old Advances",
                            "liability", None, "2110")
    conn.execute("UPDATE account SET is_group = 1 WHERE id = ?", (grouped,))
    conn.execute("UPDATE account SET disabled = 1 WHERE id = ?", (disabled,))
    conn.commit()

    before = snapshot_tables(conn, SNAPSHOT_TABLES)
    refused = _classify(conn, env, grant_id=grant_id, advance=advance,
                        cash=grouped)
    assert is_error(refused), refused
    assert "group" in refused["message"].lower()
    assert snapshot_tables(conn, SNAPSHOT_TABLES) == before

    before = snapshot_tables(conn, SNAPSHOT_TABLES)
    refused = _classify(conn, env, grant_id=grant_id, advance=disabled)
    assert is_error(refused), refused
    assert "disabled" in refused["message"].lower()
    assert snapshot_tables(conn, SNAPSHOT_TABLES) == before


def test_invalid_decimal_refused_without_writes(conn, env):
    grant_id = _activate(conn, env)
    advance = _advance_acct(conn, env["company_id"])
    conn.commit()
    before = snapshot_tables(conn, SNAPSHOT_TABLES)
    refused = _classify(conn, env, grant_id=grant_id, advance=advance,
                        amount="five-hundred")
    assert is_error(refused), refused
    assert "Decimal" in refused["message"]
    assert snapshot_tables(conn, SNAPSHOT_TABLES) == before


def test_missing_explicit_condition_refused_without_writes(conn, env):
    grant_id = _activate(conn, env)
    advance = _advance_acct(conn, env["company_id"])
    conn.commit()

    before = snapshot_tables(conn, SNAPSHOT_TABLES)
    refused = _classify(conn, env, grant_id=grant_id, advance=advance,
                        condition=None)
    assert is_error(refused), refused
    assert "condition" in refused["message"].lower()
    assert snapshot_tables(conn, SNAPSHOT_TABLES) == before

    before = snapshot_tables(conn, SNAPSHOT_TABLES)
    refused = _classify(conn, env, grant_id=grant_id, advance=advance,
                        condition="   ")
    assert is_error(refused), refused
    assert snapshot_tables(conn, SNAPSHOT_TABLES) == before

    before = snapshot_tables(conn, SNAPSHOT_TABLES)
    refused = _classify(conn, env, grant_id=grant_id, advance=advance,
                        met=None)
    assert is_error(refused), refused
    assert "condition-met" in refused["message"].lower()
    assert snapshot_tables(conn, SNAPSHOT_TABLES) == before

    before = snapshot_tables(conn, SNAPSHOT_TABLES)
    refused = _classify(conn, env, grant_id=grant_id, advance=advance,
                        met="maybe")
    assert is_error(refused), refused
    assert "condition-met" in refused["message"].lower()
    assert snapshot_tables(conn, SNAPSHOT_TABLES) == before


def test_missing_accounts_refused_without_writes(conn, env):
    grant_id = _activate(conn, env)
    advance = _advance_acct(conn, env["company_id"])
    conn.commit()
    before = snapshot_tables(conn, SNAPSHOT_TABLES)
    refused = _classify(conn, env, grant_id=grant_id, advance=None)
    assert is_error(refused), refused
    assert "refundable-advance-account-id" in refused["message"]
    assert snapshot_tables(conn, SNAPSHOT_TABLES) == before


def test_inactive_grant_refused_without_writes(conn, env):
    advance = _advance_acct(conn, env["company_id"])
    conn.commit()
    before = snapshot_tables(conn, SNAPSHOT_TABLES)
    refused = _classify(conn, env, advance=advance)
    assert is_error(refused), refused
    assert "active" in refused["message"]
    assert snapshot_tables(conn, SNAPSHOT_TABLES) == before


def test_action_is_routed_and_parsed(conn, env):
    mod = load_db_query()
    assert "nonprofit-classify-conditional-contribution" in mod.ACTIONS
    parser = mod.build_parser()
    args = parser.parse_args([
        "--action", "nonprofit-classify-conditional-contribution",
        "--company-id", env["company_id"],
        "--grant-id", env["grant_id"],
        "--amount", "500.03",
        "--receipt-date", RECEIPT_DATE,
        "--cash-account-id", env["cash_acct"],
        "--revenue-account-id", env["revenue_acct"],
        "--refundable-advance-account-id", env["cash_acct"],
        "--condition-text", CONDITION,
        "--condition-met", "true",
    ])
    assert args.refundable_advance_account_id == env["cash_acct"]
    assert args.condition_text == CONDITION
    assert args.condition_met == "true"
