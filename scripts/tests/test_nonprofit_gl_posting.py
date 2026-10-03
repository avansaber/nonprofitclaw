"""Donations and approved grant expenses post their ledger, or are refused.

add-donation and approve-grant-expense posted under voucher types the GL
registry does not hold ("donation", "grant_expense") and swallowed the refusal,
so a donation or an approved expense recorded with accounts supplied left the
ledger empty, and refund-donation never reversed anything. They now post under
the registered journal_entry type, and a posting failure rolls the action back
and says why.
"""
from decimal import Decimal

from nonprofit_helpers import call_action, is_error, is_ok, load_db_query, ns, seed_grant

load_db_query()  # puts the module's scripts directory on sys.path, as the sibling files do


def _gl(conn, voucher_id):
    return conn.execute(
        "SELECT account_id, debit, credit FROM gl_entry "
        "WHERE voucher_type = 'journal_entry' AND voucher_id = ?",
        (voucher_id,)).fetchall()


def _donate(conn, env, **accounts):
    import donors as donors_mod
    args = dict(company_id=env["company_id"], donor_id=env["donor_id"],
                amount="250.00", payment_method="check", donation_date="2026-03-01",
                reference="CHK-GL", fund_id=None, campaign_id=None,
                is_recurring=None, recurrence_freq=None, notes=None,
                cash_account_id=None, revenue_account_id=None,
                cost_center_id=env["cc_id"])
    args.update(accounts)
    return call_action(donors_mod.add_donation, conn, ns(**args))


def test_donation_with_accounts_posts_a_balanced_entry(conn, env):
    r = _donate(conn, env, cash_account_id=env["cash_acct"],
                revenue_account_id=env["revenue_acct"])
    assert is_ok(r), r
    rows = _gl(conn, r["id"])
    by_account = {x["account_id"]: (Decimal(x["debit"]), Decimal(x["credit"])) for x in rows}
    assert by_account == {env["cash_acct"]: (Decimal("250.00"), Decimal("0")),
                          env["revenue_acct"]: (Decimal("0"), Decimal("250.00"))}


def test_a_donation_posting_failure_refuses_and_records_nothing(conn, env):
    r = _donate(conn, env, cash_account_id=env["cash_acct"],
                revenue_account_id="no-such-account")
    assert is_error(r), r
    assert "GL posting failed" in (r.get("message", "") + r.get("error", ""))
    assert conn.execute("SELECT COUNT(*) FROM nonprofitclaw_donation WHERE reference = ?",
                        ("CHK-GL",)).fetchone()[0] == 0


def test_refund_reverses_the_donation_entry(conn, env):
    import donors as donors_mod
    r = _donate(conn, env, cash_account_id=env["cash_acct"],
                revenue_account_id=env["revenue_acct"])
    assert is_ok(r), r
    refund = call_action(donors_mod.refund_donation, conn, ns(id=r["id"], donation_id=None))
    assert is_ok(refund), refund
    rows = _gl(conn, r["id"])
    assert len(rows) == 4
    net = {}
    for x in rows:
        d, c = net.get(x["account_id"], (Decimal("0"), Decimal("0")))
        net[x["account_id"]] = (d + Decimal(x["debit"]), c + Decimal(x["credit"]))
    assert all(d == c for d, c in net.values())


def test_approved_grant_expense_with_accounts_posts_a_balanced_entry(conn, env):
    import grants as grants_mod
    gid = seed_grant(conn, env["company_id"], "GL Grant", amount="10000", status="active")
    added = call_action(grants_mod.add_grant_expense, conn, ns(
        company_id=env["company_id"], grant_id=gid, amount="2000.00",
        category="personnel", description="Staff", expense_date="2026-03-01",
        receipt_reference=None))
    assert is_ok(added), added
    r = call_action(grants_mod.approve_grant_expense, conn, ns(
        id=added["id"], expense_account_id=env["expense_acct"],
        cash_account_id=env["cash_acct"], cost_center_id=env["cc_id"]))
    assert is_ok(r), r
    rows = _gl(conn, added["id"])
    by_account = {x["account_id"]: (Decimal(x["debit"]), Decimal(x["credit"])) for x in rows}
    assert by_account == {env["expense_acct"]: (Decimal("2000.00"), Decimal("0")),
                          env["cash_acct"]: (Decimal("0"), Decimal("2000.00"))}
