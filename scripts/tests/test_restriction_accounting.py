"""Restriction accounting: grants spend received money, expenses reduce the fund.

Grants spend only received money less what is already spent, and an approved
grant expense also comes out of the fund the grant was received into. Money in
a permanently restricted fund is never transferred out, and a fund's type
cannot change while it holds money. An annual summary tax receipt leaves out
donations that already have their own receipt, and a donor gets at most one
annual summary per tax year.
"""
import uuid
from decimal import Decimal

from nonprofit_helpers import (
    call_action, is_error, is_ok, load_db_query, ns, snapshot_tables,
    seed_company, seed_fund, seed_naming_series,
)

load_db_query()

SNAP = [
    "nonprofitclaw_fund",
    "nonprofitclaw_fund_transfer",
    "nonprofitclaw_grant",
    "nonprofitclaw_grant_expense",
    "nonprofitclaw_tax_receipt",
    "gl_entry",
    "audit_log",
]


def _balance(conn, fund_id):
    return conn.execute(
        "SELECT current_balance FROM nonprofitclaw_fund WHERE id = ?",
        (fund_id,)).fetchone()["current_balance"]


def _legs(conn, voucher_id):
    rows = conn.execute(
        "SELECT account_id, debit, credit, voucher_type, posting_date, is_cancelled "
        "FROM gl_entry WHERE voucher_id = ?", (voucher_id,)).fetchall()
    return sorted((r["account_id"], r["debit"], r["credit"], r["voucher_type"],
                   r["posting_date"], r["is_cancelled"]) for r in rows)


def _add_fund(conn, env, name, fund_type):
    import funds as funds_mod
    r = call_action(funds_mod.add_fund, conn, ns(
        company_id=env["company_id"], name=name, fund_type=fund_type,
        description=None, target_amount=None,
        start_date=None, end_date=None))
    assert is_ok(r), r
    return r["id"]


def _add_grant(conn, env, name, amount, fund_id):
    import grants as grants_mod
    r = call_action(grants_mod.add_grant, conn, ns(
        company_id=env["company_id"], name=name, grantor_name="Test Foundation",
        grantor_type="foundation", grant_type="project", amount=amount,
        fund_id=fund_id, start_date="2026-01-01", end_date="2026-12-31",
        reporting_freq="quarterly", notes=None))
    assert is_ok(r), r
    return r["id"]


def _activate(conn, grant_id, amount):
    import grants as grants_mod
    r = call_action(grants_mod.activate_grant, conn, ns(id=grant_id, amount=amount))
    assert is_ok(r), r
    return r


def _add_expense(conn, env, grant_id, amount, expense_date):
    import grants as grants_mod
    r = call_action(grants_mod.add_grant_expense, conn, ns(
        company_id=env["company_id"], grant_id=grant_id, amount=amount,
        category="program", description="Program spend",
        expense_date=expense_date, receipt_reference=None))
    assert is_ok(r), r
    return r["id"]


def _approve_expense(conn, expense_id, expense_acct=None, cash_acct=None, cc_id=None):
    import grants as grants_mod
    return call_action(grants_mod.approve_grant_expense, conn, ns(
        id=expense_id, expense_account_id=expense_acct,
        cash_account_id=cash_acct, cost_center_id=cc_id))


def _grant_row(conn, grant_id):
    row = conn.execute(
        "SELECT received_amount, spent_amount, remaining_amount "
        "FROM nonprofitclaw_grant WHERE id = ?", (grant_id,)).fetchone()
    return (row["received_amount"], row["spent_amount"], row["remaining_amount"])


def _donate(conn, env, amount, donation_date, fund_id=None):
    import donors as donors_mod
    r = call_action(donors_mod.add_donation, conn, ns(
        company_id=env["company_id"], donor_id=env["donor_id"], amount=amount,
        payment_method="check", donation_date=donation_date, reference=None,
        fund_id=fund_id, campaign_id=None, is_recurring=None,
        recurrence_freq=None, notes=None,
        cash_account_id=None, revenue_account_id=None, cost_center_id=None))
    assert is_ok(r), r
    return r["id"]


def _transfer(conn, company_id, from_fund_id, to_fund_id, amount,
              transfer_date="2026-03-01"):
    import funds as funds_mod
    return call_action(funds_mod.add_fund_transfer, conn, ns(
        company_id=company_id, from_fund_id=from_fund_id, to_fund_id=to_fund_id,
        amount=amount, transfer_date=transfer_date, reason="Reallocation",
        approved_by=None))


def test_grant_spends_only_received_money_and_reduces_its_fund(conn, env):
    scholarship = _add_fund(conn, env, "Scholarship Fund", "temporarily_restricted")
    gid = _add_grant(conn, env, "Scholarship Grant", "10000", scholarship)
    _activate(conn, gid, "4000")
    assert _balance(conn, scholarship) == "4000.00"

    e1 = _add_expense(conn, env, gid, "3000.00", "2026-03-05")
    r1 = _approve_expense(conn, e1, env["expense_acct"], env["cash_acct"], env["cc_id"])
    assert is_ok(r1), r1
    assert r1["grant_spent"] == "3000.00"
    assert r1["grant_remaining"] == "1000.00"
    assert _grant_row(conn, gid) == ("4000.00", "3000.00", "1000.00")
    assert _balance(conn, scholarship) == "1000.00"
    assert _legs(conn, e1) == sorted([
        (env["expense_acct"], "3000.00", "0.00", "journal_entry", "2026-03-05", 0),
        (env["cash_acct"], "0.00", "3000.00", "journal_entry", "2026-03-05", 0),
    ])

    e2 = _add_expense(conn, env, gid, "1500.00", "2026-03-06")
    before = snapshot_tables(conn, SNAP)
    refused = _approve_expense(conn, e2)
    assert is_error(refused)
    assert refused["message"] == "Expense amount (1500.00) exceeds grant remaining (1000.00)"
    assert snapshot_tables(conn, SNAP) == before

    e3 = _add_expense(conn, env, gid, "1000.00", "2026-03-07")
    r3 = _approve_expense(conn, e3, env["expense_acct"], env["cash_acct"], env["cc_id"])
    assert is_ok(r3), r3
    assert _grant_row(conn, gid) == ("4000.00", "4000.00", "0.00")
    assert _balance(conn, scholarship) == "0.00"


def test_grant_expense_refused_when_its_fund_is_short(conn, env):
    import funds as funds_mod
    scholarship = _add_fund(conn, env, "Scholarship Fund", "temporarily_restricted")
    gid = _add_grant(conn, env, "Scholarship Grant", "10000", scholarship)
    _activate(conn, gid, "4000")

    t = _transfer(conn, env["company_id"], scholarship, env["fund_id"], "3500.00")
    assert is_ok(t), t
    gl_before = snapshot_tables(conn, ["gl_entry"])
    r = call_action(funds_mod.approve_fund_transfer, conn,
                     ns(id=t["id"], approved_by="Treasurer"))
    assert is_ok(r), r
    assert "gl_entry_ids" not in r
    assert snapshot_tables(conn, ["gl_entry"]) == gl_before
    assert _balance(conn, scholarship) == "500.00"

    e = _add_expense(conn, env, gid, "600.00", "2026-03-05")
    before = snapshot_tables(conn, SNAP)
    refused = _approve_expense(conn, e)
    assert is_error(refused)
    assert refused["message"] == (
        "Expense amount (600.00) exceeds the balance of fund Scholarship Fund (500.00)")
    assert snapshot_tables(conn, SNAP) == before


def test_restricted_to_unrestricted_transfer_posts_nothing(conn, env):
    import funds as funds_mod
    scholarship = _add_fund(conn, env, "Scholarship Fund", "temporarily_restricted")
    _donate(conn, env, "2500.20", "2026-02-15", fund_id=scholarship)
    assert _balance(conn, scholarship) == "2500.20"

    gl_before = snapshot_tables(conn, ["gl_entry"])
    t = _transfer(conn, env["company_id"], scholarship, env["fund_id"], "400.05")
    assert is_ok(t), t
    r = call_action(funds_mod.approve_fund_transfer, conn,
                     ns(id=t["id"], approved_by="Treasurer"))
    assert is_ok(r), r
    row = conn.execute("SELECT status FROM nonprofitclaw_fund_transfer WHERE id = ?",
                       (t["id"],)).fetchone()
    assert row["status"] == "completed"
    assert _balance(conn, scholarship) == "2100.15"
    assert _balance(conn, env["fund_id"]) == "400.05"
    assert "gl_entry_ids" not in r
    assert snapshot_tables(conn, ["gl_entry"]) == gl_before


def test_permanently_restricted_fund_is_refused(conn, env):
    import funds as funds_mod
    from erpclaw_lib.naming import get_next_name
    from erpclaw_lib.query import P, insert_row
    endow = _add_fund(conn, env, "Endowment", "permanently_restricted")
    _donate(conn, env, "1000.00", "2026-02-01", fund_id=endow)
    assert _balance(conn, endow) == "1000.00"

    refused = _transfer(conn, env["company_id"], endow, env["fund_id"], "100.00")
    assert is_error(refused)
    assert refused["message"] == (
        "Fund Endowment is permanently restricted; its balance cannot be transferred")
    assert conn.execute("SELECT COUNT(*) FROM nonprofitclaw_fund_transfer").fetchone()[0] == 0

    tid = str(uuid.uuid4())
    naming = get_next_name(conn, "nonprofitclaw_fund_transfer",
                            company_id=env["company_id"])
    sql, _ = insert_row("nonprofitclaw_fund_transfer", {
        "id": P(), "naming_series": P(), "from_fund_id": P(), "to_fund_id": P(),
        "amount": P(), "transfer_date": P(), "reason": P(), "status": P(),
        "company_id": P(),
    })
    conn.execute(sql, (
        tid, naming, endow, env["fund_id"], "100.00", "2026-03-01",
        "row written before the fix", "draft", env["company_id"],
    ))
    conn.commit()

    before = snapshot_tables(conn, SNAP)
    refused_approve = call_action(funds_mod.approve_fund_transfer, conn,
                                   ns(id=tid, approved_by="Treasurer"))
    assert is_error(refused_approve)
    assert refused_approve["message"] == (
        "Fund Endowment is permanently restricted; its balance cannot be transferred")
    assert snapshot_tables(conn, SNAP) == before

    snap_before_update = snapshot_tables(conn, SNAP)
    upd = call_action(funds_mod.update_fund, conn, ns(
        id=endow, name=None, fund_type="unrestricted", description=None,
        target_amount=None, start_date=None, end_date=None, is_active=None))
    assert is_error(upd)
    assert upd["message"] == (
        "Fund Endowment holds 1000.00; its fund type cannot change while it holds money")
    assert snapshot_tables(conn, SNAP) == snap_before_update


def test_annual_summary_counts_each_donation_once_and_is_issued_once(conn, env):
    import compliance as comp_mod

    def receipt(receipt_type, tax_year, donation_id=None):
        return call_action(comp_mod.generate_tax_receipt, conn, ns(
            company_id=env["company_id"], donor_id=env["donor_id"],
            tax_year=tax_year, receipt_type=receipt_type,
            donation_id=donation_id, sent_method=None))

    d1 = _donate(conn, env, "1500.10", "2026-02-01")
    d2 = _donate(conn, env, "2500.20", "2026-02-15")
    d3 = _donate(conn, env, "100.05", "2026-06-30")
    d4 = _donate(conn, env, "75.00", "2025-12-31")

    s1 = receipt("single", "2026", donation_id=d1)
    assert is_ok(s1), s1

    annual = receipt("annual_summary", "2026")
    assert is_ok(annual), annual
    assert annual["amount"] == "2600.25"

    dup = receipt("annual_summary", "2026")
    assert is_error(dup)
    assert dup["message"] == (
        f"An annual summary receipt already exists for this donor in 2026: {annual['id']}")

    covered = receipt("single", "2026", donation_id=d2)
    assert is_error(covered)
    assert covered["message"] == (
        f"Donation {d2} is already covered by annual summary receipt {annual['id']} for 2026")

    s4 = receipt("single", "2025", donation_id=d4)
    assert is_ok(s4), s4
    empty = receipt("annual_summary", "2025")
    assert is_error(empty)
    assert empty["message"] == (
        "Every tax-deductible donation for this donor in 2025 already has its own receipt")

    assert conn.execute("SELECT COUNT(*) FROM nonprofitclaw_tax_receipt").fetchone()[0] == 3


def test_other_company_permanent_fund_name_does_not_leak(conn, env):
    other = seed_company(conn)
    seed_naming_series(conn, other)
    foreign = seed_fund(conn, other, "Foreign Endowment", "permanently_restricted")
    before = snapshot_tables(conn, SNAP)
    refused = _transfer(conn, env["company_id"], foreign, env["fund_id"], "100.00")
    assert is_error(refused)
    assert refused["message"] == "Both funds must belong to the specified company"
    assert "Foreign Endowment" not in refused["message"]
    assert snapshot_tables(conn, SNAP) == before


def test_grant_expense_refused_on_permanently_restricted_fund(conn, env):
    endow = _add_fund(conn, env, "Endowment", "permanently_restricted")
    gid = _add_grant(conn, env, "Endowment Grant", "5000", endow)
    _activate(conn, gid, "5000")
    assert _balance(conn, endow) == "5000.00"
    e = _add_expense(conn, env, gid, "100.00", "2026-03-05")
    before = snapshot_tables(conn, SNAP)
    refused = _approve_expense(conn, e)
    assert is_error(refused)
    assert refused["message"] == (
        "Fund Endowment is permanently restricted; grant expenses cannot be paid from it")
    assert snapshot_tables(conn, SNAP) == before
    assert _balance(conn, endow) == "5000.00"


def test_grant_expense_refused_on_inactive_grant(conn, env):
    import grants as grants_mod
    from erpclaw_lib.naming import get_next_name
    from erpclaw_lib.query import P, Table, insert_row
    scholarship = _add_fund(conn, env, "Scholarship Fund", "temporarily_restricted")
    gid = _add_grant(conn, env, "Scholarship Grant", "2000", scholarship)
    _activate(conn, gid, "2000")
    closed = call_action(grants_mod.close_grant, conn, ns(id=gid))
    assert is_ok(closed), closed
    assert closed["grant_status"] == "completed"
    eid = str(uuid.uuid4())
    naming = get_next_name(conn, "nonprofitclaw_grant_expense",
                           company_id=env["company_id"])
    sql, _ = insert_row("nonprofitclaw_grant_expense", {
        "id": P(), "naming_series": P(), "grant_id": P(), "expense_date": P(),
        "amount": P(), "category": P(), "description": P(),
        "receipt_reference": P(), "status": P(), "company_id": P(),
    })
    conn.execute(sql, (
        eid, naming, gid, "2026-03-05", "100.00", "program",
        "Row written before the grant was closed", None,
        "draft", env["company_id"],
    ))
    conn.commit()
    before = snapshot_tables(conn, SNAP)
    refused = _approve_expense(conn, eid)
    assert is_error(refused)
    assert refused["message"] == (
        f"Grant {gid} must be 'active' to approve an expense, currently 'completed'")
    assert snapshot_tables(conn, SNAP) == before


def test_fund_balance_reconcile_lists_only_drifted_funds(conn, env):
    import donors as donors_mod
    import funds as funds_mod
    import grants as grants_mod
    from erpclaw_lib.query import P, Table
    alpha = _add_fund(conn, env, "Alpha Fund", "unrestricted")
    beta = _add_fund(conn, env, "Beta Fund", "unrestricted")
    _donate(conn, env, "1000.00", "2026-02-01", fund_id=alpha)
    d50 = _donate(conn, env, "50.00", "2026-02-02", fund_id=alpha)
    refunded = call_action(donors_mod.refund_donation, conn,
                           ns(id=d50, donation_id=None))
    assert is_ok(refunded), refunded
    gid = _add_grant(conn, env, "Alpha Grant", "400.00", alpha)
    _activate(conn, gid, "400.00")
    exp = _add_expense(conn, env, gid, "150.00", "2026-03-05")
    approved = _approve_expense(conn, exp, env["expense_acct"], env["cash_acct"], env["cc_id"])
    assert is_ok(approved), approved
    closed = call_action(grants_mod.close_grant, conn, ns(id=gid))
    assert is_ok(closed), closed
    t = _transfer(conn, env["company_id"], alpha, beta, "100.00")
    assert is_ok(t), t
    tapp = call_action(funds_mod.approve_fund_transfer, conn,
                       ns(id=t["id"], approved_by="Treasurer"))
    assert is_ok(tapp), tapp
    assert _balance(conn, alpha) == "1150.00"
    _donate(conn, env, "300.00", "2026-02-10", fund_id=beta)
    assert _balance(conn, beta) == "400.00"
    from erpclaw_lib.query import Q as _Q
    ft = Table("nonprofitclaw_fund")
    upd = _Q.update(ft).set(ft.current_balance, P()).where(ft.id == P())
    conn.execute(upd.get_sql(), ("450.00", beta))
    conn.commit()
    assert _balance(conn, beta) == "450.00"
    action = load_db_query().ACTIONS["nonprofit-fund-balance-reconcile"]
    snap_before = snapshot_tables(conn, SNAP + ["nonprofitclaw_donation"])
    result = call_action(action, conn, ns(company_id=env["company_id"]))
    assert is_ok(result), result
    assert result["mismatched"] == 1
    assert len(result["funds"]) == 1
    entry = result["funds"][0]
    assert entry["id"] == beta
    assert entry["name"] == "Beta Fund"
    assert entry["stored_balance"] == "450.00"
    assert entry["expected_balance"] == "400.00"
    assert entry["difference"] == "50.00"
    assert env["fund_id"] not in {f["id"] for f in result["funds"]}
    assert alpha not in {f["id"] for f in result["funds"]}
    assert snapshot_tables(conn, SNAP + ["nonprofitclaw_donation"]) == snap_before
    missing = call_action(action, conn, ns(company_id=None))
    assert is_error(missing)
    assert missing["message"] == "--company-id is required"


# ------------------------------------------------------------------
# Release from donor restriction v1 (floor-o012)
#
# Stored vocabulary stays `temporarily_restricted` (with-donor-restrictions)
# and `unrestricted` (without-donor-restrictions). All calls go through the
# real action map so the router registration is covered too.
# ------------------------------------------------------------------

def _release(conn, company_id, from_fund_id, to_fund_id, amount,
             transfer_date="2026-03-01", reason="Purpose met",
             approved_by="Treasurer"):
    action = load_db_query().ACTIONS["nonprofit-release-restriction"]
    return call_action(action, conn, ns(
        company_id=company_id, from_fund_id=from_fund_id,
        to_fund_id=to_fund_id, amount=amount,
        transfer_date=transfer_date, reason=reason,
        approved_by=approved_by))


def test_release_restriction_moves_exact_amounts_and_conserves(conn, env):
    restricted = _add_fund(conn, env, "Building Pledges", "temporarily_restricted")
    general = _add_fund(conn, env, "General Operating", "unrestricted")
    _donate(conn, env, "100.10", "2026-02-01", fund_id=restricted)
    assert _balance(conn, restricted) == "100.10"
    assert Decimal(_balance(conn, general)) == Decimal("0")
    before_transfers = conn.execute(
        "SELECT COUNT(*) FROM nonprofitclaw_fund_transfer").fetchone()[0]

    result = _release(conn, env["company_id"], restricted, general, "60.05")
    assert is_ok(result), result
    assert result["document_status"] == "completed"
    assert result["transfer_status"] == "completed"
    assert result["released_amount"] == "60.05"
    assert result["amount"] == "60.05"
    assert result["from_fund_id"] == restricted
    assert result["to_fund_id"] == general

    stored_src = _balance(conn, restricted)
    stored_dst = _balance(conn, general)
    assert stored_src == "40.05"
    assert stored_dst == "60.05"
    assert result["source_ending_balance"] == stored_src
    assert result["destination_ending_balance"] == stored_dst
    assert result["source_balance"] == stored_src
    assert result["destination_balance"] == stored_dst

    row = conn.execute(
        "SELECT amount, status, from_fund_id, to_fund_id "
        "FROM nonprofitclaw_fund_transfer WHERE id = ?",
        (result["id"],)).fetchone()
    assert row["amount"] == "60.05"
    assert row["status"] == "completed"
    assert row["from_fund_id"] == restricted
    assert row["to_fund_id"] == general
    assert conn.execute(
        "SELECT COUNT(*) FROM nonprofitclaw_fund_transfer").fetchone()[0] == before_transfers + 1

    assert Decimal(stored_src) + Decimal(stored_dst) == Decimal("100.10")


def test_release_restriction_refuses_permanently_restricted_source(conn, env):
    endow = _add_fund(conn, env, "Endowment", "permanently_restricted")
    _donate(conn, env, "1000.00", "2026-02-01", fund_id=endow)
    assert _balance(conn, endow) == "1000.00"
    before = snapshot_tables(conn, SNAP)
    refused = _release(conn, env["company_id"], endow, env["fund_id"], "100.00")
    assert is_error(refused)
    assert snapshot_tables(conn, SNAP) == before
    assert _balance(conn, endow) == "1000.00"


def test_release_restriction_refuses_restricted_destination(conn, env):
    from nonprofit_helpers import seed_company, seed_naming_series, seed_fund
    src = _add_fund(conn, env, "Grant Fund", "temporarily_restricted")
    _donate(conn, env, "200.00", "2026-02-01", fund_id=src)
    dst = _add_fund(conn, env, "Other Restricted", "temporarily_restricted")
    before = snapshot_tables(conn, SNAP)
    refused = _release(conn, env["company_id"], src, dst, "50.00")
    assert is_error(refused)
    assert snapshot_tables(conn, SNAP) == before
    assert _balance(conn, src) == "200.00"
    assert Decimal(_balance(conn, dst)) == Decimal("0")

    other = seed_company(conn)
    seed_naming_series(conn, other)
    foreign = seed_fund(conn, other, "Foreign General", "unrestricted")
    before_foreign = snapshot_tables(conn, SNAP)
    refused_foreign = _release(conn, env["company_id"], src, foreign, "50.00")
    assert is_error(refused_foreign)
    assert refused_foreign["message"] == "Both funds must belong to the specified company"
    assert snapshot_tables(conn, SNAP) == before_foreign
    assert _balance(conn, src) == "200.00"


def test_release_restriction_refuses_insufficient_and_nonfinite(conn, env):
    src = _add_fund(conn, env, "Program Fund", "temporarily_restricted")
    _donate(conn, env, "100.10", "2026-02-01", fund_id=src)
    dst = _add_fund(conn, env, "Operating", "unrestricted")

    before = snapshot_tables(conn, SNAP)
    refused = _release(conn, env["company_id"], src, dst, "100.11")
    assert is_error(refused)
    assert snapshot_tables(conn, SNAP) == before

    for bad in ("NaN", "Infinity", "-Infinity"):
        snap = snapshot_tables(conn, SNAP)
        refused_bad = _release(conn, env["company_id"], src, dst, bad)
        assert is_error(refused_bad), bad
        assert snapshot_tables(conn, SNAP) == snap

    assert _balance(conn, src) == "100.10"
    assert Decimal(_balance(conn, dst)) == Decimal("0")
    assert conn.execute(
        "SELECT COUNT(*) FROM nonprofitclaw_fund_transfer").fetchone()[0] == 0
