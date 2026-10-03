"""Fund balances, fund transfers, grant expenses and tax receipts, read back.

Drives donations, fund transfers, grant activation and expenses, and tax
receipts through the real actions with fixed dates, then reads the fund, grant,
receipt and gl_entry rows back and compares every amount as an exact string.

A fund's current_balance is money kept as TEXT. It must be written as an exact
two-place Decimal string ("1500.10"), never as the shortest form of a floating
point result ("1500.1"), on every path that moves it: a donation into the fund,
its refund, both sides of an approved transfer, and a grant received into it.
"""
from nonprofit_helpers import (
    call_action, is_error, is_ok, load_db_query, ns,
    seed_company, seed_donor, seed_fiscal_year, seed_fund, seed_naming_series,
)

load_db_query()  # puts the module's scripts directory on sys.path, as the sibling files do


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

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


def _add_fund(conn, env, name, fund_type, target_amount=None):
    import funds as funds_mod
    r = call_action(funds_mod.add_fund, conn, ns(
        company_id=env["company_id"], name=name, fund_type=fund_type,
        description=None, target_amount=target_amount,
        start_date=None, end_date=None))
    assert is_ok(r), r
    return r["id"]


def _donate(conn, env, amount, donation_date, fund_id=None, reference=None):
    import donors as donors_mod
    r = call_action(donors_mod.add_donation, conn, ns(
        company_id=env["company_id"], donor_id=env["donor_id"], amount=amount,
        payment_method="check", donation_date=donation_date, reference=reference,
        fund_id=fund_id, campaign_id=None, is_recurring=None,
        recurrence_freq=None, notes=None,
        cash_account_id=env["cash_acct"], revenue_account_id=env["revenue_acct"],
        cost_center_id=env["cc_id"]))
    assert is_ok(r), r
    return r["id"]


def _transfer(conn, company_id, from_fund_id, to_fund_id, amount,
              transfer_date="2026-03-01"):
    import funds as funds_mod
    return call_action(funds_mod.add_fund_transfer, conn, ns(
        company_id=company_id, from_fund_id=from_fund_id, to_fund_id=to_fund_id,
        amount=amount, transfer_date=transfer_date, reason="Reallocation",
        approved_by=None))


def _other_company(conn):
    cid = seed_company(conn, "Second Nonprofit", "SNP")
    seed_naming_series(conn, cid)
    donor = seed_donor(conn, cid, "Other Donor")
    fund_id = seed_fund(conn, cid, "Other Fund", "unrestricted", "900.00")
    return {"company_id": cid, "donor_id": donor["donor_id"], "fund_id": fund_id}


# ─────────────────────────────────────────────────────────────────────────────
# Donations, transfers and the fund balance report
# ─────────────────────────────────────────────────────────────────────────────

def test_donations_and_an_approved_transfer_move_exact_fund_balances(conn, env):
    import funds as funds_mod
    general = env["fund_id"]
    scholarship = _add_fund(conn, env, "Scholarship Fund", "temporarily_restricted",
                            target_amount="10000")
    dormant = _add_fund(conn, env, "Dormant Fund", "unrestricted")

    d1 = _donate(conn, env, "1500.10", "2026-02-01", fund_id=general)
    d2 = _donate(conn, env, "2500.20", "2026-02-15", fund_id=scholarship)
    _donate(conn, env, "100.00", "2026-02-20", fund_id=dormant)

    assert _balance(conn, general) == "1500.10"
    assert _balance(conn, scholarship) == "2500.20"
    assert _balance(conn, dormant) == "100.00"

    # each donation posts DR cash / CR contribution revenue under journal_entry
    assert _legs(conn, d1) == sorted([
        (env["cash_acct"], "1500.10", "0.00", "journal_entry", "2026-02-01", 0),
        (env["revenue_acct"], "0.00", "1500.10", "journal_entry", "2026-02-01", 0),
    ])
    assert _legs(conn, d2) == sorted([
        (env["cash_acct"], "2500.20", "0.00", "journal_entry", "2026-02-15", 0),
        (env["revenue_acct"], "0.00", "2500.20", "journal_entry", "2026-02-15", 0),
    ])

    # a draft transfer moves nothing
    t = _transfer(conn, env["company_id"], general, scholarship, "400.05")
    assert is_ok(t), t
    row = conn.execute("SELECT * FROM nonprofitclaw_fund_transfer WHERE id = ?",
                       (t["id"],)).fetchone()
    assert (row["amount"], row["status"], row["transfer_date"], row["approved_by"]) == (
        "400.05", "draft", "2026-03-01", None)
    assert row["naming_series"].startswith("FT-") and row["naming_series"].endswith("-00001")
    assert _balance(conn, general) == "1500.10"
    assert _balance(conn, scholarship) == "2500.20"

    r = call_action(funds_mod.approve_fund_transfer, conn, ns(id=t["id"], approved_by="Treasurer"))
    assert is_ok(r), r
    assert r["amount"] == "400.05"
    row = conn.execute("SELECT status, approved_by FROM nonprofitclaw_fund_transfer WHERE id = ?",
                       (t["id"],)).fetchone()
    assert (row["status"], row["approved_by"]) == ("completed", "Treasurer")
    assert _balance(conn, general) == "1100.05"
    assert _balance(conn, scholarship) == "2900.25"

    # an approved transfer cannot be approved a second time
    again = call_action(funds_mod.approve_fund_transfer, conn, ns(id=t["id"], approved_by="Treasurer"))
    assert is_error(again)
    assert again["message"] == "Transfer is in 'completed' status, can only approve 'draft' transfers"
    assert _balance(conn, general) == "1100.05"
    assert _balance(conn, scholarship) == "2900.25"

    # an inactive fund drops out of the report
    upd = call_action(funds_mod.update_fund, conn, ns(
        id=dormant, name=None, fund_type=None, description=None, target_amount=None,
        start_date=None, end_date=None, is_active="0"))
    assert is_ok(upd), upd

    other = _other_company(conn)
    rep = call_action(funds_mod.fund_balance_report, conn, ns(company_id=env["company_id"]))
    assert is_ok(rep), rep
    assert rep["fund_count"] == 2
    assert rep["total_balance"] == "4000.30"
    assert [(f["name"], f["fund_type"], f["current_balance"]) for f in rep["funds"]] == [
        ("General Fund", "unrestricted", "1100.05"),
        ("Scholarship Fund", "temporarily_restricted", "2900.25"),
    ]
    assert "percent_of_target" not in rep["funds"][0]
    assert rep["funds"][1]["target_amount"] == "10000.00"
    assert rep["funds"][1]["percent_of_target"] == "29.00"

    rep_b = call_action(funds_mod.fund_balance_report, conn, ns(company_id=other["company_id"]))
    assert (rep_b["fund_count"], rep_b["total_balance"]) == (1, "900.00")

    # listing the transfer
    lst = call_action(funds_mod.list_fund_transfers, conn, ns(
        company_id=env["company_id"], status=None, fund_id=None, limit=None, offset=None))
    assert is_ok(lst), lst
    assert lst["total"] == 1
    (item,) = lst["fund_transfers"]
    assert (item["from_fund_name"], item["to_fund_name"], item["amount"],
            item["transfer_date"], item["approved_by"], item["status"]) == (
        "General Fund", "Scholarship Fund", "400.05", "2026-03-01", "Treasurer", "completed")

    by_fund = call_action(funds_mod.list_fund_transfers, conn, ns(
        company_id=env["company_id"], status=None, fund_id=scholarship, limit=None, offset=None))
    assert [x["id"] for x in by_fund["fund_transfers"]] == [t["id"]]
    drafts = call_action(funds_mod.list_fund_transfers, conn, ns(
        company_id=env["company_id"], status="draft", fund_id=None, limit=None, offset=None))
    assert (drafts["total"], drafts["fund_transfers"]) == (0, [])
    unrelated = call_action(funds_mod.list_fund_transfers, conn, ns(
        company_id=env["company_id"], status=None, fund_id=dormant, limit=None, offset=None))
    assert unrelated["total"] == 0
    other_list = call_action(funds_mod.list_fund_transfers, conn, ns(
        company_id=other["company_id"], status=None, fund_id=None, limit=None, offset=None))
    assert (other_list["total"], other_list["fund_transfers"]) == (0, [])


def test_a_transfer_larger_than_the_source_balance_is_refused(conn, env):
    import funds as funds_mod
    general = env["fund_id"]
    target = _add_fund(conn, env, "Building Fund", "temporarily_restricted")
    _donate(conn, env, "300.00", "2026-02-01", fund_id=general)

    t = _transfer(conn, env["company_id"], general, target, "300.01")
    assert is_ok(t), t
    r = call_action(funds_mod.approve_fund_transfer, conn, ns(id=t["id"], approved_by="Treasurer"))
    assert is_error(r)
    assert r["message"] == (
        "Insufficient balance in source fund. Available: 300.00, Required: 300.01")
    status = conn.execute("SELECT status FROM nonprofitclaw_fund_transfer WHERE id = ?",
                          (t["id"],)).fetchone()["status"]
    assert status == "draft"
    assert _balance(conn, general) == "300.00"
    assert _balance(conn, target) == "0"

    # the whole balance may move
    exact = _transfer(conn, env["company_id"], general, target, "300.00")
    assert is_ok(call_action(funds_mod.approve_fund_transfer, conn,
                             ns(id=exact["id"], approved_by="Treasurer")))
    assert _balance(conn, general) == "0.00"
    assert _balance(conn, target) == "300.00"


def test_fund_transfer_refusals_write_nothing(conn, env):
    general = env["fund_id"]
    target = _add_fund(conn, env, "Building Fund", "temporarily_restricted")
    other = _other_company(conn)

    same = _transfer(conn, env["company_id"], general, general, "10.00")
    assert is_error(same)
    assert same["message"] == "Source and destination fund must be different"

    zero = _transfer(conn, env["company_id"], general, target, "0")
    assert is_error(zero)
    assert zero["message"] == "Amount must be positive"

    cross = _transfer(conn, env["company_id"], general, other["fund_id"], "10.00")
    assert is_error(cross)
    assert cross["message"] == "Both funds must belong to the specified company"

    assert conn.execute("SELECT COUNT(*) FROM nonprofitclaw_fund_transfer").fetchone()[0] == 0
    assert _balance(conn, other["fund_id"]) == "900.00"


def test_refund_takes_the_donation_back_out_of_its_fund(conn, env):
    import donors as donors_mod
    general = env["fund_id"]
    _donate(conn, env, "80.40", "2026-01-10", fund_id=general)
    d = _donate(conn, env, "250.00", "2026-02-01", fund_id=general)
    assert _balance(conn, general) == "330.40"

    r = call_action(donors_mod.refund_donation, conn, ns(id=d, donation_id=None))
    assert is_ok(r), r
    assert _balance(conn, general) == "80.40"
    status = conn.execute("SELECT status FROM nonprofitclaw_donation WHERE id = ?",
                          (d,)).fetchone()["status"]
    assert status == "refunded"
    assert _legs(conn, d) == sorted([
        (env["cash_acct"], "250.00", "0.00", "journal_entry", "2026-02-01", 1),
        (env["revenue_acct"], "0.00", "250.00", "journal_entry", "2026-02-01", 1),
        (env["cash_acct"], "0.00", "250.00", "journal_entry", "2026-02-01", 1),
        (env["revenue_acct"], "250.00", "0.00", "journal_entry", "2026-02-01", 1),
    ])


# ─────────────────────────────────────────────────────────────────────────────
# Grants and grant expenses
# ─────────────────────────────────────────────────────────────────────────────

def test_grant_received_into_a_fund_and_approved_expenses(conn, env):
    import grants as grants_mod
    scholarship = _add_fund(conn, env, "Scholarship Fund", "temporarily_restricted")
    _donate(conn, env, "0.10", "2026-01-05", fund_id=scholarship)

    g = call_action(grants_mod.add_grant, conn, ns(
        company_id=env["company_id"], name="Literacy Grant", grantor_name="City Trust",
        grantor_type="government", grant_type="project", amount="12000",
        fund_id=scholarship, start_date="2026-01-01", end_date="2026-12-31",
        reporting_freq=None, notes=None))
    assert is_ok(g), g
    act = call_action(grants_mod.activate_grant, conn, ns(id=g["id"], amount=None))
    assert is_ok(act), act
    assert _balance(conn, scholarship) == "12000.10"

    def add_expense(amount, category, expense_date, company_id=None, grant_id=None):
        return call_action(grants_mod.add_grant_expense, conn, ns(
            company_id=company_id or env["company_id"], grant_id=grant_id or g["id"],
            amount=amount, category=category, description=f"{category} spend",
            expense_date=expense_date, receipt_reference=None))

    e1 = add_expense("2000.00", "personnel", "2026-03-05")
    e2 = add_expense("750.25", "supplies", "2026-04-10")
    assert is_ok(e1) and is_ok(e2)
    assert e1["naming_series"].startswith("NGE-") and e1["naming_series"].endswith("-00001")

    r = call_action(grants_mod.approve_grant_expense, conn, ns(
        id=e1["id"], expense_account_id=env["expense_acct"],
        cash_account_id=env["cash_acct"], cost_center_id=env["cc_id"]))
    assert is_ok(r), r
    assert (r["grant_spent"], r["grant_remaining"]) == ("2000.00", "10000.00")
    grant = conn.execute(
        "SELECT received_amount, spent_amount, remaining_amount, status "
        "FROM nonprofitclaw_grant WHERE id = ?", (g["id"],)).fetchone()
    assert tuple(grant) == ("12000.00", "2000.00", "10000.00", "active")
    assert _legs(conn, e1["id"]) == sorted([
        (env["expense_acct"], "2000.00", "0.00", "journal_entry", "2026-03-05", 0),
        (env["cash_acct"], "0.00", "2000.00", "journal_entry", "2026-03-05", 0),
    ])
    assert _legs(conn, e2["id"]) == []

    # an expense larger than what remains is refused and nothing moves
    big = add_expense("10000.01", "equipment", "2026-05-01")
    assert is_ok(big), big
    refused = call_action(grants_mod.approve_grant_expense, conn, ns(
        id=big["id"], expense_account_id=env["expense_acct"],
        cash_account_id=env["cash_acct"], cost_center_id=env["cc_id"]))
    assert is_error(refused)
    assert refused["message"] == "Expense amount (10000.01) exceeds grant remaining (10000.00)"
    assert conn.execute("SELECT status FROM nonprofitclaw_grant_expense WHERE id = ?",
                        (big["id"],)).fetchone()["status"] == "draft"
    assert _legs(conn, big["id"]) == []
    grant = conn.execute("SELECT spent_amount, remaining_amount FROM nonprofitclaw_grant "
                         "WHERE id = ?", (g["id"],)).fetchone()
    assert tuple(grant) == ("2000.00", "10000.00")

    # approving an approved expense again is refused
    twice = call_action(grants_mod.approve_grant_expense, conn, ns(
        id=e1["id"], expense_account_id=env["expense_acct"],
        cash_account_id=env["cash_acct"], cost_center_id=env["cc_id"]))
    assert is_error(twice)
    assert twice["message"] == (
        "Expense must be in 'draft' or 'submitted' status to approve, currently 'approved'")
    assert len(_legs(conn, e1["id"])) == 2

    # another company cannot book against this grant
    other = _other_company(conn)
    cross = add_expense("5.00", "other", "2026-03-06", company_id=other["company_id"])
    assert is_error(cross)
    assert cross["message"] == "Grant does not belong to this company"
    assert conn.execute("SELECT COUNT(*) FROM nonprofitclaw_grant_expense").fetchone()[0] == 3

    lst = call_action(grants_mod.list_grant_expenses, conn, ns(
        company_id=env["company_id"], grant_id=g["id"], status=None, category=None,
        from_date=None, to_date=None, limit=None, offset=None))
    assert is_ok(lst), lst
    assert lst["total"] == 3
    assert [(x["expense_date"], x["amount"], x["category"], x["status"], x["grant_name"])
            for x in lst["grant_expenses"]] == [
        ("2026-05-01", "10000.01", "equipment", "draft", "Literacy Grant"),
        ("2026-04-10", "750.25", "supplies", "draft", "Literacy Grant"),
        ("2026-03-05", "2000.00", "personnel", "approved", "Literacy Grant"),
    ]
    approved = call_action(grants_mod.list_grant_expenses, conn, ns(
        company_id=env["company_id"], grant_id=None, status="approved", category=None,
        from_date=None, to_date=None, limit=None, offset=None))
    assert [(x["id"], x["amount"]) for x in approved["grant_expenses"]] == [(e1["id"], "2000.00")]
    april = call_action(grants_mod.list_grant_expenses, conn, ns(
        company_id=env["company_id"], grant_id=None, status=None, category=None,
        from_date="2026-04-01", to_date="2026-04-30", limit=None, offset=None))
    assert [(x["id"], x["amount"]) for x in april["grant_expenses"]] == [(e2["id"], "750.25")]
    other_list = call_action(grants_mod.list_grant_expenses, conn, ns(
        company_id=other["company_id"], grant_id=None, status=None, category=None,
        from_date=None, to_date=None, limit=None, offset=None))
    assert (other_list["total"], other_list["grant_expenses"]) == (0, [])


# ─────────────────────────────────────────────────────────────────────────────
# Tax receipts
# ─────────────────────────────────────────────────────────────────────────────

def test_tax_receipts_amounts_numbering_refusals_and_listing(conn, env):
    import compliance as comp_mod
    import donors as donors_mod
    general = env["fund_id"]
    seed_fiscal_year(conn, env["company_id"], start="2025-01-01", end="2025-12-31")
    d1 = _donate(conn, env, "1500.10", "2026-02-01", fund_id=general)
    _donate(conn, env, "2500.20", "2026-02-15")
    _donate(conn, env, "99.99", "2025-12-31")
    refunded = _donate(conn, env, "40.00", "2026-05-01")
    assert is_ok(call_action(donors_mod.refund_donation, conn, ns(id=refunded, donation_id=None)))

    def receipt(receipt_type, tax_year, donation_id=None, sent_method=None,
                company_id=None, donor_id=None):
        return call_action(comp_mod.generate_tax_receipt, conn, ns(
            company_id=company_id or env["company_id"], donor_id=donor_id or env["donor_id"],
            tax_year=tax_year, receipt_type=receipt_type, donation_id=donation_id,
            sent_method=sent_method))

    single = receipt("single", "2026", donation_id=d1, sent_method="email")
    assert is_ok(single), single
    assert (single["amount"], single["donor_name"]) == ("1500.10", "Alice Benefactor")
    row = conn.execute("SELECT * FROM nonprofitclaw_tax_receipt WHERE id = ?",
                       (single["id"],)).fetchone()
    assert (row["amount"], row["donation_id"], row["tax_year"], row["receipt_type"],
            row["sent_method"]) == ("1500.10", d1, "2026", "single", "email")
    assert row["naming_series"].startswith("NTR-") and row["naming_series"].endswith("-00001")
    assert conn.execute("SELECT receipt_sent FROM nonprofitclaw_donation WHERE id = ?",
                        (d1,)).fetchone()["receipt_sent"] == 1

    dup = receipt("single", "2026", donation_id=d1)
    assert is_error(dup)
    assert dup["message"] == f"Tax receipt already exists for this donation: {single['id']}"

    ref = receipt("single", "2026", donation_id=refunded)
    assert is_error(ref)
    assert ref["message"] == "Cannot issue receipt for 'refunded' donation"
    assert conn.execute("SELECT receipt_sent FROM nonprofitclaw_donation WHERE id = ?",
                        (refunded,)).fetchone()["receipt_sent"] == 0

    # the annual summary sums the year's deductible donations and skips the refund
    annual = receipt("annual_summary", "2026", sent_method="mail")
    assert is_ok(annual), annual
    assert annual["amount"] == "2500.20"
    row = conn.execute("SELECT amount, donation_id, naming_series FROM nonprofitclaw_tax_receipt "
                       "WHERE id = ?", (annual["id"],)).fetchone()
    assert (row["amount"], row["donation_id"]) == ("2500.20", None)
    assert row["naming_series"].endswith("-00002")
    prior = receipt("annual_summary", "2025")
    assert is_ok(prior), prior
    assert prior["amount"] == "99.99"

    empty = receipt("annual_summary", "2024")
    assert is_error(empty)
    assert empty["message"] == "No tax-deductible donations found for donor in 2024"

    other = _other_company(conn)
    cross = receipt("annual_summary", "2026", company_id=other["company_id"])
    assert is_error(cross)
    assert cross["message"] == "Donor does not belong to this company"
    assert conn.execute("SELECT COUNT(*) FROM nonprofitclaw_tax_receipt").fetchone()[0] == 3

    def listed(**filters):
        args = dict(company_id=env["company_id"], donor_id=None, tax_year=None,
                    receipt_type=None, limit=None, offset=None)
        args.update(filters)
        r = call_action(comp_mod.list_tax_receipts, conn, ns(**args))
        assert is_ok(r), r
        rows = sorted(r["tax_receipts"], key=lambda x: x["naming_series"])
        return r["total"], [(x["naming_series"][-5:], x["receipt_type"], x["tax_year"],
                             x["amount"], x["donor_name"]) for x in rows]

    assert listed() == (3, [
        ("00001", "single", "2026", "1500.10", "Alice Benefactor"),
        ("00002", "annual_summary", "2026", "2500.20", "Alice Benefactor"),
        ("00003", "annual_summary", "2025", "99.99", "Alice Benefactor"),
    ])
    assert listed(tax_year="2026")[0] == 2
    assert listed(receipt_type="single") == (1, [
        ("00001", "single", "2026", "1500.10", "Alice Benefactor")])
    assert listed(donor_id=other["donor_id"]) == (0, [])
    assert listed(company_id=other["company_id"]) == (0, [])
