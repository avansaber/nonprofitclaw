"""Form 990 preparation worksheet v1 (floor-o062).

The worksheet is a deterministic, read-only summary of recorded company
books for one company fiscal year. These tests prove exact Decimal money
(500.03), fiscal-year and company isolation, optional-table degradation,
malformed-money refusal, stable warnings and ordering, identical repeat
reads, and zero writes.
"""
import uuid

from nonprofit_helpers import (
    call_action, is_error, is_ok, load_db_query, ns,
    seed_company, seed_donation, seed_donor, seed_fiscal_year, seed_fund,
    snapshot_tables,
)

load_db_query()  # puts the module's scripts directory on sys.path

import reports as reports_mod

ACTION = "nonprofit-prepare-form-990"

ALL_TABLES = [
    "company", "customer", "naming_series", "audit_log",
    "account", "gl_entry", "fiscal_year", "cost_center",
    "nonprofitclaw_donor_ext", "nonprofitclaw_donation",
    "nonprofitclaw_fund", "nonprofitclaw_fund_transfer",
    "nonprofitclaw_endowment_appropriation", "nonprofitclaw_grant",
    "nonprofitclaw_grant_expense", "nonprofitclaw_grant_receipt",
    "nonprofitclaw_program", "nonprofitclaw_volunteer",
    "nonprofitclaw_volunteer_shift", "nonprofitclaw_pledge",
    "nonprofitclaw_campaign", "nonprofitclaw_tax_receipt",
]


def _run(conn, company_id, fiscal_year_id):
    return call_action(
        reports_mod.prepare_form_990, conn,
        ns(company_id=company_id, fiscal_year_id=fiscal_year_id),
    )


def _add_donation(conn, company_id, donor_id, amount, day, status="received"):
    did = str(uuid.uuid4())
    conn.execute(
        """INSERT INTO nonprofitclaw_donation
           (id, naming_series, donor_id, donation_date, amount,
            payment_method, status, company_id)
           VALUES (?, 'DON-990', ?, ?, ?, 'check', ?, ?)""",
        (did, donor_id, "2026-%s" % day, amount, status, company_id),
    )
    conn.commit()
    return did


def _add_receipt(conn, company_id, grant_id, amount, day, cash_acct,
                 revenue_acct, status="received"):
    rid = str(uuid.uuid4())
    conn.execute(
        """INSERT INTO nonprofitclaw_grant_receipt
           (id, naming_series, grant_id, receipt_date, amount,
            cash_account_id, credit_account_id, status, company_id)
           VALUES (?, 'GRC-990', ?, ?, ?, ?, ?, ?, ?)""",
        (rid, grant_id, "2026-%s" % day, amount,
         cash_acct, revenue_acct, status, company_id),
    )
    conn.commit()
    return rid


def _add_expense(conn, company_id, grant_id, amount, day,
                 category="program", status="approved"):
    eid = str(uuid.uuid4())
    conn.execute(
        """INSERT INTO nonprofitclaw_grant_expense
           (id, naming_series, grant_id, expense_date, amount,
            category, status, company_id)
           VALUES (?, 'GEXP-990', ?, ?, ?, ?, ?, ?)""",
        (eid, grant_id, "2026-%s" % day, amount, category, status, company_id),
    )
    conn.commit()
    return eid


def _add_gl(conn, account_id, posting_date, debit, credit, cancelled=0):
    gid = str(uuid.uuid4())
    conn.execute(
        """INSERT INTO gl_entry
           (id, posting_date, account_id, debit, credit,
            voucher_type, voucher_id, is_cancelled)
           VALUES (?, ?, ?, ?, ?, 'journal_entry', ?, ?)""",
        (gid, posting_date, account_id, debit, credit, gid, cancelled),
    )
    conn.commit()
    return gid


def test_action_is_routed_with_fiscal_year_flag():
    mod = load_db_query()
    assert ACTION in mod.ACTIONS
    args = mod.build_parser().parse_args([
        "--action", ACTION,
        "--company-id", "c1", "--fiscal-year-id", "f1",
    ])
    assert args.company_id == "c1"
    assert args.fiscal_year_id == "f1"


def test_exact_500_03_contributions(conn, env):
    for day, amount in (("01-10", "0.10"), ("02-10", "0.20"), ("03-10", "499.73")):
        _add_donation(conn, env["company_id"], env["donor_id"], amount, day)
    result = _run(conn, env["company_id"], env["fiscal_year_id"])
    assert is_ok(result), result
    assert result["report"] == "preparation_worksheet"
    assert result["totals"]["contributions"] == "500.03"
    assert result["totals"]["revenue"] == "500.03"
    assert result["counts"]["donations"] == 3
    assert "Review and filing remain outside ERPClaw" in result["notice"]


def test_gl_memo_exact_and_bounded(conn, env):
    _add_gl(conn, env["revenue_acct"], "2026-04-01", "0", "300.02")
    _add_gl(conn, env["revenue_acct"], "2026-05-01", "0", "200.01")
    _add_gl(conn, env["revenue_acct"], "2025-12-31", "0", "999.99")
    _add_gl(conn, env["revenue_acct"], "2026-06-01", "0", "111.11", cancelled=1)
    _add_gl(conn, env["expense_acct"], "2026-07-01", "25.00", "0")
    result = _run(conn, env["company_id"], env["fiscal_year_id"])
    assert is_ok(result), result
    assert result["totals"]["gl_income_total"] == "500.03"
    assert result["totals"]["gl_expense_total"] == "25.00"
    assert result["counts"]["gl_entries"] == 3
    assert result["sources"]["gl_entry"] is True


def test_fiscal_year_isolation(conn, env):
    _add_donation(conn, env["company_id"], env["donor_id"], "100.00", "06-15")
    _add_donation(conn, env["company_id"], env["donor_id"], "999.99", "12-31")
    conn.execute(
        """UPDATE nonprofitclaw_donation SET donation_date = '2025-12-31'
           WHERE amount = '999.99'"""
    )
    _add_donation(conn, env["company_id"], env["donor_id"], "888.88", "06-15")
    conn.execute(
        """UPDATE nonprofitclaw_donation SET donation_date = '2027-01-01'
           WHERE amount = '888.88'"""
    )
    conn.commit()
    result = _run(conn, env["company_id"], env["fiscal_year_id"])
    assert is_ok(result), result
    assert result["totals"]["contributions"] == "100.00"
    assert result["counts"]["donations"] == 1


def test_company_isolation_and_foreign_fiscal_year_refused(conn, env):
    other_company = seed_company(conn)
    other_donor = seed_donor(conn, other_company, "Other Donor")
    other_fy = seed_fiscal_year(conn, other_company)
    _add_donation(conn, other_company, other_donor["donor_id"], "999.99", "06-15")
    _add_donation(conn, env["company_id"], env["donor_id"], "50.00", "06-15")

    own = _run(conn, env["company_id"], env["fiscal_year_id"])
    assert is_ok(own), own
    assert own["totals"]["contributions"] == "50.00"

    foreign = _run(conn, env["company_id"], other_fy)
    assert is_error(foreign)
    assert "does not belong" in foreign["message"]

    missing = _run(conn, env["company_id"], str(uuid.uuid4()))
    assert is_error(missing)
    assert "not found" in missing["message"]


def test_optional_tables_degrade_with_warnings(conn, env):
    _add_donation(conn, env["company_id"], env["donor_id"], "75.00", "06-15")
    conn.execute("DROP TABLE nonprofitclaw_grant_receipt")
    conn.execute("DROP TABLE nonprofitclaw_program")
    conn.commit()
    result = _run(conn, env["company_id"], env["fiscal_year_id"])
    assert is_ok(result), result
    assert result["totals"]["contributions"] == "75.00"
    assert result["totals"]["grants"] == "0.00"
    assert result["sources"]["nonprofitclaw_grant_receipt"] is False
    assert result["sources"]["nonprofitclaw_program"] is False
    assert result["sources"]["nonprofitclaw_donation"] is True
    assert any("nonprofitclaw_grant_receipt" in w for w in result["warnings"])
    assert any("nonprofitclaw_program" in w for w in result["warnings"])


def test_malformed_money_refused_without_writes(conn, env):
    _add_donation(conn, env["company_id"], env["donor_id"], "10.00", "06-15")
    before = snapshot_tables(conn, ALL_TABLES)
    conn.execute(
        """INSERT INTO nonprofitclaw_donation
           (id, naming_series, donor_id, donation_date, amount,
            payment_method, status, company_id)
           VALUES (?, 'DON-990', ?, '2026-06-16', 'MUCH',
                   'check', 'received', ?)""",
        (str(uuid.uuid4()), env["donor_id"], env["company_id"]),
    )
    conn.commit()
    mid = snapshot_tables(conn, ALL_TABLES)
    result = _run(conn, env["company_id"], env["fiscal_year_id"])
    assert is_error(result)
    assert "Malformed" in result["message"]
    after = snapshot_tables(conn, ALL_TABLES)
    assert after == mid
    assert mid != before


def test_expense_buckets_and_net_assets(conn, env):
    _add_receipt(conn, env["company_id"], env["grant_id"], "100.01", "03-01",
                 env["cash_acct"], env["revenue_acct"])
    _add_receipt(conn, env["company_id"], env["grant_id"], "200.02", "04-01",
                 env["cash_acct"], env["revenue_acct"])
    _add_expense(conn, env["company_id"], env["grant_id"], "50.00", "05-01", "program")
    _add_expense(conn, env["company_id"], env["grant_id"], "5.00", "05-02", "travel")
    _add_expense(conn, env["company_id"], env["grant_id"], "10.00", "05-03", "overhead")
    _add_expense(conn, env["company_id"], env["grant_id"], "2.50", "05-04", "other")
    seed_fund(conn, env["company_id"], "Reserve Fund", "unrestricted", "1250.50")
    result = _run(conn, env["company_id"], env["fiscal_year_id"])
    assert is_ok(result), result
    assert result["totals"]["grants"] == "300.03"
    assert result["totals"]["revenue"] == "300.03"
    assert result["totals"]["program_expenses"] == "55.00"
    assert result["totals"]["administrative_expenses"] == "12.50"
    assert result["totals"]["fundraising_expenses"] == "0.00"
    assert result["totals"]["program_service_revenue"] == "0.00"
    assert result["totals"]["expenses"] == "67.50"
    assert result["totals"]["ending_net_assets"] == "1250.50"
    assert any("fundraising_expenses" in w for w in result["warnings"])
    assert any("program_service_revenue" in w for w in result["warnings"])


def test_status_filtering(conn, env):
    _add_donation(conn, env["company_id"], env["donor_id"], "40.00", "06-15")
    _add_donation(conn, env["company_id"], env["donor_id"], "999.99", "06-15", status="refunded")
    _add_receipt(conn, env["company_id"], env["grant_id"], "30.00", "03-01",
                 env["cash_acct"], env["revenue_acct"])
    _add_receipt(conn, env["company_id"], env["grant_id"], "777.77", "03-02",
                 env["cash_acct"], env["revenue_acct"], status="cancelled")
    _add_expense(conn, env["company_id"], env["grant_id"], "20.00", "05-01", "program")
    _add_expense(conn, env["company_id"], env["grant_id"], "555.55", "05-02", "program", status="draft")
    result = _run(conn, env["company_id"], env["fiscal_year_id"])
    assert is_ok(result), result
    assert result["totals"]["contributions"] == "40.00"
    assert result["totals"]["grants"] == "30.00"
    assert result["totals"]["program_expenses"] == "20.00"


def test_repeat_reads_identical_and_sorted_without_writes(conn, env):
    seed_fund(conn, env["company_id"], "Zulu Fund", "unrestricted", "1.00")
    seed_fund(conn, env["company_id"], "Alpha Fund", "unrestricted", "2.00")
    _add_donation(conn, env["company_id"], env["donor_id"], "5.00", "06-20")
    _add_donation(conn, env["company_id"], env["donor_id"], "7.00", "06-10")
    before = snapshot_tables(conn, ALL_TABLES)
    first = _run(conn, env["company_id"], env["fiscal_year_id"])
    second = _run(conn, env["company_id"], env["fiscal_year_id"])
    assert is_ok(first), first
    assert is_ok(second), second
    assert first == second
    assert first["warnings"] == sorted(first["warnings"])
    fund_names = [f["name"] for f in first["details"]["funds"]]
    assert fund_names == sorted(fund_names)
    donation_dates = [d["donation_date"] for d in first["details"]["donations"]]
    assert donation_dates == sorted(donation_dates)
    assert snapshot_tables(conn, ALL_TABLES) == before


def test_missing_ids_refused(conn, env):
    assert is_error(call_action(
        reports_mod.prepare_form_990, conn, ns(company_id=None, fiscal_year_id=None)))
    assert is_error(call_action(
        reports_mod.prepare_form_990, conn,
        ns(company_id=env["company_id"], fiscal_year_id=None)))
    assert is_error(_run(conn, None, env["fiscal_year_id"]))
