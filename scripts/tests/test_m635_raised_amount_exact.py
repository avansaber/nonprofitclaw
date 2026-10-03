"""Campaign raised_amount must be accumulated as exact Decimal strings (m635).

Binary-float SQL arithmetic (CAST(x AS NUMERIC) +/- ?) drifts money totals,
so each total update now reads the stored TEXT value on the same connection,
adds/subtracts in Python with Decimal, and writes the exact two-place string.
These tests drive the three affected actions with amounts whose float sum
drifts, then read the stored total back on a fresh connection and compare the
exact string computed by hand with Decimal (never float).
"""
from decimal import Decimal, ROUND_HALF_UP

from nonprofit_helpers import (
    call_action, get_conn, is_ok, load_db_query, ns, seed_campaign,
)

load_db_query()  # puts the module's scripts directory on sys.path

# Amounts whose binary-float sum drifts; the large one is near 1e13 with cents.
AMOUNTS = ["0.10", "0.20", "1000.10", "2000.20", "9000000000000.10"]
# Hand-computed exact total: 0.10+0.20+1000.10+2000.20+9000000000000.10
EXPECTED_TOTAL = "9000000003000.70"
# After refunding the 1000.10 donation: 9000000003000.70-1000.10
EXPECTED_AFTER_REFUND = "9000000002000.60"


def _raised_on_fresh_conn(db_path, campaign_id):
    fresh = get_conn(db_path)
    try:
        row = fresh.execute(
            "SELECT raised_amount FROM nonprofitclaw_campaign WHERE id = ?",
            (campaign_id,),
        ).fetchone()
        return row["raised_amount"]
    finally:
        fresh.close()


def _donate(donors_mod, conn, env, campaign_id, amount, day):
    result = call_action(donors_mod.add_donation, conn, ns(
        company_id=env["company_id"],
        donor_id=env["donor_id"],
        amount=amount,
        payment_method="check",
        donation_date="2026-03-%02d" % day,
        reference=None,
        fund_id=None,
        campaign_id=campaign_id,
        is_recurring=None,
        recurrence_freq=None,
        notes=None,
        cash_account_id=None,
        revenue_account_id=None,
        cost_center_id=None,
    ))
    assert is_ok(result), result
    return result["id"]


def test_add_donation_accumulates_exact_campaign_total(conn, db_path, env):
    import donors as donors_mod
    campaign_id = seed_campaign(
        conn, env["company_id"], "Exact Giving", "20000000000000.00", "active")
    for day, amount in enumerate(AMOUNTS, start=1):
        _donate(donors_mod, conn, env, campaign_id, amount, day)
    assert _raised_on_fresh_conn(db_path, campaign_id) == EXPECTED_TOTAL


def test_refund_donation_subtracts_exact_campaign_total(conn, db_path, env):
    import donors as donors_mod
    campaign_id = seed_campaign(
        conn, env["company_id"], "Exact Refund", "20000000000000.00", "active")
    donation_ids = {}
    for day, amount in enumerate(AMOUNTS, start=1):
        donation_ids[amount] = _donate(
            donors_mod, conn, env, campaign_id, amount, day)
    refunded = call_action(donors_mod.refund_donation, conn, ns(
        id=donation_ids["1000.10"], donation_id=None,
    ))
    assert is_ok(refunded), refunded
    assert refunded["refunded"] is True
    assert _raised_on_fresh_conn(db_path, campaign_id) == EXPECTED_AFTER_REFUND


def test_fulfill_pledge_accumulates_exact_campaign_total(conn, db_path, env):
    import campaigns as camp_mod
    campaign_id = seed_campaign(
        conn, env["company_id"], "Exact Pledge", "20000000000000.00", "active")
    pledge = call_action(camp_mod.add_pledge, conn, ns(
        company_id=env["company_id"],
        donor_id=env["donor_id"],
        campaign_id=campaign_id,
        fund_id=None,
        amount=EXPECTED_TOTAL,
        pledge_date="2026-03-01",
        frequency="one_time",
        next_due_date=None,
        end_date=None,
        notes=None,
    ))
    assert is_ok(pledge), pledge
    for amount in AMOUNTS:
        fulfilled = call_action(camp_mod.fulfill_pledge, conn, ns(
            pledge_id=pledge["id"], id=None, amount=amount,
        ))
        assert is_ok(fulfilled), fulfilled
    assert _raised_on_fresh_conn(db_path, campaign_id) == EXPECTED_TOTAL
    stored = conn.execute(
        "SELECT fulfilled_amount, status FROM nonprofitclaw_pledge WHERE id = ?",
        (pledge["id"],),
    ).fetchone()
    assert stored["fulfilled_amount"] == EXPECTED_TOTAL
    assert stored["status"] == "fulfilled"


def test_expected_totals_are_hand_computed():
    total = sum((Decimal(a) for a in AMOUNTS), Decimal("0"))
    total = total.quantize(Decimal("0.01"), ROUND_HALF_UP)
    assert str(total) == EXPECTED_TOTAL
    after = (total - Decimal("1000.10")).quantize(
        Decimal("0.01"), ROUND_HALF_UP)
    assert str(after) == EXPECTED_AFTER_REFUND
