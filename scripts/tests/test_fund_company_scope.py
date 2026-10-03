"""Fund and campaign references stay inside their own company.

A grant, donation, pledge or campaign can only name a fund or campaign of its
own company, and a grant's fund is fixed once the grant has received money.
"""
from nonprofit_helpers import (
    call_action, ns, is_error, is_ok, load_db_query,
    seed_company, seed_naming_series, seed_fund, seed_campaign, seed_donor,
    snapshot_tables,
)

load_db_query()  # puts the module's scripts directory on sys.path, as the sibling files do

TABLES = [
    "nonprofitclaw_donor_ext",
    "nonprofitclaw_donation",
    "nonprofitclaw_fund",
    "nonprofitclaw_fund_transfer",
    "nonprofitclaw_grant",
    "nonprofitclaw_grant_expense",
    "nonprofitclaw_program",
    "nonprofitclaw_volunteer",
    "nonprofitclaw_volunteer_shift",
    "nonprofitclaw_pledge",
    "nonprofitclaw_campaign",
    "nonprofitclaw_tax_receipt",
    "audit_log",
]


def _foreign(conn):
    other = seed_company(conn)
    seed_naming_series(conn, other)
    foreign_fund = seed_fund(conn, other, "Foreign Fund")
    foreign_campaign = seed_campaign(conn, other, "Foreign Drive")
    return other, foreign_fund, foreign_campaign


def _assert_refused(result, message):
    assert is_error(result), result
    assert result["message"] == message
    assert "Foreign Fund" not in result["message"]
    assert "Foreign Drive" not in result["message"]


def _stored_fund(conn, table, row_id):
    return conn.execute(
        f"SELECT fund_id FROM {table} WHERE id = ?",
        (row_id,)).fetchone()["fund_id"]


def test_add_grant_refuses_other_company_fund(conn, env):
    import grants as grants_mod
    _, foreign_fund, _ = _foreign(conn)
    before = snapshot_tables(conn, TABLES)
    result = call_action(grants_mod.add_grant, conn, ns(
        company_id=env["company_id"],
        name="Cross Grant",
        grantor_name="Far Foundation",
        grantor_type="foundation",
        grant_type="project",
        amount="1000",
        fund_id=foreign_fund,
        start_date=None,
        end_date=None,
        reporting_freq="quarterly",
        notes=None,
    ))
    _assert_refused(result, "Fund does not belong to this company")
    assert snapshot_tables(conn, TABLES) == before


def test_update_grant_refuses_other_company_fund(conn, env):
    import grants as grants_mod
    _, foreign_fund, _ = _foreign(conn)
    before = snapshot_tables(conn, TABLES)
    result = call_action(grants_mod.update_grant, conn, ns(
        id=env["grant_id"],
        fund_id=foreign_fund,
    ))
    _assert_refused(result, "Fund does not belong to this company")
    assert snapshot_tables(conn, TABLES) == before


def test_add_donation_refuses_other_company_fund(conn, env):
    import donors as donors_mod
    _, foreign_fund, _ = _foreign(conn)
    before = snapshot_tables(conn, TABLES)
    result = call_action(donors_mod.add_donation, conn, ns(
        company_id=env["company_id"],
        donor_id=env["donor_id"],
        fund_id=foreign_fund,
        campaign_id=None,
        amount="100.00",
        donation_date=None,
        payment_method=None,
        reference=None,
        is_recurring=None,
        recurrence_freq=None,
        notes=None,
    ))
    _assert_refused(result, "Fund does not belong to this company")
    assert snapshot_tables(conn, TABLES) == before


def test_add_donation_refuses_other_company_campaign(conn, env):
    import donors as donors_mod
    _, _, foreign_campaign = _foreign(conn)
    before = snapshot_tables(conn, TABLES)
    result = call_action(donors_mod.add_donation, conn, ns(
        company_id=env["company_id"],
        donor_id=env["donor_id"],
        fund_id=None,
        campaign_id=foreign_campaign,
        amount="100.00",
        donation_date=None,
        payment_method=None,
        reference=None,
        is_recurring=None,
        recurrence_freq=None,
        notes=None,
    ))
    _assert_refused(result, "Campaign does not belong to this company")
    assert snapshot_tables(conn, TABLES) == before


def test_add_campaign_refuses_other_company_fund(conn, env):
    import campaigns as camp_mod
    _, foreign_fund, _ = _foreign(conn)
    before = snapshot_tables(conn, TABLES)
    result = call_action(camp_mod.add_campaign, conn, ns(
        company_id=env["company_id"],
        name="Cross Campaign",
        description=None,
        fund_id=foreign_fund,
        goal_amount="1000",
        start_date=None,
        end_date=None,
    ))
    _assert_refused(result, "Fund does not belong to this company")
    assert snapshot_tables(conn, TABLES) == before


def test_update_campaign_refuses_other_company_fund(conn, env):
    import campaigns as camp_mod
    _, foreign_fund, _ = _foreign(conn)
    before = snapshot_tables(conn, TABLES)
    result = call_action(camp_mod.update_campaign, conn, ns(
        id=env["campaign_id"],
        fund_id=foreign_fund,
    ))
    _assert_refused(result, "Fund does not belong to this company")
    assert snapshot_tables(conn, TABLES) == before


def test_add_pledge_refuses_other_company_campaign(conn, env):
    import campaigns as camp_mod
    _, _, foreign_campaign = _foreign(conn)
    before = snapshot_tables(conn, TABLES)
    result = call_action(camp_mod.add_pledge, conn, ns(
        company_id=env["company_id"],
        donor_id=env["donor_id"],
        campaign_id=foreign_campaign,
        fund_id=None,
        amount="100.00",
        pledge_date=None,
        frequency=None,
        next_due_date=None,
        end_date=None,
        notes=None,
    ))
    _assert_refused(result, "Campaign does not belong to this company")
    assert snapshot_tables(conn, TABLES) == before


def test_add_pledge_refuses_other_company_fund(conn, env):
    import campaigns as camp_mod
    _, foreign_fund, _ = _foreign(conn)
    before = snapshot_tables(conn, TABLES)
    result = call_action(camp_mod.add_pledge, conn, ns(
        company_id=env["company_id"],
        donor_id=env["donor_id"],
        campaign_id=env["campaign_id"],
        fund_id=foreign_fund,
        amount="100.00",
        pledge_date=None,
        frequency=None,
        next_due_date=None,
        end_date=None,
        notes=None,
    ))
    _assert_refused(result, "Fund does not belong to this company")
    assert snapshot_tables(conn, TABLES) == before


def _activated_grant(conn, env):
    import grants as grants_mod
    created = call_action(grants_mod.add_grant, conn, ns(
        company_id=env["company_id"],
        name="Fixed Fund Grant",
        grantor_name="Steady Foundation",
        grantor_type="foundation",
        grant_type="project",
        amount="500.00",
        fund_id=env["fund_id"],
        start_date=None,
        end_date=None,
        reporting_freq="quarterly",
        notes=None,
    ))
    assert is_ok(created), created
    activated = call_action(grants_mod.activate_grant, conn, ns(
        id=created["id"],
        amount="500.00",
    ))
    assert is_ok(activated), activated
    return created["id"]


def test_update_grant_refuses_fund_change_after_activation(conn, env):
    import grants as grants_mod
    second = seed_fund(conn, env["company_id"], "Second Fund")
    grant_id = _activated_grant(conn, env)
    expected = f"Grant {grant_id} is 'active'; its fund cannot change after activation"
    before = snapshot_tables(conn, TABLES)
    change = call_action(grants_mod.update_grant, conn, ns(
        id=grant_id,
        fund_id=second,
    ))
    _assert_refused(change, expected)
    assert snapshot_tables(conn, TABLES) == before
    cleared = call_action(grants_mod.update_grant, conn, ns(
        id=grant_id,
        fund_id="",
    ))
    _assert_refused(cleared, expected)
    assert snapshot_tables(conn, TABLES) == before
    balance = conn.execute(
        "SELECT current_balance FROM nonprofitclaw_fund WHERE id = ?",
        (env["fund_id"],)).fetchone()["current_balance"]
    assert balance == "500.00"


def test_update_grant_same_fund_after_activation_is_allowed(conn, env):
    import grants as grants_mod
    grant_id = _activated_grant(conn, env)
    result = call_action(grants_mod.update_grant, conn, ns(
        id=grant_id,
        fund_id=env["fund_id"],
        notes="kept fund",
    ))
    assert is_ok(result), result
    row = conn.execute(
        "SELECT fund_id, notes FROM nonprofitclaw_grant WHERE id = ?",
        (grant_id,)).fetchone()
    assert row["fund_id"] == env["fund_id"]
    assert row["notes"] == "kept fund"


def test_same_company_fund_still_accepted(conn, env):
    import campaigns as camp_mod
    import donors as donors_mod
    import grants as grants_mod
    grant = call_action(grants_mod.add_grant, conn, ns(
        company_id=env["company_id"],
        name="Home Grant",
        grantor_name="Near Foundation",
        grantor_type="foundation",
        grant_type="project",
        amount="1000",
        fund_id=env["fund_id"],
        start_date=None,
        end_date=None,
        reporting_freq="quarterly",
        notes=None,
    ))
    assert is_ok(grant), grant
    assert _stored_fund(conn, "nonprofitclaw_grant", grant["id"]) == env["fund_id"]
    donation = call_action(donors_mod.add_donation, conn, ns(
        company_id=env["company_id"],
        donor_id=env["donor_id"],
        fund_id=env["fund_id"],
        campaign_id=None,
        amount="50.00",
        donation_date=None,
        payment_method=None,
        reference=None,
        is_recurring=None,
        recurrence_freq=None,
        notes=None,
    ))
    assert is_ok(donation), donation
    assert _stored_fund(conn, "nonprofitclaw_donation", donation["id"]) == env["fund_id"]
    pledge = call_action(camp_mod.add_pledge, conn, ns(
        company_id=env["company_id"],
        donor_id=env["donor_id"],
        campaign_id=env["campaign_id"],
        fund_id=env["fund_id"],
        amount="75.00",
        pledge_date=None,
        frequency=None,
        next_due_date=None,
        end_date=None,
        notes=None,
    ))
    assert is_ok(pledge), pledge
    assert _stored_fund(conn, "nonprofitclaw_pledge", pledge["id"]) == env["fund_id"]
