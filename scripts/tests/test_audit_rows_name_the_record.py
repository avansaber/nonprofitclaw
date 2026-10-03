"""Every nonprofitclaw audit row names the table and the record it changed.

Each audit row must carry skill ``nonprofitclaw``, the action name, the table
the record lives in as ``entity_type`` and that record's id as ``entity_id``,
so a record's history can be found by its id.
"""
from erpclaw_lib.query import Q, P, Table
from nonprofit_helpers import (
    call_action, ns, is_ok, load_db_query,
    seed_customer, seed_donor, seed_donation, seed_fund, seed_grant,
)

load_db_query()  # puts the module's scripts directory on sys.path

_audit = Table("audit_log")


def _audit_rows_for(conn, entity_id):
    """Audit rows filed under one record id (PyPika select, bound parameter)."""
    q = (
        Q.from_(_audit)
        .select(_audit.skill, _audit.action, _audit.entity_type, _audit.entity_id)
        .where(_audit.entity_id == P())
    )
    return conn.execute(q.get_sql(), (entity_id,)).fetchall()


def _assert_single_audit_row(conn, entity_id, skill, action, entity_type):
    rows = _audit_rows_for(conn, entity_id)
    matches = [
        row for row in rows
        if (row["skill"], row["action"], row["entity_type"])
        == (skill, action, entity_type)
    ]
    assert len(matches) == 1, (
        f"expected exactly one audit row { (skill, action, entity_type) }"
        f" for entity_id {entity_id!r}, found {len(matches)} in "
        f"{[(r['skill'], r['action'], r['entity_type'], r['entity_id']) for r in rows]}"
    )
    return matches[0]


def call_action_and_rollback(fn, conn, args):
    """Call an action the way the router leaves it: discard uncommitted rows.

    The command router closes the database without another commit, so
    anything the action did not commit is lost. A rollback on the still
    open test connection discards exactly what the router would lose.
    """
    result = call_action(fn, conn, args)
    conn.rollback()
    return result


def _add_donor(conn, company_id, name="Audit Donor"):
    import donors as donors_mod
    customer_id = seed_customer(conn, company_id, name)
    original = donors_mod.create_customer
    try:
        donors_mod.create_customer = lambda **kw: {"customer_id": customer_id}
        result = call_action_and_rollback(donors_mod.add_donor, conn, ns(
            company_id=company_id,
            name=name,
            donor_type="individual",
            donor_level="gold",
            email="audit@example.com",
            phone="555-0100",
            notes="audit trail donor",
        ))
    finally:
        donors_mod.create_customer = original
    assert is_ok(result), result
    return result["id"]


def test_campaigns_audit_rows_name_the_record(conn, env):
    import campaigns as camp_mod
    created = call_action_and_rollback(camp_mod.add_campaign, conn, ns(
        company_id=env["company_id"],
        name="Audit Appeal",
        description="audit trail campaign",
        fund_id=env["fund_id"],
        goal_amount="25000",
        start_date="2026-11-01",
        end_date="2026-12-31",
    ))
    assert is_ok(created), created
    campaign_id = created["id"]
    _assert_single_audit_row(conn, campaign_id, "nonprofitclaw",
                             "nonprofit-add-campaign", "nonprofitclaw_campaign")

    updated = call_action_and_rollback(camp_mod.update_campaign, conn, ns(
        id=campaign_id,
        name=None, description=None,
        start_date=None, end_date=None,
        goal_amount="50000.00",
        fund_id=None,
    ))
    assert is_ok(updated), updated
    _assert_single_audit_row(conn, campaign_id, "nonprofitclaw",
                             "nonprofit-update-campaign", "nonprofitclaw_campaign")

    activated = call_action_and_rollback(camp_mod.activate_campaign, conn, ns(id=campaign_id))
    assert is_ok(activated), activated
    _assert_single_audit_row(conn, campaign_id, "nonprofitclaw",
                             "nonprofit-activate-campaign", "nonprofitclaw_campaign")

    pledge_a = call_action_and_rollback(camp_mod.add_pledge, conn, ns(
        company_id=env["company_id"],
        donor_id=env["donor_id"],
        campaign_id=env["campaign_id"],
        fund_id=None,
        amount="1000",
        pledge_date="2026-03-01",
        frequency="one_time",
        next_due_date=None,
        end_date=None,
        notes=None,
    ))
    assert is_ok(pledge_a), pledge_a
    pledge_a_id = pledge_a["id"]
    _assert_single_audit_row(conn, pledge_a_id, "nonprofitclaw",
                             "nonprofit-add-pledge", "nonprofitclaw_pledge")

    fulfilled = call_action_and_rollback(camp_mod.fulfill_pledge, conn, ns(
        id=pledge_a_id, pledge_id=None, amount="400",
    ))
    assert is_ok(fulfilled), fulfilled
    _assert_single_audit_row(conn, pledge_a_id, "nonprofitclaw",
                             "nonprofit-fulfill-pledge", "nonprofitclaw_pledge")

    pledge_b = call_action_and_rollback(camp_mod.add_pledge, conn, ns(
        company_id=env["company_id"],
        donor_id=env["donor_id"],
        campaign_id=env["campaign_id"],
        fund_id=None,
        amount="500",
        pledge_date="2026-03-01",
        frequency="one_time",
        next_due_date=None,
        end_date=None,
        notes=None,
    ))
    assert is_ok(pledge_b), pledge_b
    pledge_b_id = pledge_b["id"]
    _assert_single_audit_row(conn, pledge_b_id, "nonprofitclaw",
                             "nonprofit-add-pledge", "nonprofitclaw_pledge")

    cancelled = call_action_and_rollback(camp_mod.cancel_pledge, conn, ns(id=pledge_b_id))
    assert is_ok(cancelled), cancelled
    _assert_single_audit_row(conn, pledge_b_id, "nonprofitclaw",
                             "nonprofit-cancel-pledge", "nonprofitclaw_pledge")

    closed = call_action_and_rollback(camp_mod.close_campaign, conn, ns(id=campaign_id))
    assert is_ok(closed), closed
    _assert_single_audit_row(conn, campaign_id, "nonprofitclaw",
                             "nonprofit-close-campaign", "nonprofitclaw_campaign")


def test_compliance_audit_rows_name_the_record(conn, env):
    import compliance as comp_mod
    donation_id = seed_donation(conn, env["company_id"], env["donor_id"], "500.00")
    created = call_action_and_rollback(comp_mod.generate_tax_receipt, conn, ns(
        company_id=env["company_id"],
        donor_id=env["donor_id"],
        tax_year="2026",
        receipt_type="single",
        donation_id=donation_id,
        sent_method="email",
    ))
    assert is_ok(created), created
    _assert_single_audit_row(conn, created["id"], "nonprofitclaw",
                             "nonprofit-generate-tax-receipt",
                             "nonprofitclaw_tax_receipt")


def test_donors_audit_rows_name_the_record(conn, env):
    import donors as donors_mod
    donor_id = _add_donor(conn, env["company_id"])
    _assert_single_audit_row(conn, donor_id, "nonprofitclaw",
                             "nonprofit-add-donor", "nonprofitclaw_donor_ext")

    updated = call_action_and_rollback(donors_mod.update_donor, conn, ns(
        id=donor_id,
        name=None,
        email=None,
        phone=None,
        address=None,
        tax_id=None,
        donor_type=None,
        donor_level="platinum",
        notes=None,
        is_active=None,
    ))
    assert is_ok(updated), updated
    _assert_single_audit_row(conn, donor_id, "nonprofitclaw",
                             "nonprofit-update-donor", "nonprofitclaw_donor_ext")

    source = seed_donor(conn, env["company_id"], "Merge Source")
    seed_donation(conn, env["company_id"], donor_id, "100.00")
    seed_donation(conn, env["company_id"], source["donor_id"], "200.00")
    merged = call_action_and_rollback(donors_mod.merge_donors, conn, ns(
        source_donor_id=source["donor_id"],
        target_donor_id=donor_id,
    ))
    assert is_ok(merged), merged
    _assert_single_audit_row(conn, donor_id, "nonprofitclaw",
                             "nonprofit-merge-donors", "nonprofitclaw_donor_ext")

    added = call_action_and_rollback(donors_mod.add_donation, conn, ns(
        company_id=env["company_id"],
        donor_id=donor_id,
        amount="300.00",
        payment_method="bank_transfer",
        donation_date="2026-02-15",
        reference=None,
        fund_id=None,
        campaign_id=None,
        is_recurring=None,
        recurrence_freq=None,
        notes=None,
        cash_account_id=None,
        revenue_account_id=None,
        cost_center_id=None,
    ))
    assert is_ok(added), added
    donation_id = added["id"]
    _assert_single_audit_row(conn, donation_id, "nonprofitclaw",
                             "nonprofit-add-donation", "nonprofitclaw_donation")

    donation_updated = call_action_and_rollback(donors_mod.update_donation, conn, ns(
        id=donation_id,
        payment_method="credit_card",
        reference="CC-999",
        notes=None,
        donation_date=None,
        is_recurring=None,
        recurrence_freq=None,
    ))
    assert is_ok(donation_updated), donation_updated
    _assert_single_audit_row(conn, donation_id, "nonprofitclaw",
                             "nonprofit-update-donation", "nonprofitclaw_donation")

    refunded = call_action_and_rollback(donors_mod.refund_donation, conn, ns(
        id=donation_id, donation_id=None,
    ))
    assert is_ok(refunded), refunded
    _assert_single_audit_row(conn, donation_id, "nonprofitclaw",
                             "nonprofit-refund-donation", "nonprofitclaw_donation")


def test_funds_audit_rows_name_the_record(conn, env):
    import funds as funds_mod
    created = call_action_and_rollback(funds_mod.add_fund, conn, ns(
        company_id=env["company_id"],
        name="Audit Fund",
        fund_type="unrestricted",
        description="audit trail fund",
        target_amount="50000",
        start_date="2026-01-01",
        end_date="2026-12-31",
    ))
    assert is_ok(created), created
    fund_id = created["id"]
    _assert_single_audit_row(conn, fund_id, "nonprofitclaw",
                             "nonprofit-add-fund", "nonprofitclaw_fund")

    updated = call_action_and_rollback(funds_mod.update_fund, conn, ns(
        id=fund_id,
        name="Renamed Audit Fund",
        fund_type=None,
        description=None,
        target_amount=None,
        start_date=None,
        end_date=None,
        is_active=None,
    ))
    assert is_ok(updated), updated
    _assert_single_audit_row(conn, fund_id, "nonprofitclaw",
                             "nonprofit-update-fund", "nonprofitclaw_fund")

    conn.execute(
        "UPDATE nonprofitclaw_fund SET current_balance='5000' WHERE id=?",
        (fund_id,),
    )
    conn.commit()
    transfer = call_action_and_rollback(funds_mod.add_fund_transfer, conn, ns(
        company_id=env["company_id"],
        from_fund_id=fund_id,
        to_fund_id=env["fund_id"],
        amount="2000.00",
        transfer_date="2026-03-01",
        reason="Reallocation",
        approved_by=None,
    ))
    assert is_ok(transfer), transfer
    transfer_id = transfer["id"]
    _assert_single_audit_row(conn, transfer_id, "nonprofitclaw",
                             "nonprofit-add-fund-transfer",
                             "nonprofitclaw_fund_transfer")

    approved = call_action_and_rollback(funds_mod.approve_fund_transfer, conn, ns(
        id=transfer_id,
        approved_by="Admin",
    ))
    assert is_ok(approved), approved
    _assert_single_audit_row(conn, transfer_id, "nonprofitclaw",
                             "nonprofit-approve-fund-transfer",
                             "nonprofitclaw_fund_transfer")


def test_grants_audit_rows_name_the_record(conn, env):
    import grants as grants_mod
    created = call_action_and_rollback(grants_mod.add_grant, conn, ns(
        company_id=env["company_id"],
        name="Audit Grant",
        grantor_name="Audit Foundation",
        grantor_type="foundation",
        grant_type="project",
        amount="100000",
        fund_id=None,
        start_date="2026-01-01",
        end_date="2026-12-31",
        reporting_freq="quarterly",
        notes="audit trail grant",
    ))
    assert is_ok(created), created
    grant_id = created["id"]
    _assert_single_audit_row(conn, grant_id, "nonprofitclaw",
                             "nonprofit-add-grant", "nonprofitclaw_grant")

    updated = call_action_and_rollback(grants_mod.update_grant, conn, ns(
        id=grant_id,
        name="Renamed Audit Grant",
        grantor_name=None,
        grantor_type=None,
        grant_type=None,
        reporting_freq=None,
        start_date=None,
        end_date=None,
        notes=None,
        fund_id=None,
    ))
    assert is_ok(updated), updated
    _assert_single_audit_row(conn, grant_id, "nonprofitclaw",
                             "nonprofit-update-grant", "nonprofitclaw_grant")

    activated = call_action_and_rollback(grants_mod.activate_grant, conn, ns(
        id=grant_id,
        amount=None,
    ))
    assert is_ok(activated), activated
    _assert_single_audit_row(conn, grant_id, "nonprofitclaw",
                             "nonprofit-activate-grant", "nonprofitclaw_grant")

    expense = call_action_and_rollback(grants_mod.add_grant_expense, conn, ns(
        company_id=env["company_id"],
        grant_id=grant_id,
        amount="2000.00",
        category="personnel",
        description="Staff salary",
        expense_date="2026-03-01",
        receipt_reference=None,
    ))
    assert is_ok(expense), expense
    expense_id = expense["id"]
    _assert_single_audit_row(conn, expense_id, "nonprofitclaw",
                             "nonprofit-add-grant-expense",
                             "nonprofitclaw_grant_expense")

    approved = call_action_and_rollback(grants_mod.approve_grant_expense, conn, ns(
        id=expense_id,
        expense_account_id=env["expense_acct"],
        cash_account_id=env["cash_acct"],
        cost_center_id=env["cc_id"],
    ))
    assert is_ok(approved), approved
    _assert_single_audit_row(conn, expense_id, "nonprofitclaw",
                             "nonprofit-approve-grant-expense",
                             "nonprofitclaw_grant_expense")

    closed = call_action_and_rollback(grants_mod.close_grant, conn, ns(id=grant_id))
    assert is_ok(closed), closed
    _assert_single_audit_row(conn, grant_id, "nonprofitclaw",
                             "nonprofit-close-grant", "nonprofitclaw_grant")


def test_programs_audit_rows_name_the_record(conn, env):
    import programs as programs_mod
    created = call_action_and_rollback(programs_mod.add_program, conn, ns(
        company_id=env["company_id"],
        name="Audit Program",
        description="audit trail program",
        fund_id=None,
        budget="8000",
        beneficiary_count="50",
        start_date="2026-01-01",
        end_date="2026-06-30",
        outcome_metrics=None,
    ))
    assert is_ok(created), created
    program_id = created["id"]
    _assert_single_audit_row(conn, program_id, "nonprofitclaw",
                             "nonprofit-add-program", "nonprofitclaw_program")

    updated = call_action_and_rollback(programs_mod.update_program, conn, ns(
        id=program_id,
        name=None,
        description=None,
        start_date=None,
        end_date=None,
        outcome_metrics=None,
        budget="12000",
        fund_id=None,
        is_active=None,
        beneficiary_count=None,
    ))
    assert is_ok(updated), updated
    _assert_single_audit_row(conn, program_id, "nonprofitclaw",
                             "nonprofit-update-program", "nonprofitclaw_program")

    outcomes = call_action_and_rollback(programs_mod.update_program_outcomes, conn, ns(
        id=program_id,
        beneficiary_count="150",
        outcome_metrics='{"students_served": 150, "graduation_rate": "85%"}',
    ))
    assert is_ok(outcomes), outcomes
    _assert_single_audit_row(conn, program_id, "nonprofitclaw",
                             "nonprofit-update-program-outcomes",
                             "nonprofitclaw_program")


def test_volunteers_audit_rows_name_the_record(conn, env):
    import volunteers as vol_mod
    created = call_action_and_rollback(vol_mod.add_volunteer, conn, ns(
        company_id=env["company_id"],
        name="Audit Helper",
        email="audit-helper@example.com",
        phone="555-9876",
        skills="teaching,counseling",
        availability="weekends",
        start_date="2026-01-15",
    ))
    assert is_ok(created), created
    volunteer_id = created["id"]
    _assert_single_audit_row(conn, volunteer_id, "nonprofitclaw",
                             "nonprofit-add-volunteer", "nonprofitclaw_volunteer")

    updated = call_action_and_rollback(vol_mod.update_volunteer, conn, ns(
        id=volunteer_id,
        name=None, email=None, phone=None,
        skills="mentoring,driving,cooking",
        availability=None, start_date=None, is_active=None,
    ))
    assert is_ok(updated), updated
    _assert_single_audit_row(conn, volunteer_id, "nonprofitclaw",
                             "nonprofit-update-volunteer",
                             "nonprofitclaw_volunteer")

    shift = call_action_and_rollback(vol_mod.add_volunteer_shift, conn, ns(
        company_id=env["company_id"],
        volunteer_id=volunteer_id,
        program_id=env["program_id"],
        shift_date="2026-03-15",
        hours="4.5",
        description="Morning tutoring session",
    ))
    assert is_ok(shift), shift
    shift_id = shift["id"]
    _assert_single_audit_row(conn, shift_id, "nonprofitclaw",
                             "nonprofit-add-volunteer-shift",
                             "nonprofitclaw_volunteer_shift")

    completed = call_action_and_rollback(vol_mod.complete_volunteer_shift, conn, ns(
        id=shift_id,
        hours=None,
    ))
    assert is_ok(completed), completed
    _assert_single_audit_row(conn, shift_id, "nonprofitclaw",
                             "nonprofit-complete-volunteer-shift",
                             "nonprofitclaw_volunteer_shift")


def test_no_audit_row_is_keyed_by_company(conn, env):
    import campaigns as camp_mod
    import compliance as comp_mod
    import donors as donors_mod
    import funds as funds_mod
    import grants as grants_mod
    import programs as programs_mod
    import volunteers as vol_mod

    company_id = env["company_id"]

    campaign = call_action_and_rollback(camp_mod.add_campaign, conn, ns(
        company_id=company_id,
        name="Company Key Audit",
        description=None,
        fund_id=None,
        goal_amount="1000",
        start_date=None,
        end_date=None,
    ))
    assert is_ok(campaign), campaign
    call_action_and_rollback(camp_mod.update_campaign, conn, ns(
        id=campaign["id"],
        name=None, description=None,
        start_date=None, end_date=None,
        goal_amount="2000",
        fund_id=None,
    ))
    call_action_and_rollback(camp_mod.activate_campaign, conn, ns(id=campaign["id"]))
    pledge = call_action_and_rollback(camp_mod.add_pledge, conn, ns(
        company_id=company_id,
        donor_id=env["donor_id"],
        campaign_id=env["campaign_id"],
        fund_id=None,
        amount="100",
        pledge_date="2026-03-01",
        frequency="one_time",
        next_due_date=None,
        end_date=None,
        notes=None,
    ))
    assert is_ok(pledge), pledge
    call_action_and_rollback(camp_mod.fulfill_pledge, conn, ns(
        id=pledge["id"], pledge_id=None, amount="100",
    ))
    pledge_b = call_action_and_rollback(camp_mod.add_pledge, conn, ns(
        company_id=company_id,
        donor_id=env["donor_id"],
        campaign_id=env["campaign_id"],
        fund_id=None,
        amount="50",
        pledge_date="2026-03-01",
        frequency="one_time",
        next_due_date=None,
        end_date=None,
        notes=None,
    ))
    assert is_ok(pledge_b), pledge_b
    call_action_and_rollback(camp_mod.cancel_pledge, conn, ns(id=pledge_b["id"]))
    call_action_and_rollback(camp_mod.close_campaign, conn, ns(id=campaign["id"]))

    donation_id = seed_donation(conn, company_id, env["donor_id"], "500.00")
    receipt = call_action_and_rollback(comp_mod.generate_tax_receipt, conn, ns(
        company_id=company_id,
        donor_id=env["donor_id"],
        tax_year="2026",
        receipt_type="single",
        donation_id=donation_id,
        sent_method="email",
    ))
    assert is_ok(receipt), receipt

    donor_id = _add_donor(conn, company_id, name="Company Key Donor")
    call_action_and_rollback(donors_mod.update_donor, conn, ns(
        id=donor_id,
        name=None, email=None, phone=None, address=None, tax_id=None,
        donor_type=None, donor_level="silver", notes=None, is_active=None,
    ))
    source = seed_donor(conn, company_id, "Company Key Source")
    call_action_and_rollback(donors_mod.merge_donors, conn, ns(
        source_donor_id=source["donor_id"],
        target_donor_id=donor_id,
    ))
    donation = call_action_and_rollback(donors_mod.add_donation, conn, ns(
        company_id=company_id,
        donor_id=donor_id,
        amount="120.00",
        payment_method="check",
        donation_date="2026-02-15",
        reference=None,
        fund_id=None,
        campaign_id=None,
        is_recurring=None,
        recurrence_freq=None,
        notes=None,
        cash_account_id=None,
        revenue_account_id=None,
        cost_center_id=None,
    ))
    assert is_ok(donation), donation
    call_action_and_rollback(donors_mod.update_donation, conn, ns(
        id=donation["id"],
        payment_method="credit_card",
        reference=None,
        notes=None,
        donation_date=None,
        is_recurring=None,
        recurrence_freq=None,
    ))
    call_action_and_rollback(donors_mod.refund_donation, conn, ns(
        id=donation["id"], donation_id=None,
    ))

    fund = call_action_and_rollback(funds_mod.add_fund, conn, ns(
        company_id=company_id,
        name="Company Key Fund",
        fund_type="unrestricted",
        description=None,
        target_amount=None,
        start_date=None,
        end_date=None,
    ))
    assert is_ok(fund), fund
    call_action_and_rollback(funds_mod.update_fund, conn, ns(
        id=fund["id"],
        name="Company Key Fund II",
        fund_type=None,
        description=None,
        target_amount=None,
        start_date=None,
        end_date=None,
        is_active=None,
    ))
    conn.execute(
        "UPDATE nonprofitclaw_fund SET current_balance='5000' WHERE id=?",
        (fund["id"],),
    )
    conn.commit()
    transfer = call_action_and_rollback(funds_mod.add_fund_transfer, conn, ns(
        company_id=company_id,
        from_fund_id=fund["id"],
        to_fund_id=env["fund_id"],
        amount="1000.00",
        transfer_date="2026-03-01",
        reason="Reallocation",
        approved_by=None,
    ))
    assert is_ok(transfer), transfer
    call_action_and_rollback(funds_mod.approve_fund_transfer, conn, ns(
        id=transfer["id"],
        approved_by="Admin",
    ))

    grant = call_action_and_rollback(grants_mod.add_grant, conn, ns(
        company_id=company_id,
        name="Company Key Grant",
        grantor_name="Audit Foundation",
        grantor_type="foundation",
        grant_type="project",
        amount="20000",
        fund_id=None,
        start_date=None,
        end_date=None,
        reporting_freq="quarterly",
        notes=None,
    ))
    assert is_ok(grant), grant
    call_action_and_rollback(grants_mod.update_grant, conn, ns(
        id=grant["id"],
        name="Company Key Grant II",
        grantor_name=None,
        grantor_type=None,
        grant_type=None,
        reporting_freq=None,
        start_date=None,
        end_date=None,
        notes=None,
        fund_id=None,
    ))
    call_action_and_rollback(grants_mod.activate_grant, conn, ns(id=grant["id"], amount=None))
    expense = call_action_and_rollback(grants_mod.add_grant_expense, conn, ns(
        company_id=company_id,
        grant_id=grant["id"],
        amount="500.00",
        category="program",
        description=None,
        expense_date="2026-03-05",
        receipt_reference=None,
    ))
    assert is_ok(expense), expense
    call_action_and_rollback(grants_mod.approve_grant_expense, conn, ns(
        id=expense["id"],
        expense_account_id=env["expense_acct"],
        cash_account_id=env["cash_acct"],
        cost_center_id=env["cc_id"],
    ))
    call_action_and_rollback(grants_mod.close_grant, conn, ns(id=grant["id"]))

    program = call_action_and_rollback(programs_mod.add_program, conn, ns(
        company_id=company_id,
        name="Company Key Program",
        description=None,
        fund_id=None,
        budget="8000",
        beneficiary_count=None,
        start_date=None,
        end_date=None,
        outcome_metrics=None,
    ))
    assert is_ok(program), program
    call_action_and_rollback(programs_mod.update_program, conn, ns(
        id=program["id"],
        name=None,
        description=None,
        start_date=None,
        end_date=None,
        outcome_metrics=None,
        budget="9000",
        fund_id=None,
        is_active=None,
        beneficiary_count=None,
    ))
    call_action_and_rollback(programs_mod.update_program_outcomes, conn, ns(
        id=program["id"],
        beneficiary_count="150",
        outcome_metrics='{"served": 150}',
    ))

    volunteer = call_action_and_rollback(vol_mod.add_volunteer, conn, ns(
        company_id=company_id,
        name="Company Key Helper",
        email="company-key@example.com",
        phone=None,
        skills=None,
        availability=None,
        start_date=None,
    ))
    assert is_ok(volunteer), volunteer
    call_action_and_rollback(vol_mod.update_volunteer, conn, ns(
        id=volunteer["id"],
        name=None, email=None, phone=None,
        skills="driving",
        availability=None, start_date=None, is_active=None,
    ))
    shift = call_action_and_rollback(vol_mod.add_volunteer_shift, conn, ns(
        company_id=company_id,
        volunteer_id=volunteer["id"],
        program_id=None,
        shift_date="2026-03-10",
        hours="3.00",
        description=None,
    ))
    assert is_ok(shift), shift
    call_action_and_rollback(vol_mod.complete_volunteer_shift, conn, ns(
        id=shift["id"],
        hours=None,
    ))

    keyed_by_company = _audit_rows_for(conn, company_id)
    assert keyed_by_company == [], (
        f"expected no audit row with entity_id equal to the company id "
        f"{company_id!r}, found "
        f"{[(r['skill'], r['action'], r['entity_type'], r['entity_id']) for r in keyed_by_company]}"
    )

    q_all = (
        Q.from_(_audit)
        .select(_audit.skill, _audit.action, _audit.entity_type, _audit.entity_id)
        .where(_audit.skill == P())
    )
    skill_rows = conn.execute(q_all.get_sql(), ("nonprofitclaw",)).fetchall()
    assert skill_rows, "expected nonprofitclaw audit rows to exist"
    foreign = [
        row for row in skill_rows
        if not row["entity_type"].startswith("nonprofitclaw_")
    ]
    assert foreign == [], (
        f"expected every nonprofitclaw entity_type to start with "
        f"'nonprofitclaw_', found "
        f"{[(r['action'], r['entity_type'], r['entity_id']) for r in foreign]}"
    )
