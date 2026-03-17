#!/usr/bin/env python3
"""NonprofitClaw campaigns & pledges domain — 11 actions."""
import os
import sys
import uuid
from datetime import date
from decimal import Decimal, ROUND_HALF_UP

sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
from erpclaw_lib.naming import get_next_name
from erpclaw_lib.response import ok, err
from erpclaw_lib.audit import audit
from erpclaw_lib.query import (
    Q, P, Table, Field, fn, Order, LiteralValue,
    insert_row, update_row, dynamic_update,
)

SKILL = "nonprofitclaw"

# ── Table aliases ──
_campaign = Table("nonprofitclaw_campaign")
_pledge = Table("nonprofitclaw_pledge")
_de = Table("nonprofitclaw_donor_ext")
_c = Table("customer")
_fund = Table("nonprofitclaw_fund")
_don = Table("nonprofitclaw_donation")


def _dec(val):
    if val is None:
        return Decimal("0")
    return Decimal(str(val))


def _round(val):
    return val.quantize(Decimal("0.01"), ROUND_HALF_UP)


# ------------------------------------------------------------------
# Campaign CRUD
# ------------------------------------------------------------------

def add_campaign(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    name = args.name
    if not name:
        return err("--name is required")

    campaign_id = str(uuid.uuid4())
    naming = get_next_name(conn, "nonprofitclaw_campaign", company_id=company_id)

    goal_amount = getattr(args, "goal_amount", None)
    if goal_amount:
        goal_amount = str(_round(_dec(goal_amount)))
    else:
        goal_amount = "0"

    fund_id = getattr(args, "fund_id", None)
    if fund_id:
        fq = Q.from_(_fund).select(_fund.id).where(_fund.id == P())
        if not conn.execute(fq.get_sql(), (fund_id,)).fetchone():
            return err(f"Fund {fund_id} not found")

    sql, _ = insert_row("nonprofitclaw_campaign", {
        "id": P(), "naming_series": P(), "name": P(), "description": P(),
        "fund_id": P(), "goal_amount": P(), "start_date": P(),
        "end_date": P(), "status": P(), "company_id": P(),
    })
    conn.execute(sql, (
        campaign_id, naming, name,
        getattr(args, "description", None),
        fund_id, goal_amount,
        getattr(args, "start_date", None),
        getattr(args, "end_date", None),
        "draft", company_id,
    ))
    conn.commit()
    audit(conn, SKILL, "nonprofit-add-campaign", campaign_id, company_id)
    return ok({"id": campaign_id, "naming_series": naming, "name": name})


def update_campaign(conn, args):
    campaign_id = args.id
    if not campaign_id:
        return err("--id is required")

    q = Q.from_(_campaign).select(_campaign.id, _campaign.company_id, _campaign.status).where(_campaign.id == P())
    row = conn.execute(q.get_sql(), (campaign_id,)).fetchone()
    if not row:
        return err(f"Campaign {campaign_id} not found")
    if row["status"] in ("completed", "cancelled"):
        return err(f"Cannot update campaign in '{row['status']}' status")

    data = {}
    for col, attr in [
        ("name", "name"), ("description", "description"),
        ("start_date", "start_date"), ("end_date", "end_date"),
    ]:
        val = getattr(args, attr, None)
        if val is not None:
            data[col] = val

    goal_amount = getattr(args, "goal_amount", None)
    if goal_amount is not None:
        data["goal_amount"] = str(_round(_dec(goal_amount)))

    fund_id = getattr(args, "fund_id", None)
    if fund_id is not None:
        if fund_id:
            fq = Q.from_(_fund).select(_fund.id).where(_fund.id == P())
            if not conn.execute(fq.get_sql(), (fund_id,)).fetchone():
                return err(f"Fund {fund_id} not found")
        data["fund_id"] = fund_id if fund_id else None

    if not data:
        return err("No fields to update")

    data["updated_at"] = now()
    sql, params = dynamic_update("nonprofitclaw_campaign", data, where={"id": campaign_id})
    conn.execute(sql, params)
    conn.commit()
    audit(conn, SKILL, "nonprofit-update-campaign", campaign_id, row["company_id"])
    return ok({"id": campaign_id, "updated": True})


def list_campaigns(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    limit = int(getattr(args, "limit", None) or 50)
    offset = int(getattr(args, "offset", None) or 0)

    conditions = [_campaign.company_id == P()]
    params = [company_id]

    status = getattr(args, "status", None)
    if status:
        conditions.append(_campaign.status == P())
        params.append(status)

    search = getattr(args, "search", None)
    if search:
        conditions.append(_campaign.name.like(P()))
        params.append(f"%{search}%")

    count_q = Q.from_(_campaign).select(fn.Count("*"))
    for cond in conditions:
        count_q = count_q.where(cond)
    total = conn.execute(count_q.get_sql(), params).fetchone()[0]

    data_q = Q.from_(_campaign).select(
        _campaign.id, _campaign.naming_series, _campaign.name, _campaign.description,
        _campaign.fund_id, _campaign.goal_amount, _campaign.raised_amount,
        _campaign.donor_count, _campaign.start_date, _campaign.end_date, _campaign.status,
    )
    for cond in conditions:
        data_q = data_q.where(cond)
    data_q = data_q.orderby(_campaign.created_at, order=Order.desc).limit(P()).offset(P())

    rows = conn.execute(data_q.get_sql(), params + [limit, offset]).fetchall()

    campaigns = []
    for r in rows:
        c = dict(r)
        goal = _dec(r["goal_amount"])
        raised = _dec(r["raised_amount"])
        if goal > Decimal("0"):
            c["percent_of_goal"] = str(_round(raised / goal * Decimal("100")))
        campaigns.append(c)

    return ok({"campaigns": campaigns, "total": total})


def get_campaign(conn, args):
    campaign_id = args.id
    if not campaign_id:
        return err("--id is required")

    q = Q.from_(_campaign).select(_campaign.star).where(_campaign.id == P())
    row = conn.execute(q.get_sql(), (campaign_id,)).fetchone()
    if not row:
        return err(f"Campaign {campaign_id} not found")

    # Get pledge summary
    pledge_q = (
        Q.from_(_pledge)
        .select(
            fn.Count("*").as_("pledge_count"),
            LiteralValue("SUM(CAST(amount AS NUMERIC))").as_("pledged_total"),
            LiteralValue("SUM(CAST(fulfilled_amount AS NUMERIC))").as_("fulfilled_total"),
        )
        .where(_pledge.campaign_id == P())
        .where(_pledge.status != "cancelled")
    )
    pledge_stats = conn.execute(pledge_q.get_sql(), (campaign_id,)).fetchone()

    # Get donation summary
    don_q = (
        Q.from_(_don)
        .select(
            fn.Count("*").as_("donation_count"),
            LiteralValue("SUM(CAST(amount AS NUMERIC))").as_("donation_total"),
        )
        .where(_don.campaign_id == P())
        .where(_don.status.notin(["refunded", "cancelled"]))
    )
    donation_stats = conn.execute(don_q.get_sql(), (campaign_id,)).fetchone()

    campaign_data = dict(row)
    campaign_data["pledge_count"] = pledge_stats["pledge_count"] or 0
    campaign_data["pledged_total"] = str(_round(_dec(pledge_stats["pledged_total"]))) if pledge_stats["pledged_total"] else "0.00"
    campaign_data["fulfilled_total"] = str(_round(_dec(pledge_stats["fulfilled_total"]))) if pledge_stats["fulfilled_total"] else "0.00"
    campaign_data["donation_count"] = donation_stats["donation_count"] or 0
    campaign_data["donation_total"] = str(_round(_dec(donation_stats["donation_total"]))) if donation_stats["donation_total"] else "0.00"

    goal = _dec(row["goal_amount"])
    raised = _dec(row["raised_amount"])
    if goal > Decimal("0"):
        campaign_data["percent_of_goal"] = str(_round(raised / goal * Decimal("100")))

    return ok({"campaign": campaign_data})


def activate_campaign(conn, args):
    campaign_id = args.id
    if not campaign_id:
        return err("--id is required")

    q = Q.from_(_campaign).select(_campaign.id, _campaign.company_id, _campaign.status).where(_campaign.id == P())
    row = conn.execute(q.get_sql(), (campaign_id,)).fetchone()
    if not row:
        return err(f"Campaign {campaign_id} not found")
    if row["status"] != "draft":
        return err(f"Campaign must be in 'draft' status to activate, currently '{row['status']}'")

    sql, params = dynamic_update("nonprofitclaw_campaign",
        {"status": "active", "updated_at": now()},
        where={"id": campaign_id})
    conn.execute(sql, params)
    conn.commit()
    audit(conn, SKILL, "nonprofit-activate-campaign", campaign_id, row["company_id"])
    return ok({"id": campaign_id, "campaign_status": "active"})


def close_campaign(conn, args):
    campaign_id = args.id
    if not campaign_id:
        return err("--id is required")

    q = Q.from_(_campaign).select(_campaign.star).where(_campaign.id == P())
    row = conn.execute(q.get_sql(), (campaign_id,)).fetchone()
    if not row:
        return err(f"Campaign {campaign_id} not found")
    if row["status"] in ("completed", "cancelled"):
        return err(f"Campaign is already '{row['status']}'")

    # Lapse active pledges
    sql_lapse, params_lapse = dynamic_update("nonprofitclaw_pledge",
        {"status": "lapsed", "updated_at": now()},
        where={"campaign_id": campaign_id, "status": "active"})
    conn.execute(sql_lapse, params_lapse)

    sql_c, params_c = dynamic_update("nonprofitclaw_campaign",
        {"status": "completed", "updated_at": now()},
        where={"id": campaign_id})
    conn.execute(sql_c, params_c)
    conn.commit()
    audit(conn, SKILL, "nonprofit-close-campaign", campaign_id, row["company_id"])
    return ok({
        "id": campaign_id,
        "campaign_status": "completed",
        "raised_amount": row["raised_amount"],
        "goal_amount": row["goal_amount"],
    })


# ------------------------------------------------------------------
# Pledge CRUD
# ------------------------------------------------------------------

def add_pledge(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    donor_id = getattr(args, "donor_id", None)
    if not donor_id:
        return err("--donor-id is required")

    dq = Q.from_(_de).select(_de.id, _de.company_id).where(_de.id == P())
    donor = conn.execute(dq.get_sql(), (donor_id,)).fetchone()
    if not donor:
        return err(f"Donor {donor_id} not found")
    if donor["company_id"] != company_id:
        return err("Donor does not belong to this company")

    amount_str = getattr(args, "amount", None)
    if not amount_str:
        return err("--amount is required")
    amount = _round(_dec(amount_str))
    if amount <= Decimal("0"):
        return err("Amount must be positive")

    campaign_id = getattr(args, "campaign_id", None)
    if campaign_id:
        cq = Q.from_(_campaign).select(_campaign.id, _campaign.status).where(_campaign.id == P())
        campaign = conn.execute(cq.get_sql(), (campaign_id,)).fetchone()
        if not campaign:
            return err(f"Campaign {campaign_id} not found")
        if campaign["status"] != "active":
            return err(f"Campaign must be 'active' to accept pledges, currently '{campaign['status']}'")

    fund_id = getattr(args, "fund_id", None)
    if fund_id:
        fq = Q.from_(_fund).select(_fund.id).where(_fund.id == P())
        if not conn.execute(fq.get_sql(), (fund_id,)).fetchone():
            return err(f"Fund {fund_id} not found")

    pledge_id = str(uuid.uuid4())
    naming = get_next_name(conn, "nonprofitclaw_pledge", company_id=company_id)
    frequency = getattr(args, "frequency", None) or "one_time"

    sql, _ = insert_row("nonprofitclaw_pledge", {
        "id": P(), "naming_series": P(), "donor_id": P(), "campaign_id": P(),
        "fund_id": P(), "pledge_date": P(), "amount": P(), "frequency": P(),
        "next_due_date": P(), "end_date": P(), "notes": P(), "status": P(),
        "company_id": P(),
    })
    conn.execute(sql, (
        pledge_id, naming, donor_id, campaign_id, fund_id,
        getattr(args, "pledge_date", None) or str(date.today()),
        str(amount), frequency,
        getattr(args, "next_due_date", None),
        getattr(args, "end_date", None),
        getattr(args, "notes", None),
        "active", company_id,
    ))
    conn.commit()
    audit(conn, SKILL, "nonprofit-add-pledge", pledge_id, company_id)
    return ok({"id": pledge_id, "naming_series": naming, "amount": str(amount)})


def list_pledges(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    limit = int(getattr(args, "limit", None) or 50)
    offset = int(getattr(args, "offset", None) or 0)

    pl = _pledge
    de = _de
    cust = _c
    camp = _campaign

    conditions = [pl.company_id == P()]
    params = [company_id]

    donor_id = getattr(args, "donor_id", None)
    if donor_id:
        conditions.append(pl.donor_id == P())
        params.append(donor_id)

    campaign_id = getattr(args, "campaign_id", None)
    if campaign_id:
        conditions.append(pl.campaign_id == P())
        params.append(campaign_id)

    status = getattr(args, "status", None)
    if status:
        conditions.append(pl.status == P())
        params.append(status)

    count_q = Q.from_(pl).select(fn.Count("*"))
    for cond in conditions:
        count_q = count_q.where(cond)
    total = conn.execute(count_q.get_sql(), params).fetchone()[0]

    data_q = (
        Q.from_(pl)
        .left_join(de).on(pl.donor_id == de.id)
        .left_join(cust).on(de.customer_id == cust.id)
        .left_join(camp).on(pl.campaign_id == camp.id)
        .select(
            pl.id, pl.naming_series, pl.donor_id, cust.name.as_("donor_name"),
            pl.campaign_id, camp.name.as_("campaign_name"),
            pl.pledge_date, pl.amount, pl.fulfilled_amount,
            pl.frequency, pl.next_due_date, pl.status,
        )
    )
    for cond in conditions:
        data_q = data_q.where(cond)
    data_q = data_q.orderby(pl.pledge_date, order=Order.desc).limit(P()).offset(P())

    rows = conn.execute(data_q.get_sql(), params + [limit, offset]).fetchall()

    pledges = []
    for r in rows:
        p = dict(r)
        amt = _dec(r["amount"])
        fulfilled = _dec(r["fulfilled_amount"])
        p["remaining"] = str(_round(amt - fulfilled))
        if amt > Decimal("0"):
            p["percent_fulfilled"] = str(_round(fulfilled / amt * Decimal("100")))
        pledges.append(p)

    return ok({"pledges": pledges, "total": total})


def get_pledge(conn, args):
    pledge_id = args.id
    if not pledge_id:
        return err("--id is required")

    q = (
        Q.from_(_pledge)
        .left_join(_de).on(_pledge.donor_id == _de.id)
        .left_join(_c).on(_de.customer_id == _c.id)
        .left_join(_campaign).on(_pledge.campaign_id == _campaign.id)
        .select(_pledge.star, _c.name.as_("donor_name"), _campaign.name.as_("campaign_name"))
        .where(_pledge.id == P())
    )
    row = conn.execute(q.get_sql(), (pledge_id,)).fetchone()
    if not row:
        return err(f"Pledge {pledge_id} not found")

    pledge_data = dict(row)
    amt = _dec(row["amount"])
    fulfilled = _dec(row["fulfilled_amount"])
    pledge_data["remaining"] = str(_round(amt - fulfilled))
    if amt > Decimal("0"):
        pledge_data["percent_fulfilled"] = str(_round(fulfilled / amt * Decimal("100")))

    return ok({"pledge": pledge_data})


def fulfill_pledge(conn, args):
    pledge_id = getattr(args, "pledge_id", None) or args.id
    if not pledge_id:
        return err("--pledge-id or --id is required")

    q = Q.from_(_pledge).select(_pledge.star).where(_pledge.id == P())
    row = conn.execute(q.get_sql(), (pledge_id,)).fetchone()
    if not row:
        return err(f"Pledge {pledge_id} not found")
    if row["status"] in ("fulfilled", "cancelled", "lapsed"):
        return err(f"Cannot fulfill pledge in '{row['status']}' status")

    amount_str = getattr(args, "amount", None)
    if not amount_str:
        return err("--amount is required (fulfillment amount)")
    amount = _round(_dec(amount_str))
    if amount <= Decimal("0"):
        return err("Amount must be positive")

    pledge_amount = _dec(row["amount"])
    old_fulfilled = _dec(row["fulfilled_amount"])
    new_fulfilled = _round(old_fulfilled + amount)

    if new_fulfilled > pledge_amount:
        return err(f"Fulfillment would exceed pledge amount. Pledge: {str(pledge_amount)}, Already fulfilled: {str(old_fulfilled)}, This payment: {str(amount)}")

    if new_fulfilled == pledge_amount:
        new_status = "fulfilled"
    else:
        new_status = "partially_fulfilled"

    # Transaction (implicit)
    try:
        sql_f, params_f = dynamic_update("nonprofitclaw_pledge",
            {"fulfilled_amount": str(new_fulfilled), "status": new_status,
             "updated_at": now()},
            where={"id": pledge_id})
        conn.execute(sql_f, params_f)

        # Update campaign raised_amount if linked
        campaign_id = row["campaign_id"]
        if campaign_id:
            ct = Table("nonprofitclaw_campaign")
            camp_upd = (
                Q.update(ct)
                .set(ct.raised_amount, LiteralValue("CAST(CAST(raised_amount AS NUMERIC) + ? AS TEXT)"))
                .set(ct.updated_at, now())
                .where(ct.id == P())
            )
            conn.execute(camp_upd.get_sql(), (float(amount), campaign_id))

        conn.commit()
    except Exception as e:
        conn.rollback()
        return err(f"Fulfillment failed: {e}")

    audit(conn, SKILL, "nonprofit-fulfill-pledge", pledge_id, row["company_id"])
    return ok({
        "id": pledge_id,
        "fulfilled_amount": str(new_fulfilled),
        "pledge_status": new_status,
        "this_payment": str(amount),
        "remaining": str(_round(pledge_amount - new_fulfilled)),
    })


def cancel_pledge(conn, args):
    pledge_id = args.id
    if not pledge_id:
        return err("--id is required")

    q = Q.from_(_pledge).select(_pledge.id, _pledge.company_id, _pledge.status).where(_pledge.id == P())
    row = conn.execute(q.get_sql(), (pledge_id,)).fetchone()
    if not row:
        return err(f"Pledge {pledge_id} not found")
    if row["status"] in ("fulfilled", "cancelled"):
        return err(f"Cannot cancel pledge in '{row['status']}' status")

    sql, params = dynamic_update("nonprofitclaw_pledge",
        {"status": "cancelled", "updated_at": now()},
        where={"id": pledge_id})
    conn.execute(sql, params)
    conn.commit()
    audit(conn, SKILL, "nonprofit-cancel-pledge", pledge_id, row["company_id"])
    return ok({"id": pledge_id, "pledge_status": "cancelled"})


ACTIONS = {
    "nonprofit-add-campaign": add_campaign,
    "nonprofit-update-campaign": update_campaign,
    "nonprofit-list-campaigns": list_campaigns,
    "nonprofit-get-campaign": get_campaign,
    "nonprofit-activate-campaign": activate_campaign,
    "nonprofit-close-campaign": close_campaign,
    "nonprofit-add-pledge": add_pledge,
    "nonprofit-list-pledges": list_pledges,
    "nonprofit-get-pledge": get_pledge,
    "nonprofit-fulfill-pledge": fulfill_pledge,
    "nonprofit-cancel-pledge": cancel_pledge,
}
