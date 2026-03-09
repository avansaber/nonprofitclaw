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

SKILL = "nonprofitclaw"


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
    naming = get_next_name(conn, "campaign", company_id=company_id)

    goal_amount = getattr(args, "goal_amount", None)
    if goal_amount:
        goal_amount = str(_round(_dec(goal_amount)))
    else:
        goal_amount = "0"

    fund_id = getattr(args, "fund_id", None)
    if fund_id:
        fund = conn.execute("SELECT id FROM nonprofitclaw_fund WHERE id=?", (fund_id,)).fetchone()
        if not fund:
            return err(f"Fund {fund_id} not found")

    conn.execute(
        """INSERT INTO nonprofitclaw_campaign
           (id, naming_series, name, description, fund_id, goal_amount,
            start_date, end_date, status, company_id)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (
            campaign_id, naming, name,
            getattr(args, "description", None),
            fund_id, goal_amount,
            getattr(args, "start_date", None),
            getattr(args, "end_date", None),
            "draft", company_id,
        ),
    )
    conn.commit()
    audit(conn, SKILL, "nonprofit-add-campaign", campaign_id, company_id)
    return ok({"id": campaign_id, "naming_series": naming, "name": name})


def update_campaign(conn, args):
    campaign_id = args.id
    if not campaign_id:
        return err("--id is required")

    row = conn.execute("SELECT id, company_id, status FROM nonprofitclaw_campaign WHERE id=?", (campaign_id,)).fetchone()
    if not row:
        return err(f"Campaign {campaign_id} not found")
    if row["status"] in ("completed", "cancelled"):
        return err(f"Cannot update campaign in '{row['status']}' status")

    fields = []
    values = []
    for col, attr in [
        ("name", "name"), ("description", "description"),
        ("start_date", "start_date"), ("end_date", "end_date"),
    ]:
        val = getattr(args, attr, None)
        if val is not None:
            fields.append(f"{col}=?")
            values.append(val)

    goal_amount = getattr(args, "goal_amount", None)
    if goal_amount is not None:
        fields.append("goal_amount=?")
        values.append(str(_round(_dec(goal_amount))))

    fund_id = getattr(args, "fund_id", None)
    if fund_id is not None:
        if fund_id:
            fund = conn.execute("SELECT id FROM nonprofitclaw_fund WHERE id=?", (fund_id,)).fetchone()
            if not fund:
                return err(f"Fund {fund_id} not found")
        fields.append("fund_id=?")
        values.append(fund_id if fund_id else None)

    if not fields:
        return err("No fields to update")

    fields.append("updated_at=datetime('now')")
    values.append(campaign_id)
    conn.execute(f"UPDATE nonprofitclaw_campaign SET {', '.join(fields)} WHERE id=?", values)
    conn.commit()
    audit(conn, SKILL, "nonprofit-update-campaign", campaign_id, row["company_id"])
    return ok({"id": campaign_id, "updated": True})


def list_campaigns(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    limit = int(getattr(args, "limit", None) or 50)
    offset = int(getattr(args, "offset", None) or 0)

    where = ["company_id=?"]
    params = [company_id]

    status = getattr(args, "status", None)
    if status:
        where.append("status=?")
        params.append(status)

    search = getattr(args, "search", None)
    if search:
        where.append("name LIKE ?")
        params.append(f"%{search}%")

    where_sql = " AND ".join(where)

    total = conn.execute(
        f"SELECT COUNT(*) FROM nonprofitclaw_campaign WHERE {where_sql}", params
    ).fetchone()[0]

    rows = conn.execute(
        f"""SELECT id, naming_series, name, description, fund_id,
                   goal_amount, raised_amount, donor_count,
                   start_date, end_date, status
            FROM nonprofitclaw_campaign WHERE {where_sql}
            ORDER BY created_at DESC LIMIT ? OFFSET ?""",
        params + [limit, offset],
    ).fetchall()

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

    row = conn.execute("SELECT * FROM nonprofitclaw_campaign WHERE id=?", (campaign_id,)).fetchone()
    if not row:
        return err(f"Campaign {campaign_id} not found")

    # Get pledge summary
    pledge_stats = conn.execute(
        """SELECT COUNT(*) as pledge_count,
                  SUM(CAST(amount AS REAL)) as pledged_total,
                  SUM(CAST(fulfilled_amount AS REAL)) as fulfilled_total
           FROM nonprofitclaw_pledge
           WHERE campaign_id=? AND status != 'cancelled'""",
        (campaign_id,),
    ).fetchone()

    # Get donation summary
    donation_stats = conn.execute(
        """SELECT COUNT(*) as donation_count,
                  SUM(CAST(amount AS REAL)) as donation_total
           FROM nonprofitclaw_donation
           WHERE campaign_id=? AND status NOT IN ('refunded','cancelled')""",
        (campaign_id,),
    ).fetchone()

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

    row = conn.execute("SELECT id, company_id, status FROM nonprofitclaw_campaign WHERE id=?", (campaign_id,)).fetchone()
    if not row:
        return err(f"Campaign {campaign_id} not found")
    if row["status"] != "draft":
        return err(f"Campaign must be in 'draft' status to activate, currently '{row['status']}'")

    conn.execute(
        "UPDATE nonprofitclaw_campaign SET status='active', updated_at=datetime('now') WHERE id=?",
        (campaign_id,),
    )
    conn.commit()
    audit(conn, SKILL, "nonprofit-activate-campaign", campaign_id, row["company_id"])
    return ok({"id": campaign_id, "campaign_status": "active"})


def close_campaign(conn, args):
    campaign_id = args.id
    if not campaign_id:
        return err("--id is required")

    row = conn.execute("SELECT * FROM nonprofitclaw_campaign WHERE id=?", (campaign_id,)).fetchone()
    if not row:
        return err(f"Campaign {campaign_id} not found")
    if row["status"] in ("completed", "cancelled"):
        return err(f"Campaign is already '{row['status']}'")

    # Lapse active pledges
    conn.execute(
        "UPDATE nonprofitclaw_pledge SET status='lapsed', updated_at=datetime('now') WHERE campaign_id=? AND status='active'",
        (campaign_id,),
    )

    conn.execute(
        "UPDATE nonprofitclaw_campaign SET status='completed', updated_at=datetime('now') WHERE id=?",
        (campaign_id,),
    )
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

    donor = conn.execute("SELECT id, company_id FROM nonprofitclaw_donor WHERE id=?", (donor_id,)).fetchone()
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
        campaign = conn.execute("SELECT id, status FROM nonprofitclaw_campaign WHERE id=?", (campaign_id,)).fetchone()
        if not campaign:
            return err(f"Campaign {campaign_id} not found")
        if campaign["status"] != "active":
            return err(f"Campaign must be 'active' to accept pledges, currently '{campaign['status']}'")

    fund_id = getattr(args, "fund_id", None)
    if fund_id:
        fund = conn.execute("SELECT id FROM nonprofitclaw_fund WHERE id=?", (fund_id,)).fetchone()
        if not fund:
            return err(f"Fund {fund_id} not found")

    pledge_id = str(uuid.uuid4())
    naming = get_next_name(conn, "pledge", company_id=company_id)
    frequency = getattr(args, "frequency", None) or "one_time"

    conn.execute(
        """INSERT INTO nonprofitclaw_pledge
           (id, naming_series, donor_id, campaign_id, fund_id,
            pledge_date, amount, frequency, next_due_date,
            end_date, notes, status, company_id)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            pledge_id, naming, donor_id, campaign_id, fund_id,
            getattr(args, "pledge_date", None) or str(date.today()),
            str(amount), frequency,
            getattr(args, "next_due_date", None),
            getattr(args, "end_date", None),
            getattr(args, "notes", None),
            "active", company_id,
        ),
    )
    conn.commit()
    audit(conn, SKILL, "nonprofit-add-pledge", pledge_id, company_id)
    return ok({"id": pledge_id, "naming_series": naming, "amount": str(amount)})


def list_pledges(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    limit = int(getattr(args, "limit", None) or 50)
    offset = int(getattr(args, "offset", None) or 0)

    where = ["pl.company_id=?"]
    params = [company_id]

    donor_id = getattr(args, "donor_id", None)
    if donor_id:
        where.append("pl.donor_id=?")
        params.append(donor_id)

    campaign_id = getattr(args, "campaign_id", None)
    if campaign_id:
        where.append("pl.campaign_id=?")
        params.append(campaign_id)

    status = getattr(args, "status", None)
    if status:
        where.append("pl.status=?")
        params.append(status)

    where_sql = " AND ".join(where)

    total = conn.execute(
        f"SELECT COUNT(*) FROM nonprofitclaw_pledge pl WHERE {where_sql}", params
    ).fetchone()[0]

    rows = conn.execute(
        f"""SELECT pl.id, pl.naming_series, pl.donor_id, d.name as donor_name,
                   pl.campaign_id, c.name as campaign_name,
                   pl.pledge_date, pl.amount, pl.fulfilled_amount,
                   pl.frequency, pl.next_due_date, pl.status
            FROM nonprofitclaw_pledge pl
            LEFT JOIN nonprofitclaw_donor d ON pl.donor_id = d.id
            LEFT JOIN nonprofitclaw_campaign c ON pl.campaign_id = c.id
            WHERE {where_sql}
            ORDER BY pl.pledge_date DESC LIMIT ? OFFSET ?""",
        params + [limit, offset],
    ).fetchall()

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

    row = conn.execute(
        """SELECT pl.*, d.name as donor_name, c.name as campaign_name
           FROM nonprofitclaw_pledge pl
           LEFT JOIN nonprofitclaw_donor d ON pl.donor_id = d.id
           LEFT JOIN nonprofitclaw_campaign c ON pl.campaign_id = c.id
           WHERE pl.id=?""",
        (pledge_id,),
    ).fetchone()
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

    row = conn.execute("SELECT * FROM nonprofitclaw_pledge WHERE id=?", (pledge_id,)).fetchone()
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
        conn.execute(
            """UPDATE nonprofitclaw_pledge SET
                fulfilled_amount=?, status=?,
                updated_at=datetime('now')
               WHERE id=?""",
            (str(new_fulfilled), new_status, pledge_id),
        )

        # Update campaign raised_amount if linked
        campaign_id = row["campaign_id"]
        if campaign_id:
            conn.execute(
                """UPDATE nonprofitclaw_campaign SET
                    raised_amount = CAST(
                        CAST(raised_amount AS REAL) + ? AS TEXT
                    ), updated_at=datetime('now')
                   WHERE id=?""",
                (float(amount), campaign_id),
            )

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

    row = conn.execute("SELECT id, company_id, status FROM nonprofitclaw_pledge WHERE id=?", (pledge_id,)).fetchone()
    if not row:
        return err(f"Pledge {pledge_id} not found")
    if row["status"] in ("fulfilled", "cancelled"):
        return err(f"Cannot cancel pledge in '{row['status']}' status")

    conn.execute(
        "UPDATE nonprofitclaw_pledge SET status='cancelled', updated_at=datetime('now') WHERE id=?",
        (pledge_id,),
    )
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
