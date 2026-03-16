#!/usr/bin/env python3
"""NonprofitClaw compliance domain — 4 actions."""
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
_de = Table("nonprofitclaw_donor_ext")
_c = Table("customer")
_don = Table("nonprofitclaw_donation")
_receipt = Table("nonprofitclaw_tax_receipt")
_fund = Table("nonprofitclaw_fund")
_ft = Table("nonprofitclaw_fund_transfer")
_grant = Table("nonprofitclaw_grant")
_ge = Table("nonprofitclaw_grant_expense")
_prog = Table("nonprofitclaw_program")
_vol = Table("nonprofitclaw_volunteer")
_vs = Table("nonprofitclaw_volunteer_shift")
_pledge = Table("nonprofitclaw_pledge")
_campaign = Table("nonprofitclaw_campaign")


def _dec(val):
    if val is None:
        return Decimal("0")
    return Decimal(str(val))


def _round(val):
    return val.quantize(Decimal("0.01"), ROUND_HALF_UP)


# ------------------------------------------------------------------
# Tax Receipts
# ------------------------------------------------------------------

def generate_tax_receipt(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    donor_id = getattr(args, "donor_id", None)
    if not donor_id:
        return err("--donor-id is required")

    q = (
        Q.from_(_de)
        .join(_c).on(_de.customer_id == _c.id)
        .select(_de.id, _c.name, _de.company_id)
        .where(_de.id == P())
    )
    donor = conn.execute(q.get_sql(), (donor_id,)).fetchone()
    if not donor:
        return err(f"Donor {donor_id} not found")
    if donor["company_id"] != company_id:
        return err("Donor does not belong to this company")

    tax_year = getattr(args, "tax_year", None)
    if not tax_year:
        return err("--tax-year is required")

    receipt_type = getattr(args, "receipt_type", None) or "single"
    donation_id = getattr(args, "donation_id", None)
    sent_method = getattr(args, "sent_method", None)

    if receipt_type == "single":
        # Single donation receipt
        if not donation_id:
            return err("--donation-id is required for single receipt type")

        dq = (
            Q.from_(_don)
            .select(_don.id, _don.amount, _don.tax_deductible, _don.status)
            .where(_don.id == P())
            .where(_don.donor_id == P())
        )
        donation = conn.execute(dq.get_sql(), (donation_id, donor_id)).fetchone()
        if not donation:
            return err(f"Donation {donation_id} not found for this donor")
        if donation["status"] in ("refunded", "cancelled"):
            return err(f"Cannot issue receipt for '{donation['status']}' donation")
        if not donation["tax_deductible"]:
            return err("Donation is not tax-deductible")

        amount = donation["amount"]

        # Check for duplicate receipt
        dup_q = Q.from_(_receipt).select(_receipt.id).where(_receipt.donation_id == P())
        existing = conn.execute(dup_q.get_sql(), (donation_id,)).fetchone()
        if existing:
            return err(f"Tax receipt already exists for this donation: {existing['id']}")

    elif receipt_type == "annual_summary":
        # Annual summary receipt — aggregate all deductible donations for the year
        total_q = (
            Q.from_(_don)
            .select(LiteralValue("SUM(CAST(amount AS REAL))").as_("total"))
            .where(_don.donor_id == P())
            .where(_don.company_id == P())
            .where(_don.tax_deductible == 1)
            .where(_don.status.notin(["refunded", "cancelled"]))
            .where(LiteralValue("strftime('%Y', donation_date)") == P())
        )
        total_row = conn.execute(total_q.get_sql(), (donor_id, company_id, tax_year)).fetchone()

        if not total_row["total"]:
            return err(f"No tax-deductible donations found for donor in {tax_year}")

        amount = str(_round(_dec(total_row["total"])))
        donation_id = None  # No single donation for annual summary
    else:
        return err(f"Invalid receipt_type: {receipt_type}")

    receipt_id = str(uuid.uuid4())
    naming = get_next_name(conn, "nonprofitclaw_tax_receipt", company_id=company_id)

    sql, _ = insert_row("nonprofitclaw_tax_receipt", {
        "id": P(), "naming_series": P(), "donor_id": P(), "donation_id": P(),
        "receipt_date": P(), "amount": P(), "tax_year": P(), "receipt_type": P(),
        "sent_date": P(), "sent_method": P(), "company_id": P(),
    })
    conn.execute(sql, (
        receipt_id, naming, donor_id, donation_id,
        str(date.today()), amount, tax_year, receipt_type,
        str(date.today()) if sent_method else None,
        sent_method, company_id,
    ))

    # Mark donation as receipt_sent if single
    if donation_id:
        sql_u, params_u = dynamic_update("nonprofitclaw_donation",
            {"receipt_sent": 1, "updated_at": LiteralValue("datetime('now')")},
            where={"id": donation_id})
        conn.execute(sql_u, params_u)

    conn.commit()
    audit(conn, SKILL, "nonprofit-generate-tax-receipt", receipt_id, company_id)
    return ok({
        "id": receipt_id,
        "naming_series": naming,
        "donor_name": donor["name"],
        "amount": amount,
        "tax_year": tax_year,
        "receipt_type": receipt_type,
    })


def list_tax_receipts(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    limit = int(getattr(args, "limit", None) or 50)
    offset = int(getattr(args, "offset", None) or 0)

    tr = _receipt
    de = _de
    cust = _c

    conditions = [tr.company_id == P()]
    params = [company_id]

    donor_id = getattr(args, "donor_id", None)
    if donor_id:
        conditions.append(tr.donor_id == P())
        params.append(donor_id)

    tax_year = getattr(args, "tax_year", None)
    if tax_year:
        conditions.append(tr.tax_year == P())
        params.append(tax_year)

    receipt_type = getattr(args, "receipt_type", None)
    if receipt_type:
        conditions.append(tr.receipt_type == P())
        params.append(receipt_type)

    count_q = Q.from_(tr).select(fn.Count("*"))
    for cond in conditions:
        count_q = count_q.where(cond)
    total = conn.execute(count_q.get_sql(), params).fetchone()[0]

    data_q = (
        Q.from_(tr)
        .left_join(de).on(tr.donor_id == de.id)
        .left_join(cust).on(de.customer_id == cust.id)
        .select(
            tr.id, tr.naming_series, tr.donor_id, cust.name.as_("donor_name"),
            tr.donation_id, tr.receipt_date, tr.amount, tr.tax_year,
            tr.receipt_type, tr.sent_date, tr.sent_method,
        )
    )
    for cond in conditions:
        data_q = data_q.where(cond)
    data_q = data_q.orderby(tr.receipt_date, order=Order.desc).limit(P()).offset(P())

    rows = conn.execute(data_q.get_sql(), params + [limit, offset]).fetchall()
    receipts = [dict(r) for r in rows]
    return ok({"tax_receipts": receipts, "total": total})


# ------------------------------------------------------------------
# Donor Summary
# ------------------------------------------------------------------

def donor_summary(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    # Overall donor stats
    ds_q = (
        Q.from_(_de)
        .select(
            fn.Count("*").as_("total_donors"),
            LiteralValue("SUM(CASE WHEN is_active=1 THEN 1 ELSE 0 END)").as_("active_donors"),
            LiteralValue("SUM(CASE WHEN donor_type='individual' THEN 1 ELSE 0 END)").as_("individual_donors"),
            LiteralValue("SUM(CASE WHEN donor_type='corporate' THEN 1 ELSE 0 END)").as_("org_donors"),
            LiteralValue("SUM(CASE WHEN donor_type='foundation' THEN 1 ELSE 0 END)").as_("foundation_donors"),
        )
        .where(_de.company_id == P())
    )
    donor_stats = conn.execute(ds_q.get_sql(), (company_id,)).fetchone()

    # Donation stats
    don_q = (
        Q.from_(_don)
        .select(
            fn.Count("*").as_("total_donations"),
            LiteralValue("SUM(CAST(amount AS REAL))").as_("total_amount"),
            LiteralValue("AVG(CAST(amount AS REAL))").as_("avg_donation"),
        )
        .where(_don.company_id == P())
        .where(_don.status.notin(["refunded", "cancelled"]))
    )
    donation_stats = conn.execute(don_q.get_sql(), (company_id,)).fetchone()

    # Donor level breakdown
    level_q = (
        Q.from_(_de)
        .select(_de.donor_level, fn.Count("*").as_("count"))
        .where(_de.company_id == P())
        .where(_de.is_active == 1)
        .groupby(_de.donor_level)
        .orderby(Field("count"), order=Order.desc)
    )
    level_breakdown = conn.execute(level_q.get_sql(), (company_id,)).fetchall()

    # Top donors
    top_q = (
        Q.from_(_de)
        .join(_c).on(_de.customer_id == _c.id)
        .select(
            _de.id, _c.name, _de.donor_type, _de.total_donated,
            _de.donation_count, _de.donor_level,
        )
        .where(_de.company_id == P())
        .where(_de.is_active == 1)
        .orderby(LiteralValue("CAST(\"nonprofitclaw_donor_ext\".\"total_donated\" AS REAL)"), order=Order.desc)
        .limit(10)
    )
    top_donors = conn.execute(top_q.get_sql(), (company_id,)).fetchall()

    # Monthly trend (last 12 months)
    trend_q = (
        Q.from_(_don)
        .select(
            LiteralValue("strftime('%Y-%m', donation_date)").as_("month"),
            fn.Count("*").as_("count"),
            LiteralValue("SUM(CAST(amount AS REAL))").as_("total"),
        )
        .where(_don.company_id == P())
        .where(_don.status.notin(["refunded", "cancelled"]))
        .where(_don.donation_date >= LiteralValue("date('now', '-12 months')"))
        .groupby(LiteralValue("month"))
        .orderby(LiteralValue("month"))
    )
    monthly_trend = conn.execute(trend_q.get_sql(), (company_id,)).fetchall()

    trend = []
    for m in monthly_trend:
        trend.append({
            "month": m["month"],
            "count": m["count"],
            "total": str(_round(_dec(m["total"]))) if m["total"] else "0.00",
        })

    return ok({
        "total_donors": donor_stats["total_donors"] or 0,
        "active_donors": donor_stats["active_donors"] or 0,
        "individual_donors": donor_stats["individual_donors"] or 0,
        "organization_donors": donor_stats["org_donors"] or 0,
        "foundation_donors": donor_stats["foundation_donors"] or 0,
        "total_donations": donation_stats["total_donations"] or 0,
        "total_donated": str(_round(_dec(donation_stats["total_amount"]))) if donation_stats["total_amount"] else "0.00",
        "average_donation": str(_round(_dec(donation_stats["avg_donation"]))) if donation_stats["avg_donation"] else "0.00",
        "donor_levels": [dict(l) for l in level_breakdown],
        "top_donors": [dict(d) for d in top_donors],
        "monthly_trend": trend,
    })


# ------------------------------------------------------------------
# Module Status
# ------------------------------------------------------------------

def module_status(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    counts = {}
    tables = [
        ("donors", "nonprofitclaw_donor_ext"),
        ("donations", "nonprofitclaw_donation"),
        ("funds", "nonprofitclaw_fund"),
        ("fund_transfers", "nonprofitclaw_fund_transfer"),
        ("grants", "nonprofitclaw_grant"),
        ("grant_expenses", "nonprofitclaw_grant_expense"),
        ("programs", "nonprofitclaw_program"),
        ("volunteers", "nonprofitclaw_volunteer"),
        ("volunteer_shifts", "nonprofitclaw_volunteer_shift"),
        ("pledges", "nonprofitclaw_pledge"),
        ("campaigns", "nonprofitclaw_campaign"),
        ("tax_receipts", "nonprofitclaw_tax_receipt"),
    ]

    for label, table_name in tables:
        try:
            t = Table(table_name)
            q = Q.from_(t).select(fn.Count("*")).where(Field("company_id") == P())
            row = conn.execute(q.get_sql(), (company_id,)).fetchone()
            counts[label] = row[0]
        except Exception:
            counts[label] = 0

    # Total donations amount
    total_q = (
        Q.from_(_don)
        .select(LiteralValue("SUM(CAST(amount AS REAL))"))
        .where(_don.company_id == P())
        .where(_don.status.notin(["refunded", "cancelled"]))
    )
    total_donated = conn.execute(total_q.get_sql(), (company_id,)).fetchone()[0]

    # Active grants
    ag_q = (
        Q.from_(_grant)
        .select(fn.Count("*"))
        .where(_grant.company_id == P())
        .where(_grant.status == "active")
    )
    active_grants = conn.execute(ag_q.get_sql(), (company_id,)).fetchone()[0]

    # Active campaigns
    ac_q = (
        Q.from_(_campaign)
        .select(fn.Count("*"))
        .where(_campaign.company_id == P())
        .where(_campaign.status == "active")
    )
    active_campaigns = conn.execute(ac_q.get_sql(), (company_id,)).fetchone()[0]

    return ok({
        "module": "nonprofitclaw",
        "module_status": "operational",
        "record_counts": counts,
        "total_donated": str(_round(_dec(total_donated))) if total_donated else "0.00",
        "active_grants": active_grants,
        "active_campaigns": active_campaigns,
    })


ACTIONS = {
    "nonprofit-generate-tax-receipt": generate_tax_receipt,
    "nonprofit-list-tax-receipts": list_tax_receipts,
    "nonprofit-donor-summary": donor_summary,
    "status": module_status,
}
