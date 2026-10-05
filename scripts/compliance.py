#!/usr/bin/env python3
"""NonprofitClaw compliance domain — 4 actions."""
import json
import os
import sys
import uuid
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP

import importlib.util
if importlib.util.find_spec("erpclaw_lib") is None:
    sys.path.insert(0, os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))
from erpclaw_lib.naming import get_next_name
from erpclaw_lib.response import ok, err
from erpclaw_lib.audit import audit
from erpclaw_lib.query import (
    Q, P, Table, Field, fn, Order, LiteralValue,
    insert_row, update_row, dynamic_update, date_format as sql_date_format, now as sql_now,
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


QUID_PRO_QUO_THRESHOLD = Decimal("75.00")
ACKNOWLEDGMENT_THRESHOLD = Decimal("250.00")


def _substantiation(amount_dec, goods_fv_dec, goods_desc):
    goods_fv = _round(goods_fv_dec)
    deductible = _round(amount_dec - goods_fv)
    provided = goods_fv > Decimal("0")
    goods_fv_str = str(goods_fv)
    amount_str = str(_round(amount_dec))
    statements = []
    if amount_dec > QUID_PRO_QUO_THRESHOLD and provided:
        statements.append(
            "Quid pro quo disclosure: goods or services with a fair value of "
            + goods_fv_str + " were provided in exchange for this payment of "
            + amount_str + "; only the excess of the payment over that value is deductible."
        )
    if amount_dec >= ACKNOWLEDGMENT_THRESHOLD:
        if provided:
            desc_part = " (" + str(goods_desc) + ")" if goods_desc else ""
            statements.append(
                "Acknowledgment: goods or services with a fair value of "
                + goods_fv_str + desc_part
                + " were provided in exchange for this donation."
            )
        else:
            statements.append(
                "Acknowledgment: no goods or services were provided in exchange for this donation."
            )
    return deductible, provided, statements


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
            .select(_don.id, _don.amount, _don.donation_date, _don.tax_deductible, _don.status,
                    _don.goods_services_fair_value, _don.goods_services_description)
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

        # A donation covered by an annual summary gets no single receipt
        year = str(donation["donation_date"])[:4]
        cover_q = (
            Q.from_(_receipt)
            .select(_receipt.id)
            .where(_receipt.donor_id == P())
            .where(_receipt.company_id == P())
            .where(_receipt.tax_year == P())
            .where(_receipt.receipt_type == P())
        )
        covering = conn.execute(cover_q.get_sql(), (donor_id, company_id, year, "annual_summary")).fetchone()
        if covering:
            return err(f"Donation {donation_id} is already covered by annual summary receipt {covering['id']} for {year}")

    elif receipt_type == "annual_summary":
        # Annual summary receipt — aggregate all deductible donations for the year
        dup_ann_q = (
            Q.from_(_receipt)
            .select(_receipt.id)
            .where(_receipt.donor_id == P())
            .where(_receipt.company_id == P())
            .where(_receipt.tax_year == P())
            .where(_receipt.receipt_type == P())
        )
        existing_annual = conn.execute(dup_ann_q.get_sql(), (donor_id, company_id, tax_year, "annual_summary")).fetchone()
        if existing_annual:
            return err(f"An annual summary receipt already exists for this donor in {tax_year}: {existing_annual['id']}")

        qual_q = (
            Q.from_(_don)
            .select(_don.id, _don.amount, _don.goods_services_fair_value,
                    _don.goods_services_description)
            .where(_don.donor_id == P())
            .where(_don.company_id == P())
            .where(_don.tax_deductible == 1)
            .where(_don.status.notin(["refunded", "cancelled"]))
            .where(sql_date_format("donation_date", "%Y") == P())
        )
        qualifying = conn.execute(qual_q.get_sql(), (donor_id, company_id, tax_year)).fetchall()

        if not qualifying:
            return err(f"No tax-deductible donations found for donor in {tax_year}")

        rec_q = (
            Q.from_(_receipt)
            .select(_receipt.donation_id)
            .where(_receipt.donor_id == P())
            .where(_receipt.company_id == P())
            .where(_receipt.donation_id.notnull())
        )
        receipted = {r["donation_id"] for r in conn.execute(rec_q.get_sql(), (donor_id, company_id)).fetchall()}
        unreceipted = [q for q in qualifying if q["id"] not in receipted]
        if not unreceipted:
            return err(f"Every tax-deductible donation for this donor in {tax_year} already has its own receipt")

        total = sum((_dec(q["amount"]) for q in unreceipted), Decimal("0"))
        amount = str(_round(total))
        donation_id = None  # No single donation for annual summary
        annual_goods_total = sum((_dec(q["goods_services_fair_value"]) for q in unreceipted), Decimal("0"))
        annual_descs = sorted({str(q["goods_services_description"]) for q in unreceipted if q["goods_services_description"]})
        annual_goods_desc = "; ".join(annual_descs) if annual_descs else None
    else:
        return err(f"Invalid receipt_type: {receipt_type}")

    if receipt_type == "single":
        amount_dec = _dec(donation["amount"])
        goods_fv_dec = _dec(donation["goods_services_fair_value"])
        goods_desc = donation["goods_services_description"]
    else:
        amount_dec = _dec(amount)
        goods_fv_dec = _dec(annual_goods_total)
        goods_desc = annual_goods_desc
    deductible_dec, goods_provided, statements = _substantiation(amount_dec, goods_fv_dec, goods_desc)
    deductible_amount = str(deductible_dec)
    goods_fv_str = str(_round(goods_fv_dec))

    receipt_id = str(uuid.uuid4())
    naming = get_next_name(conn, "nonprofitclaw_tax_receipt", company_id=company_id)

    sql, _ = insert_row("nonprofitclaw_tax_receipt", {
        "id": P(), "naming_series": P(), "donor_id": P(), "donation_id": P(),
        "receipt_date": P(), "amount": P(), "deductible_amount": P(),
        "goods_services_fair_value": P(), "goods_services_description": P(),
        "statements": P(), "tax_year": P(), "receipt_type": P(),
        "sent_date": P(), "sent_method": P(), "company_id": P(),
    })
    conn.execute(sql, (
        receipt_id, naming, donor_id, donation_id,
        str(date.today()), amount, deductible_amount,
        goods_fv_str, goods_desc,
        json.dumps(statements), tax_year, receipt_type,
        str(date.today()) if sent_method else None,
        sent_method, company_id,
    ))

    # Mark donation as receipt_sent if single
    if donation_id:
        sql_u, params_u = dynamic_update("nonprofitclaw_donation",
            {"receipt_sent": 1, "updated_at": sql_now()},
            where={"id": donation_id})
        conn.execute(sql_u, params_u)

    audit(conn, SKILL, "nonprofit-generate-tax-receipt", "nonprofitclaw_tax_receipt", receipt_id)
    conn.commit()
    return ok({
        "id": receipt_id,
        "naming_series": naming,
        "donor_name": donor["name"],
        "amount": amount,
        "deductible_amount": deductible_amount,
        "goods_services_fair_value": goods_fv_str,
        "goods_services_description": goods_desc,
        "goods_services_provided": goods_provided,
        "statements": statements,
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
            tr.donation_id, tr.receipt_date, tr.amount, tr.deductible_amount,
            tr.goods_services_fair_value, tr.goods_services_description,
            tr.statements, tr.tax_year,
            tr.receipt_type, tr.sent_date, tr.sent_method,
        )
    )
    for cond in conditions:
        data_q = data_q.where(cond)
    data_q = data_q.orderby(tr.receipt_date, order=Order.desc).limit(P()).offset(P())

    rows = conn.execute(data_q.get_sql(), params + [limit, offset]).fetchall()
    receipts = []
    for r in rows:
        d = dict(r)
        raw = d.get("statements")
        try:
            d["statements"] = json.loads(raw) if raw else []
        except Exception:
            d["statements"] = []
        if d.get("goods_services_fair_value") is None:
            d["goods_services_fair_value"] = "0.00"
        if d.get("deductible_amount") is None:
            try:
                d["deductible_amount"] = str(_round(_dec(d.get("amount")) - _dec(d.get("goods_services_fair_value"))))
            except Exception:
                d["deductible_amount"] = d.get("amount")
        try:
            d["goods_services_provided"] = _dec(d.get("goods_services_fair_value")) > Decimal("0")
        except Exception:
            d["goods_services_provided"] = False
        receipts.append(d)
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
            LiteralValue("SUM(CAST(amount AS NUMERIC))").as_("total_amount"),
            LiteralValue("AVG(CAST(amount AS NUMERIC))").as_("avg_donation"),
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
        .orderby(LiteralValue("CAST(\"nonprofitclaw_donor_ext\".\"total_donated\" AS NUMERIC)"), order=Order.desc)
        .limit(10)
    )
    top_donors = conn.execute(top_q.get_sql(), (company_id,)).fetchall()

    # Monthly trend (last 12 months)
    cutoff_12m = (date.today() - timedelta(days=365)).strftime('%Y-%m-%d')
    trend_q = (
        Q.from_(_don)
        .select(
            sql_date_format("donation_date", "%Y-%m").as_("month"),
            fn.Count("*").as_("count"),
            LiteralValue("SUM(CAST(amount AS NUMERIC))").as_("total"),
        )
        .where(_don.company_id == P())
        .where(_don.status.notin(["refunded", "cancelled"]))
        .where(_don.donation_date >= P())
        .groupby(LiteralValue("month"))
        .orderby(LiteralValue("month"))
    )
    monthly_trend = conn.execute(trend_q.get_sql(), (company_id, cutoff_12m)).fetchall()

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
        .select(LiteralValue("SUM(CAST(amount AS NUMERIC))"))
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
