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
from erpclaw_lib.query import Q, P, Table, Field, fn, Order, insert_row, update_row

SKILL = "nonprofitclaw"


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

    donor = conn.execute(
        """SELECT de.id, c.name, de.company_id
           FROM nonprofitclaw_donor_ext de
           JOIN customer c ON de.customer_id = c.id
           WHERE de.id=?""",
        (donor_id,),
    ).fetchone()
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

        donation = conn.execute(
            "SELECT id, amount, tax_deductible, status FROM nonprofitclaw_donation WHERE id=? AND donor_id=?",
            (donation_id, donor_id),
        ).fetchone()
        if not donation:
            return err(f"Donation {donation_id} not found for this donor")
        if donation["status"] in ("refunded", "cancelled"):
            return err(f"Cannot issue receipt for '{donation['status']}' donation")
        if not donation["tax_deductible"]:
            return err("Donation is not tax-deductible")

        amount = donation["amount"]

        # Check for duplicate receipt
        existing = conn.execute(Q.from_(Table("nonprofitclaw_tax_receipt")).select(Field("id")).where(Field("donation_id") == P()).get_sql(), (donation_id,)).fetchone()
        if existing:
            return err(f"Tax receipt already exists for this donation: {existing['id']}")

    elif receipt_type == "annual_summary":
        # Annual summary receipt — aggregate all deductible donations for the year
        total_row = conn.execute(
            """SELECT SUM(CAST(amount AS REAL)) as total
               FROM nonprofitclaw_donation
               WHERE donor_id=? AND company_id=? AND tax_deductible=1
                     AND status NOT IN ('refunded','cancelled')
                     AND strftime('%%Y', donation_date) = ?""",
            (donor_id, company_id, tax_year),
        ).fetchone()

        if not total_row["total"]:
            return err(f"No tax-deductible donations found for donor in {tax_year}")

        amount = str(_round(_dec(total_row["total"])))
        donation_id = None  # No single donation for annual summary
    else:
        return err(f"Invalid receipt_type: {receipt_type}")

    receipt_id = str(uuid.uuid4())
    naming = get_next_name(conn, "nonprofitclaw_tax_receipt", company_id=company_id)

    conn.execute(
        """INSERT INTO nonprofitclaw_tax_receipt
           (id, naming_series, donor_id, donation_id, receipt_date,
            amount, tax_year, receipt_type, sent_date, sent_method, company_id)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (
            receipt_id, naming, donor_id, donation_id,
            str(date.today()), amount, tax_year, receipt_type,
            str(date.today()) if sent_method else None,
            sent_method, company_id,
        ),
    )

    # Mark donation as receipt_sent if single
    if donation_id:
        conn.execute(
            "UPDATE nonprofitclaw_donation SET receipt_sent=1, updated_at=datetime('now') WHERE id=?",
            (donation_id,),
        )

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

    where = ["tr.company_id=?"]
    params = [company_id]

    donor_id = getattr(args, "donor_id", None)
    if donor_id:
        where.append("tr.donor_id=?")
        params.append(donor_id)

    tax_year = getattr(args, "tax_year", None)
    if tax_year:
        where.append("tr.tax_year=?")
        params.append(tax_year)

    receipt_type = getattr(args, "receipt_type", None)
    if receipt_type:
        where.append("tr.receipt_type=?")
        params.append(receipt_type)

    where_sql = " AND ".join(where)

    total = conn.execute(
        f"SELECT COUNT(*) FROM nonprofitclaw_tax_receipt tr WHERE {where_sql}", params
    ).fetchone()[0]

    rows = conn.execute(
        f"""SELECT tr.id, tr.naming_series, tr.donor_id, cust.name as donor_name,
                   tr.donation_id, tr.receipt_date, tr.amount, tr.tax_year,
                   tr.receipt_type, tr.sent_date, tr.sent_method
            FROM nonprofitclaw_tax_receipt tr
            LEFT JOIN nonprofitclaw_donor_ext de ON tr.donor_id = de.id
            LEFT JOIN customer cust ON de.customer_id = cust.id
            WHERE {where_sql}
            ORDER BY tr.receipt_date DESC LIMIT ? OFFSET ?""",
        params + [limit, offset],
    ).fetchall()

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
    donor_stats = conn.execute(
        """SELECT
            COUNT(*) as total_donors,
            SUM(CASE WHEN de.is_active=1 THEN 1 ELSE 0 END) as active_donors,
            SUM(CASE WHEN de.donor_type='individual' THEN 1 ELSE 0 END) as individual_donors,
            SUM(CASE WHEN de.donor_type='corporate' THEN 1 ELSE 0 END) as org_donors,
            SUM(CASE WHEN de.donor_type='foundation' THEN 1 ELSE 0 END) as foundation_donors
           FROM nonprofitclaw_donor_ext de WHERE de.company_id=?""",
        (company_id,),
    ).fetchone()

    # Donation stats
    donation_stats = conn.execute(
        """SELECT
            COUNT(*) as total_donations,
            SUM(CAST(amount AS REAL)) as total_amount,
            AVG(CAST(amount AS REAL)) as avg_donation
           FROM nonprofitclaw_donation
           WHERE company_id=? AND status NOT IN ('refunded','cancelled')""",
        (company_id,),
    ).fetchone()

    # Donor level breakdown
    level_breakdown = conn.execute(
        """SELECT donor_level, COUNT(*) as count
           FROM nonprofitclaw_donor_ext WHERE company_id=? AND is_active=1
           GROUP BY donor_level ORDER BY count DESC""",
        (company_id,),
    ).fetchall()

    # Top donors
    top_donors = conn.execute(
        """SELECT de.id, c.name, de.donor_type, de.total_donated, de.donation_count, de.donor_level
           FROM nonprofitclaw_donor_ext de
           JOIN customer c ON de.customer_id = c.id
           WHERE de.company_id=? AND de.is_active=1
           ORDER BY CAST(de.total_donated AS REAL) DESC LIMIT 10""",
        (company_id,),
    ).fetchall()

    # Monthly trend (last 12 months)
    monthly_trend = conn.execute(
        """SELECT strftime('%%Y-%%m', donation_date) as month,
                  COUNT(*) as count,
                  SUM(CAST(amount AS REAL)) as total
           FROM nonprofitclaw_donation
           WHERE company_id=? AND status NOT IN ('refunded','cancelled')
                 AND donation_date >= date('now', '-12 months')
           GROUP BY month ORDER BY month""",
        (company_id,),
    ).fetchall()

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

    for label, table in tables:
        try:
            row = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE company_id=?", (company_id,)
            ).fetchone()
            counts[label] = row[0]
        except Exception:
            counts[label] = 0

    # Total donations amount
    total_donated = conn.execute(
        """SELECT SUM(CAST(amount AS REAL))
           FROM nonprofitclaw_donation
           WHERE company_id=? AND status NOT IN ('refunded','cancelled')""",
        (company_id,),
    ).fetchone()[0]

    # Active grants
    active_grants = conn.execute(
        "SELECT COUNT(*) FROM nonprofitclaw_grant WHERE company_id=? AND status='active'",
        (company_id,),
    ).fetchone()[0]

    # Active campaigns
    active_campaigns = conn.execute(
        "SELECT COUNT(*) FROM nonprofitclaw_campaign WHERE company_id=? AND status='active'",
        (company_id,),
    ).fetchone()[0]

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
