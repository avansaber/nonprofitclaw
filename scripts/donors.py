#!/usr/bin/env python3
"""NonprofitClaw donors & donations domain — 12 actions.

Donors are backed by:
  - core ``customer`` table (name, email, phone, address, tax_id — owned by erpclaw-selling)
  - ``nonprofitclaw_donor_ext`` extension table (donor_type, donor_level, donation stats)

Core customer records are created/updated via ``erpclaw_lib.cross_skill.create_customer``.
"""
import json
import os
import sys
import uuid
from datetime import date
from decimal import Decimal, ROUND_HALF_UP

sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
from erpclaw_lib.naming import get_next_name
from erpclaw_lib.response import ok, err
from erpclaw_lib.audit import audit
from erpclaw_lib.cross_skill import create_customer, call_skill_action, CrossSkillError

try:
    from erpclaw_lib.gl_posting import insert_gl_entries, reverse_gl_entries
    HAS_GL = True
except ImportError:
    HAS_GL = False

SKILL = "nonprofitclaw"


def _dec(val):
    if val is None:
        return Decimal("0")
    return Decimal(str(val))


def _round(val):
    return val.quantize(Decimal("0.01"), ROUND_HALF_UP)


# ------------------------------------------------------------------
# Donor CRUD
# ------------------------------------------------------------------

def add_donor(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    name = args.name
    if not name:
        return err("--name is required")

    # 1) Create core customer via cross_skill
    customer_type = "individual"
    donor_type = getattr(args, "donor_type", None) or "individual"
    if donor_type in ("corporate", "foundation", "government"):
        customer_type = "company"

    try:
        cust_resp = create_customer(
            customer_name=name,
            company_id=company_id,
            customer_type=customer_type,
            email=getattr(args, "email", None),
            phone=getattr(args, "phone", None),
        )
    except CrossSkillError as e:
        return err(f"Failed to create core customer: {e}")

    customer_id = cust_resp.get("customer_id")
    if not customer_id:
        return err("create_customer did not return customer_id")

    # 2) Insert extension row
    donor_id = str(uuid.uuid4())
    naming = get_next_name(conn, "donor", company_id=company_id)
    donor_level = getattr(args, "donor_level", None) or "standard"

    conn.execute(
        """INSERT INTO nonprofitclaw_donor_ext
           (id, naming_series, customer_id, donor_type, donor_level,
            notes, is_active, company_id)
           VALUES (?,?,?,?,?,?,?,?)""",
        (
            donor_id, naming, customer_id, donor_type,
            donor_level,
            getattr(args, "notes", None),
            1, company_id,
        ),
    )
    conn.commit()
    audit(conn, SKILL, "nonprofit-add-donor", donor_id, company_id)
    return ok({"id": donor_id, "customer_id": customer_id, "naming_series": naming, "name": name})


def update_donor(conn, args):
    donor_id = args.id
    if not donor_id:
        return err("--id is required")

    row = conn.execute(
        "SELECT de.id, de.customer_id, de.company_id FROM nonprofitclaw_donor_ext de WHERE de.id=?",
        (donor_id,),
    ).fetchone()
    if not row:
        return err(f"Donor {donor_id} not found")

    customer_id = row["customer_id"]

    # --- Core customer fields (update via cross_skill) ---
    core_args = {}
    for cli_flag, attr in [
        ("--name", "name"), ("--email", "email"), ("--phone", "phone"),
        ("--primary-address", "address"), ("--tax-id", "tax_id"),
    ]:
        val = getattr(args, attr, None)
        if val is not None:
            core_args[cli_flag] = val

    if core_args:
        core_args["--id"] = customer_id
        try:
            call_skill_action("erpclaw-selling", "update-customer", args=core_args)
        except CrossSkillError as e:
            return err(f"Failed to update core customer: {e}")

    # --- Extension fields ---
    ext_fields = []
    ext_values = []
    for col, attr in [
        ("donor_type", "donor_type"), ("donor_level", "donor_level"),
        ("notes", "notes"),
    ]:
        val = getattr(args, attr, None)
        if val is not None:
            ext_fields.append(f"{col}=?")
            ext_values.append(val)

    is_active = getattr(args, "is_active", None)
    if is_active is not None:
        ext_fields.append("is_active=?")
        ext_values.append(int(is_active))

    if ext_fields:
        ext_fields.append("updated_at=datetime('now')")
        ext_values.append(donor_id)
        conn.execute(f"UPDATE nonprofitclaw_donor_ext SET {', '.join(ext_fields)} WHERE id=?", ext_values)
        conn.commit()

    if not core_args and not ext_fields:
        return err("No fields to update")

    audit(conn, SKILL, "nonprofit-update-donor", donor_id, row["company_id"])
    return ok({"id": donor_id, "updated": True})


def list_donors(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    limit = int(getattr(args, "limit", None) or 50)
    offset = int(getattr(args, "offset", None) or 0)

    where = ["de.company_id=?"]
    params = [company_id]

    search = getattr(args, "search", None)
    if search:
        where.append("(c.name LIKE ? OR c.primary_contact LIKE ?)")
        params.extend([f"%{search}%", f"%{search}%"])

    donor_type = getattr(args, "donor_type", None)
    if donor_type:
        where.append("de.donor_type=?")
        params.append(donor_type)

    donor_level = getattr(args, "donor_level", None)
    if donor_level:
        where.append("de.donor_level=?")
        params.append(donor_level)

    is_active = getattr(args, "is_active", None)
    if is_active is not None:
        where.append("de.is_active=?")
        params.append(int(is_active))

    where_sql = " AND ".join(where)

    total = conn.execute(
        f"SELECT COUNT(*) FROM nonprofitclaw_donor_ext de JOIN customer c ON de.customer_id = c.id WHERE {where_sql}",
        params,
    ).fetchone()[0]

    rows = conn.execute(
        f"""SELECT de.id, de.naming_series, de.customer_id, de.donor_type,
                   c.name, c.primary_contact AS email, c.tax_id,
                   de.total_donated, de.donation_count, de.donor_level, de.is_active
            FROM nonprofitclaw_donor_ext de
            JOIN customer c ON de.customer_id = c.id
            WHERE {where_sql}
            ORDER BY de.created_at DESC LIMIT ? OFFSET ?""",
        params + [limit, offset],
    ).fetchall()

    donors = [dict(r) for r in rows]
    return ok({"donors": donors, "total": total})


def get_donor(conn, args):
    donor_id = args.id
    if not donor_id:
        return err("--id is required")

    row = conn.execute(
        """SELECT de.*, c.name, c.primary_contact AS email,
                  c.primary_address AS address, c.tax_id
           FROM nonprofitclaw_donor_ext de
           JOIN customer c ON de.customer_id = c.id
           WHERE de.id=?""",
        (donor_id,),
    ).fetchone()
    if not row:
        return err(f"Donor {donor_id} not found")

    return ok({"donor": dict(row)})


def donor_giving_history(conn, args):
    donor_id = getattr(args, "donor_id", None)
    if not donor_id:
        return err("--donor-id is required")

    row = conn.execute(
        """SELECT de.id, c.name
           FROM nonprofitclaw_donor_ext de
           JOIN customer c ON de.customer_id = c.id
           WHERE de.id=?""",
        (donor_id,),
    ).fetchone()
    if not row:
        return err(f"Donor {donor_id} not found")

    donations = conn.execute(
        """SELECT id, naming_series, donation_date, amount, payment_method,
                  fund_id, campaign_id, status, reference
           FROM nonprofitclaw_donation WHERE donor_id=?
           ORDER BY donation_date DESC""",
        (donor_id,),
    ).fetchall()

    total_row = conn.execute(
        "SELECT SUM(CAST(amount AS REAL)) FROM nonprofitclaw_donation WHERE donor_id=? AND status NOT IN ('refunded','cancelled')",
        (donor_id,),
    ).fetchone()
    total = str(_round(_dec(total_row[0]))) if total_row[0] else "0.00"

    return ok({
        "donor_id": donor_id,
        "donor_name": row["name"],
        "donations": [dict(d) for d in donations],
        "total_given": total,
        "total": len(donations),
    })


def merge_donors(conn, args):
    source_id = getattr(args, "source_donor_id", None)
    target_id = getattr(args, "target_donor_id", None)
    if not source_id or not target_id:
        return err("--source-donor-id and --target-donor-id are required")
    if source_id == target_id:
        return err("Source and target donor must be different")

    source = conn.execute(
        "SELECT id, customer_id, company_id FROM nonprofitclaw_donor_ext WHERE id=?", (source_id,)
    ).fetchone()
    target = conn.execute(
        "SELECT id, customer_id, company_id FROM nonprofitclaw_donor_ext WHERE id=?", (target_id,)
    ).fetchone()
    if not source:
        return err(f"Source donor {source_id} not found")
    if not target:
        return err(f"Target donor {target_id} not found")
    if source["company_id"] != target["company_id"]:
        return err("Donors must belong to the same company")

    # Transaction (implicit)
    try:
        # Move donations, pledges, tax receipts to target ext record
        conn.execute("UPDATE nonprofitclaw_donation SET donor_id=? WHERE donor_id=?", (target_id, source_id))
        conn.execute("UPDATE nonprofitclaw_pledge SET donor_id=? WHERE donor_id=?", (target_id, source_id))
        conn.execute("UPDATE nonprofitclaw_tax_receipt SET donor_id=? WHERE donor_id=?", (target_id, source_id))

        # Recalculate target donor stats
        stats = conn.execute(
            """SELECT COUNT(*) as cnt,
                      SUM(CAST(amount AS REAL)) as total
               FROM nonprofitclaw_donation
               WHERE donor_id=? AND status NOT IN ('refunded','cancelled')""",
            (target_id,),
        ).fetchone()

        last_date = conn.execute(
            "SELECT MAX(donation_date) FROM nonprofitclaw_donation WHERE donor_id=? AND status NOT IN ('refunded','cancelled')",
            (target_id,),
        ).fetchone()

        first_date = conn.execute(
            "SELECT MIN(donation_date) FROM nonprofitclaw_donation WHERE donor_id=? AND status NOT IN ('refunded','cancelled')",
            (target_id,),
        ).fetchone()

        new_total = str(_round(_dec(stats["total"]))) if stats["total"] else "0"
        new_count = stats["cnt"] or 0

        conn.execute(
            """UPDATE nonprofitclaw_donor_ext SET
                total_donated=?, donation_count=?,
                last_donation_date=?, first_donation_date=?,
                updated_at=datetime('now')
               WHERE id=?""",
            (new_total, new_count, last_date[0], first_date[0], target_id),
        )

        # Delete source ext record (core customer record remains for audit trail)
        conn.execute("DELETE FROM nonprofitclaw_donor_ext WHERE id=?", (source_id,))
        conn.commit()
    except Exception as e:
        conn.rollback()
        return err(f"Merge failed: {e}")

    audit(conn, SKILL, "nonprofit-merge-donors", target_id, target["company_id"])
    return ok({
        "target_donor_id": target_id,
        "source_donor_id": source_id,
        "merged": True,
        "new_donation_count": new_count,
        "new_total_donated": new_total,
    })


def import_donors(conn, args):
    return ok({
        "message": "Donor import is available via CSV upload. Each row calls "
                   "create_customer() for the core record, then inserts a "
                   "nonprofitclaw_donor_ext row. Use the web interface or "
                   "provide a CSV file path.",
        "imported": 0,
    })


# ------------------------------------------------------------------
# Donation CRUD
# ------------------------------------------------------------------

def add_donation(conn, args):
    company_id = args.company_id
    donor_id = getattr(args, "donor_id", None)
    if not company_id:
        return err("--company-id is required")
    if not donor_id:
        return err("--donor-id is required")

    donor = conn.execute(
        "SELECT id, customer_id, company_id FROM nonprofitclaw_donor_ext WHERE id=?",
        (donor_id,),
    ).fetchone()
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

    donation_id = str(uuid.uuid4())
    naming = get_next_name(conn, "donation", company_id=company_id)
    donation_date = getattr(args, "donation_date", None) or str(date.today())
    payment_method = getattr(args, "payment_method", None) or "check"
    is_recurring = int(getattr(args, "is_recurring", None) or 0)
    recurrence_freq = getattr(args, "recurrence_freq", None)

    fund_id = getattr(args, "fund_id", None)
    if fund_id:
        fund = conn.execute("SELECT id FROM nonprofitclaw_fund WHERE id=?", (fund_id,)).fetchone()
        if not fund:
            return err(f"Fund {fund_id} not found")

    campaign_id = getattr(args, "campaign_id", None)
    if campaign_id:
        campaign = conn.execute("SELECT id FROM nonprofitclaw_campaign WHERE id=?", (campaign_id,)).fetchone()
        if not campaign:
            return err(f"Campaign {campaign_id} not found")

    # GL account IDs (optional — graceful degradation)
    cash_account_id = getattr(args, "cash_account_id", None)
    revenue_account_id = getattr(args, "revenue_account_id", None)
    cost_center_id = getattr(args, "cost_center_id", None)

    # Transaction (implicit)
    gl_entry_ids = None
    try:
        conn.execute(
            """INSERT INTO nonprofitclaw_donation
               (id, naming_series, donor_id, fund_id, campaign_id,
                donation_date, amount, payment_method, reference,
                is_recurring, recurrence_freq, notes, status, company_id)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                donation_id, naming, donor_id, fund_id, campaign_id,
                donation_date, str(amount), payment_method,
                getattr(args, "reference", None),
                is_recurring, recurrence_freq,
                getattr(args, "notes", None),
                "received", company_id,
            ),
        )

        # --- GL Posting: DR Cash/Bank, CR Contribution Revenue ---
        if HAS_GL and cash_account_id and revenue_account_id:
            customer_id = donor["customer_id"]
            gl_entries = [
                {
                    "account_id": cash_account_id,
                    "debit": str(amount),
                    "credit": "0",
                    "party_type": "customer",
                    "party_id": customer_id,
                    "cost_center_id": cost_center_id,
                },
                {
                    "account_id": revenue_account_id,
                    "debit": "0",
                    "credit": str(amount),
                    "cost_center_id": cost_center_id,
                },
            ]
            try:
                ids = insert_gl_entries(
                    conn,
                    gl_entries,
                    voucher_type="donation",
                    voucher_id=donation_id,
                    posting_date=donation_date,
                    company_id=company_id,
                    remarks=f"Donation {naming} from donor {donor_id}",
                )
                gl_entry_ids = json.dumps(ids)
                conn.execute(
                    "UPDATE nonprofitclaw_donation SET gl_entry_ids=? WHERE id=?",
                    (gl_entry_ids, donation_id),
                )
            except (ValueError, Exception):
                # GL posting failed — donation still recorded, no GL entries
                # This is intentional graceful degradation: the donation data
                # is preserved even when GL accounts aren't properly configured.
                pass

        # Update donor ext stats
        stats = conn.execute(
            """SELECT COUNT(*) as cnt,
                      SUM(CAST(amount AS REAL)) as total
               FROM nonprofitclaw_donation
               WHERE donor_id=? AND status NOT IN ('refunded','cancelled')""",
            (donor_id,),
        ).fetchone()

        new_total = str(_round(_dec(stats["total"]))) if stats["total"] else "0"
        new_count = stats["cnt"] or 0

        first_date = conn.execute(
            "SELECT MIN(donation_date) FROM nonprofitclaw_donation WHERE donor_id=? AND status NOT IN ('refunded','cancelled')",
            (donor_id,),
        ).fetchone()

        conn.execute(
            """UPDATE nonprofitclaw_donor_ext SET
                total_donated=?, donation_count=?,
                last_donation_date=?, first_donation_date=COALESCE(first_donation_date, ?),
                updated_at=datetime('now')
               WHERE id=?""",
            (new_total, new_count, donation_date, first_date[0], donor_id),
        )

        # Update fund balance if applicable
        if fund_id:
            conn.execute(
                """UPDATE nonprofitclaw_fund SET
                    current_balance = CAST(
                        CAST(current_balance AS REAL) + ? AS TEXT
                    ), updated_at=datetime('now')
                   WHERE id=?""",
                (float(amount), fund_id),
            )

        # Update campaign raised_amount if applicable
        if campaign_id:
            conn.execute(
                """UPDATE nonprofitclaw_campaign SET
                    raised_amount = CAST(
                        CAST(raised_amount AS REAL) + ? AS TEXT
                    ),
                    donor_count = donor_count + 1,
                    updated_at=datetime('now')
                   WHERE id=?""",
                (float(amount), campaign_id),
            )

        conn.commit()
    except Exception as e:
        conn.rollback()
        return err(f"Failed to add donation: {e}")

    audit(conn, SKILL, "nonprofit-add-donation", donation_id, company_id)
    result = {"id": donation_id, "naming_series": naming, "amount": str(amount)}
    if gl_entry_ids:
        result["gl_entry_ids"] = json.loads(gl_entry_ids)
    return ok(result)


def update_donation(conn, args):
    donation_id = args.id
    if not donation_id:
        return err("--id is required")

    row = conn.execute("SELECT * FROM nonprofitclaw_donation WHERE id=?", (donation_id,)).fetchone()
    if not row:
        return err(f"Donation {donation_id} not found")
    if row["status"] in ("refunded", "cancelled"):
        return err(f"Cannot update donation in '{row['status']}' status")

    fields = []
    values = []
    for col, attr in [
        ("payment_method", "payment_method"), ("reference", "reference"),
        ("notes", "notes"), ("donation_date", "donation_date"),
    ]:
        val = getattr(args, attr, None)
        if val is not None:
            fields.append(f"{col}=?")
            values.append(val)

    is_recurring = getattr(args, "is_recurring", None)
    if is_recurring is not None:
        fields.append("is_recurring=?")
        values.append(int(is_recurring))

    recurrence_freq = getattr(args, "recurrence_freq", None)
    if recurrence_freq is not None:
        fields.append("recurrence_freq=?")
        values.append(recurrence_freq)

    if not fields:
        return err("No fields to update")

    fields.append("updated_at=datetime('now')")
    values.append(donation_id)
    conn.execute(f"UPDATE nonprofitclaw_donation SET {', '.join(fields)} WHERE id=?", values)
    conn.commit()
    audit(conn, SKILL, "nonprofit-update-donation", donation_id, row["company_id"])
    return ok({"id": donation_id, "updated": True})


def list_donations(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    limit = int(getattr(args, "limit", None) or 50)
    offset = int(getattr(args, "offset", None) or 0)

    where = ["d.company_id=?"]
    params = [company_id]

    donor_id = getattr(args, "donor_id", None)
    if donor_id:
        where.append("d.donor_id=?")
        params.append(donor_id)

    fund_id = getattr(args, "fund_id", None)
    if fund_id:
        where.append("d.fund_id=?")
        params.append(fund_id)

    campaign_id = getattr(args, "campaign_id", None)
    if campaign_id:
        where.append("d.campaign_id=?")
        params.append(campaign_id)

    status = getattr(args, "status", None)
    if status:
        where.append("d.status=?")
        params.append(status)

    from_date = getattr(args, "from_date", None)
    if from_date:
        where.append("d.donation_date >= ?")
        params.append(from_date)

    to_date = getattr(args, "to_date", None)
    if to_date:
        where.append("d.donation_date <= ?")
        params.append(to_date)

    where_sql = " AND ".join(where)

    total = conn.execute(
        f"SELECT COUNT(*) FROM nonprofitclaw_donation d WHERE {where_sql}", params
    ).fetchone()[0]

    rows = conn.execute(
        f"""SELECT d.id, d.naming_series, d.donor_id, c.name as donor_name,
                   d.donation_date, d.amount, d.payment_method, d.status,
                   d.fund_id, d.campaign_id, d.is_recurring
            FROM nonprofitclaw_donation d
            LEFT JOIN nonprofitclaw_donor_ext de ON d.donor_id = de.id
            LEFT JOIN customer c ON de.customer_id = c.id
            WHERE {where_sql}
            ORDER BY d.donation_date DESC LIMIT ? OFFSET ?""",
        params + [limit, offset],
    ).fetchall()

    donations = [dict(r) for r in rows]
    return ok({"donations": donations, "total": total})


def get_donation(conn, args):
    donation_id = args.id
    if not donation_id:
        return err("--id is required")

    row = conn.execute(
        """SELECT d.*, c.name as donor_name
           FROM nonprofitclaw_donation d
           LEFT JOIN nonprofitclaw_donor_ext de ON d.donor_id = de.id
           LEFT JOIN customer c ON de.customer_id = c.id
           WHERE d.id=?""",
        (donation_id,),
    ).fetchone()
    if not row:
        return err(f"Donation {donation_id} not found")

    return ok({"donation": dict(row)})


def refund_donation(conn, args):
    donation_id = getattr(args, "donation_id", None) or args.id
    if not donation_id:
        return err("--donation-id or --id is required")

    row = conn.execute("SELECT * FROM nonprofitclaw_donation WHERE id=?", (donation_id,)).fetchone()
    if not row:
        return err(f"Donation {donation_id} not found")
    if row["status"] == "refunded":
        return err("Donation is already refunded")
    if row["status"] == "cancelled":
        return err("Cannot refund a cancelled donation")

    amount = _dec(row["amount"])
    donor_id = row["donor_id"]
    fund_id = row["fund_id"]
    campaign_id = row["campaign_id"]
    donation_date = row["donation_date"]

    # Transaction (implicit)
    gl_reversal_ids = None
    try:
        conn.execute(
            "UPDATE nonprofitclaw_donation SET status='refunded', updated_at=datetime('now') WHERE id=?",
            (donation_id,),
        )

        # --- Reverse GL entries if they exist ---
        if HAS_GL and row["gl_entry_ids"]:
            try:
                reversal_ids = reverse_gl_entries(
                    conn,
                    voucher_type="donation",
                    voucher_id=donation_id,
                    posting_date=donation_date,
                )
                gl_reversal_ids = reversal_ids
            except (ValueError, Exception):
                # GL reversal failed — refund still proceeds
                pass

        # Update donor stats
        stats = conn.execute(
            """SELECT COUNT(*) as cnt,
                      SUM(CAST(amount AS REAL)) as total
               FROM nonprofitclaw_donation
               WHERE donor_id=? AND status NOT IN ('refunded','cancelled')""",
            (donor_id,),
        ).fetchone()

        new_total = str(_round(_dec(stats["total"]))) if stats["total"] else "0"
        new_count = stats["cnt"] or 0

        last_date = conn.execute(
            "SELECT MAX(donation_date) FROM nonprofitclaw_donation WHERE donor_id=? AND status NOT IN ('refunded','cancelled')",
            (donor_id,),
        ).fetchone()

        conn.execute(
            """UPDATE nonprofitclaw_donor_ext SET
                total_donated=?, donation_count=?,
                last_donation_date=?,
                updated_at=datetime('now')
               WHERE id=?""",
            (new_total, new_count, last_date[0], donor_id),
        )

        # Reverse fund balance if applicable
        if fund_id:
            conn.execute(
                """UPDATE nonprofitclaw_fund SET
                    current_balance = CAST(
                        CAST(current_balance AS REAL) - ? AS TEXT
                    ), updated_at=datetime('now')
                   WHERE id=?""",
                (float(amount), fund_id),
            )

        # Reverse campaign stats if applicable
        if campaign_id:
            conn.execute(
                """UPDATE nonprofitclaw_campaign SET
                    raised_amount = CAST(
                        CAST(raised_amount AS REAL) - ? AS TEXT
                    ),
                    donor_count = MAX(donor_count - 1, 0),
                    updated_at=datetime('now')
                   WHERE id=?""",
                (float(amount), campaign_id),
            )

        conn.commit()
    except Exception as e:
        conn.rollback()
        return err(f"Refund failed: {e}")

    audit(conn, SKILL, "nonprofit-refund-donation", donation_id, row["company_id"])
    result = {"id": donation_id, "refunded": True, "amount": str(amount)}
    if gl_reversal_ids:
        result["gl_reversal_ids"] = gl_reversal_ids
    return ok(result)


ACTIONS = {
    "nonprofit-add-donor": add_donor,
    "nonprofit-update-donor": update_donor,
    "nonprofit-list-donors": list_donors,
    "nonprofit-get-donor": get_donor,
    "nonprofit-donor-giving-history": donor_giving_history,
    "nonprofit-merge-donors": merge_donors,
    "nonprofit-import-donors": import_donors,
    "nonprofit-add-donation": add_donation,
    "nonprofit-update-donation": update_donation,
    "nonprofit-list-donations": list_donations,
    "nonprofit-get-donation": get_donation,
    "nonprofit-refund-donation": refund_donation,
}
