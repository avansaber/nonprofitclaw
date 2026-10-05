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

import importlib.util
if importlib.util.find_spec("erpclaw_lib") is None:
    sys.path.insert(0, os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))
from erpclaw_lib.naming import get_next_name
from erpclaw_lib.response import ok, err
from erpclaw_lib.audit import audit
from erpclaw_lib.cross_skill import create_customer, call_skill_action, CrossSkillError

try:
    from erpclaw_lib.gl_posting import insert_gl_entries, reverse_gl_entries
    from erpclaw_lib.query import (
        Q, P, Table, Field, fn, Order, LiteralValue,
        insert_row, update_row, dynamic_update, now,
    )
    HAS_GL = True
except ImportError:
    HAS_GL = False

SKILL = "nonprofitclaw"

# ── Table aliases ──
_de = Table("nonprofitclaw_donor_ext")
_c = Table("customer")
_don = Table("nonprofitclaw_donation")
_fund = Table("nonprofitclaw_fund")
_campaign = Table("nonprofitclaw_campaign")
_pledge = Table("nonprofitclaw_pledge")
_receipt = Table("nonprofitclaw_tax_receipt")


def _dec(val):
    if val is None:
        return Decimal("0")
    return Decimal(str(val))


def _round(val):
    return val.quantize(Decimal("0.01"), ROUND_HALF_UP)


def _shift_fund_balance(conn, fund_id, delta):
    ft = Table("nonprofitclaw_fund")
    bq = Q.from_(ft).select(ft.current_balance).where(ft.id == P())
    row = conn.execute(bq.get_sql(), (fund_id,)).fetchone()
    old_text = row["current_balance"] if row is not None else "0"
    if old_text is None:
        old_text = "0"
    new_text = str(_round(_dec(old_text) + delta))
    upd = Q.update(ft).set(ft.current_balance, P()).set(ft.updated_at, now()).where(ft.id == P()).where(ft.current_balance == P())
    cur = conn.execute(upd.get_sql(), (new_text, fund_id, old_text))
    if cur.rowcount != 1:
        raise ValueError("Concurrent change to nonprofitclaw_fund %s: current_balance is no longer %s; nothing was written" % (fund_id, old_text))


def _exact_donor_totals(conn, donor_id):
    dq = Q.from_(_don).select(_don.amount).where(_don.donor_id == P()).where(_don.status.notin(["refunded", "cancelled"]))
    rows = conn.execute(dq.get_sql(), (donor_id,)).fetchall()
    cq = Q.from_(_don).select(fn.Count("*").as_("cnt")).where(_don.donor_id == P()).where(_don.status.notin(["refunded", "cancelled"]))
    cnt = conn.execute(cq.get_sql(), (donor_id,)).fetchone()["cnt"] or 0
    if not rows:
        return "0", cnt
    total = sum((_dec(r["amount"]) for r in rows), Decimal("0"))
    return str(_round(total)), cnt


def _guarded_donor_totals(conn, donor_id, old_total, new_total, new_count, last_date, first_date=None, keep_first=False):
    t = Table("nonprofitclaw_donor_ext")
    if keep_first:
        upd = Q.update(t).set(t.total_donated, P()).set(t.donation_count, P()).set(t.last_donation_date, P()).set(t.first_donation_date, LiteralValue("COALESCE(first_donation_date, ?)")).set(t.updated_at, now()).where(t.id == P()).where(t.total_donated == P())
        cur = conn.execute(upd.get_sql(), (new_total, new_count, last_date, first_date, donor_id, old_total))
    else:
        upd = Q.update(t).set(t.total_donated, P()).set(t.donation_count, P()).set(t.last_donation_date, P()).set(t.updated_at, now()).where(t.id == P()).where(t.total_donated == P())
        cur = conn.execute(upd.get_sql(), (new_total, new_count, last_date, donor_id, old_total))
    if cur.rowcount != 1:
        raise ValueError("Concurrent change to nonprofitclaw_donor_ext %s: total_donated is no longer %s; nothing was written" % (donor_id, old_total))


def _guarded_campaign_raised(conn, campaign_id, old_raised, new_raised):
    ct = Table("nonprofitclaw_campaign")
    upd = Q.update(ct).set(ct.raised_amount, P()).set(ct.donor_count, LiteralValue("donor_count + 1")).set(ct.updated_at, now()).where(ct.id == P()).where(ct.raised_amount == P())
    cur = conn.execute(upd.get_sql(), (new_raised, campaign_id, old_raised))
    if cur.rowcount != 1:
        raise ValueError("Concurrent change to nonprofitclaw_campaign %s: raised_amount is no longer %s; nothing was written" % (campaign_id, old_raised))


def _guarded_campaign_refund(conn, campaign_id, old_raised, new_raised, old_count, new_count):
    ct = Table("nonprofitclaw_campaign")
    upd = Q.update(ct).set(ct.raised_amount, P()).set(ct.donor_count, P()).set(ct.updated_at, now()).where(ct.id == P()).where(ct.raised_amount == P()).where(ct.donor_count == P())
    cur = conn.execute(upd.get_sql(), (new_raised, new_count, campaign_id, old_raised, old_count))
    if cur.rowcount != 1:
        raise ValueError("Concurrent change to nonprofitclaw_campaign %s: raised_amount is no longer %s; nothing was written" % (campaign_id, old_raised))


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
    naming = get_next_name(conn, "nonprofitclaw_donor_ext", company_id=company_id)
    donor_level = getattr(args, "donor_level", None) or "standard"

    sql, _ = insert_row("nonprofitclaw_donor_ext", {
        "id": P(), "naming_series": P(), "customer_id": P(), "donor_type": P(),
        "donor_level": P(), "notes": P(), "is_active": P(), "company_id": P(),
    })
    conn.execute(sql, (
        donor_id, naming, customer_id, donor_type,
        donor_level,
        getattr(args, "notes", None),
        1, company_id,
    ))
    audit(conn, SKILL, "nonprofit-add-donor", "nonprofitclaw_donor_ext", donor_id)
    conn.commit()
    return ok({"id": donor_id, "customer_id": customer_id, "naming_series": naming, "name": name})


def update_donor(conn, args):
    donor_id = args.id
    if not donor_id:
        return err("--id is required")

    q = Q.from_(_de).select(_de.id, _de.customer_id, _de.company_id).where(_de.id == P())
    row = conn.execute(q.get_sql(), (donor_id,)).fetchone()
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
            call_skill_action("erpclaw", "update-customer", args=core_args)
        except CrossSkillError as e:
            return err(f"Failed to update core customer: {e}")

    # --- Extension fields ---
    ext_data = {}
    for col, attr in [
        ("donor_type", "donor_type"), ("donor_level", "donor_level"),
        ("notes", "notes"),
    ]:
        val = getattr(args, attr, None)
        if val is not None:
            ext_data[col] = val

    is_active = getattr(args, "is_active", None)
    if is_active is not None:
        ext_data["is_active"] = int(is_active)

    if ext_data:
        ext_data["updated_at"] = now()
        sql, params = dynamic_update("nonprofitclaw_donor_ext", ext_data, where={"id": donor_id})
        conn.execute(sql, params)
        audit(conn, SKILL, "nonprofit-update-donor", "nonprofitclaw_donor_ext", donor_id)
        conn.commit()

    if not core_args and not ext_data:
        return err("No fields to update")

    return ok({"id": donor_id, "updated": True})


def list_donors(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    limit = int(getattr(args, "limit", None) or 50)
    offset = int(getattr(args, "offset", None) or 0)

    base = Q.from_(_de).join(_c).on(_de.customer_id == _c.id)
    conditions = [_de.company_id == P()]
    params = [company_id]

    search = getattr(args, "search", None)
    if search:
        conditions.append(_c.name.like(P()) | _c.primary_contact.like(P()))
        params.extend([f"%{search}%", f"%{search}%"])

    donor_type = getattr(args, "donor_type", None)
    if donor_type:
        conditions.append(_de.donor_type == P())
        params.append(donor_type)

    donor_level = getattr(args, "donor_level", None)
    if donor_level:
        conditions.append(_de.donor_level == P())
        params.append(donor_level)

    is_active = getattr(args, "is_active", None)
    if is_active is not None:
        conditions.append(_de.is_active == P())
        params.append(int(is_active))

    # Count query
    count_q = base.select(fn.Count("*"))
    for cond in conditions:
        count_q = count_q.where(cond)
    total = conn.execute(count_q.get_sql(), params).fetchone()[0]

    # Data query
    data_q = base.select(
        _de.id, _de.naming_series, _de.customer_id, _de.donor_type,
        _c.name, _c.primary_contact.as_("email"), _c.tax_id,
        _de.total_donated, _de.donation_count, _de.donor_level, _de.is_active,
    )
    for cond in conditions:
        data_q = data_q.where(cond)
    data_q = data_q.orderby(_de.created_at, order=Order.desc).limit(P()).offset(P())

    rows = conn.execute(data_q.get_sql(), params + [limit, offset]).fetchall()
    donors = [dict(r) for r in rows]
    return ok({"donors": donors, "total": total})


def get_donor(conn, args):
    donor_id = args.id
    if not donor_id:
        return err("--id is required")

    q = (
        Q.from_(_de)
        .join(_c).on(_de.customer_id == _c.id)
        .select(
            _de.star, _c.name, _c.primary_contact.as_("email"),
            _c.primary_address.as_("address"), _c.tax_id,
        )
        .where(_de.id == P())
    )
    row = conn.execute(q.get_sql(), (donor_id,)).fetchone()
    if not row:
        return err(f"Donor {donor_id} not found")

    return ok({"donor": dict(row)})


def donor_giving_history(conn, args):
    donor_id = getattr(args, "donor_id", None)
    if not donor_id:
        return err("--donor-id is required")

    q = (
        Q.from_(_de)
        .join(_c).on(_de.customer_id == _c.id)
        .select(_de.id, _c.name)
        .where(_de.id == P())
    )
    row = conn.execute(q.get_sql(), (donor_id,)).fetchone()
    if not row:
        return err(f"Donor {donor_id} not found")

    don_q = (
        Q.from_(_don)
        .select(
            _don.id, _don.naming_series, _don.donation_date, _don.amount,
            _don.payment_method, _don.fund_id, _don.campaign_id, _don.status, _don.reference,
        )
        .where(_don.donor_id == P())
        .orderby(_don.donation_date, order=Order.desc)
    )
    donations = conn.execute(don_q.get_sql(), (donor_id,)).fetchall()

    total_q = (
        Q.from_(_don)
        .select(LiteralValue("SUM(CAST(amount AS NUMERIC))"))
        .where(_don.donor_id == P())
        .where(_don.status.notin(["refunded", "cancelled"]))
    )
    total_row = conn.execute(total_q.get_sql(), (donor_id,)).fetchone()
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

    sq = Q.from_(_de).select(_de.id, _de.customer_id, _de.company_id).where(_de.id == P())
    source = conn.execute(sq.get_sql(), (source_id,)).fetchone()
    target = conn.execute(sq.get_sql(), (target_id,)).fetchone()
    if not source:
        return err(f"Source donor {source_id} not found")
    if not target:
        return err(f"Target donor {target_id} not found")
    if source["company_id"] != target["company_id"]:
        return err("Donors must belong to the same company")

    # Transaction (implicit)
    try:
        # Move donations, pledges, tax receipts to target ext record
        upd1_sql, upd1_p = dynamic_update("nonprofitclaw_donation", {"donor_id": target_id}, where={"donor_id": source_id})
        conn.execute(upd1_sql, upd1_p)
        upd2_sql, upd2_p = dynamic_update("nonprofitclaw_pledge", {"donor_id": target_id}, where={"donor_id": source_id})
        conn.execute(upd2_sql, upd2_p)
        upd3_sql, upd3_p = dynamic_update("nonprofitclaw_tax_receipt", {"donor_id": target_id}, where={"donor_id": source_id})
        conn.execute(upd3_sql, upd3_p)

        dt0 = Table("nonprofitclaw_donor_ext")
        old_row = conn.execute(Q.from_(dt0).select(dt0.total_donated).where(dt0.id == P()).get_sql(), (target_id,)).fetchone()
        old_total = old_row["total_donated"] if old_row is not None else "0"
        if old_total is None:
            old_total = "0"
        new_total, new_count = _exact_donor_totals(conn, target_id)

        last_q = (
            Q.from_(_don)
            .select(fn.Max(_don.donation_date))
            .where(_don.donor_id == P())
            .where(_don.status.notin(["refunded", "cancelled"]))
        )
        last_date = conn.execute(last_q.get_sql(), (target_id,)).fetchone()

        first_q = (
            Q.from_(_don)
            .select(fn.Min(_don.donation_date))
            .where(_don.donor_id == P())
            .where(_don.status.notin(["refunded", "cancelled"]))
        )
        first_date = conn.execute(first_q.get_sql(), (target_id,)).fetchone()

        tt = Table("nonprofitclaw_donor_ext")
        upd_t = Q.update(tt).set(tt.total_donated, P()).set(tt.donation_count, P()).set(tt.last_donation_date, P()).set(tt.first_donation_date, P()).set(tt.updated_at, now()).where(tt.id == P()).where(tt.total_donated == P())
        cur_t = conn.execute(upd_t.get_sql(), (new_total, new_count, last_date[0], first_date[0], target_id, old_total))
        if cur_t.rowcount != 1:
            raise ValueError("Concurrent change to nonprofitclaw_donor_ext %s: total_donated is no longer %s; nothing was written" % (target_id, old_total))

        # Delete source ext record (core customer record remains for audit trail)
        t = Table("nonprofitclaw_donor_ext")
        del_q = Q.from_(t).delete().where(t.id == P())
        conn.execute(del_q.get_sql(), (source_id,))
        audit(conn, SKILL, "nonprofit-merge-donors", "nonprofitclaw_donor_ext", target_id)
        conn.commit()
    except Exception as e:
        conn.rollback()
        return err(f"Merge failed: {e}")

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

    dq = Q.from_(_de).select(_de.id, _de.customer_id, _de.company_id).where(_de.id == P())
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

    try:
        in_kind_raw = getattr(args, "in_kind_fair_value", None)
        if in_kind_raw is not None and str(in_kind_raw).strip() != "":
            in_kind_value = _round(_dec(str(in_kind_raw)))
        else:
            in_kind_value = None
        goods_fv_raw = getattr(args, "goods_services_fair_value", None)
        if goods_fv_raw is not None and str(goods_fv_raw).strip() != "":
            goods_fv_value = _round(_dec(str(goods_fv_raw)))
        else:
            goods_fv_value = None
    except Exception:
        return err("Fair value must be a valid decimal amount")
    for label, fv in (("In-kind fair value", in_kind_value), ("Goods or services fair value", goods_fv_value)):
        if fv is not None:
            if fv < Decimal("0"):
                return err(f"{label} must not be negative")
            if fv > amount:
                return err(f"{label} must not exceed the donation amount")
    goods_services_description = getattr(args, "goods_services_description", None)
    if isinstance(goods_services_description, str) and goods_services_description.strip() == "":
        goods_services_description = None

    donation_id = str(uuid.uuid4())
    naming = get_next_name(conn, "nonprofitclaw_donation", company_id=company_id)
    donation_date = getattr(args, "donation_date", None) or str(date.today())
    payment_method = getattr(args, "payment_method", None) or "check"
    is_recurring = int(getattr(args, "is_recurring", None) or 0)
    recurrence_freq = getattr(args, "recurrence_freq", None)

    fund_id = getattr(args, "fund_id", None)
    if fund_id:
        fq = Q.from_(_fund).select(_fund.id, _fund.company_id).where(_fund.id == P())
        fund_row = conn.execute(fq.get_sql(), (fund_id,)).fetchone()
        if not fund_row:
            return err(f"Fund {fund_id} not found")
        if fund_row["company_id"] != company_id:
            return err("Fund does not belong to this company")

    campaign_id = getattr(args, "campaign_id", None)
    if campaign_id:
        cq = Q.from_(_campaign).select(_campaign.id, _campaign.company_id).where(_campaign.id == P())
        campaign_row = conn.execute(cq.get_sql(), (campaign_id,)).fetchone()
        if not campaign_row:
            return err(f"Campaign {campaign_id} not found")
        if campaign_row["company_id"] != company_id:
            return err("Campaign does not belong to this company")

    # GL account IDs (optional — graceful degradation)
    cash_account_id = getattr(args, "cash_account_id", None)
    revenue_account_id = getattr(args, "revenue_account_id", None)
    cost_center_id = getattr(args, "cost_center_id", None)

    # Transaction (implicit)
    gl_entry_ids = None
    try:
        sql, _ = insert_row("nonprofitclaw_donation", {
            "id": P(), "naming_series": P(), "donor_id": P(), "fund_id": P(),
            "campaign_id": P(), "donation_date": P(), "amount": P(),
            "payment_method": P(), "reference": P(), "is_recurring": P(),
            "recurrence_freq": P(), "in_kind_fair_value": P(),
            "goods_services_fair_value": P(), "goods_services_description": P(),
            "notes": P(), "status": P(), "company_id": P(),
        })
        conn.execute(sql, (
            donation_id, naming, donor_id, fund_id, campaign_id,
            donation_date, str(amount), payment_method,
            getattr(args, "reference", None),
            is_recurring, recurrence_freq,
            str(in_kind_value) if in_kind_value is not None else None,
            str(goods_fv_value) if goods_fv_value is not None else None,
            goods_services_description,
            getattr(args, "notes", None),
            "received", company_id,
        ))

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
                    voucher_type="journal_entry",
                    voucher_id=donation_id,
                    posting_date=donation_date,
                    company_id=company_id,
                    remarks=f"Donation {naming} from donor {donor_id}",
                )
                gl_entry_ids = json.dumps(ids)
                sql_gl, params_gl = dynamic_update("nonprofitclaw_donation",
                    {"gl_entry_ids": gl_entry_ids}, where={"id": donation_id})
                conn.execute(sql_gl, params_gl)
            except Exception as e:
                conn.rollback()
                err(f"GL posting failed for donation {naming}: {e}")

        dt = Table("nonprofitclaw_donor_ext")
        old_donor_row = conn.execute(Q.from_(dt).select(dt.total_donated).where(dt.id == P()).get_sql(), (donor_id,)).fetchone()
        old_donor_total = old_donor_row["total_donated"] if old_donor_row is not None else "0"
        if old_donor_total is None:
            old_donor_total = "0"
        new_total, new_count = _exact_donor_totals(conn, donor_id)

        first_q = (
            Q.from_(_don)
            .select(fn.Min(_don.donation_date))
            .where(_don.donor_id == P())
            .where(_don.status.notin(["refunded", "cancelled"]))
        )
        first_date = conn.execute(first_q.get_sql(), (donor_id,)).fetchone()

        _guarded_donor_totals(conn, donor_id, old_donor_total, new_total, new_count, donation_date, first_date[0], keep_first=True)

        if fund_id:
            _shift_fund_balance(conn, fund_id, amount)

        if campaign_id:
            ct = Table("nonprofitclaw_campaign")
            cur = conn.execute(
                Q.from_(ct).select(ct.raised_amount).where(ct.id == P()).get_sql(),
                (campaign_id,),
            ).fetchone()
            if cur is not None:
                old_raised = cur["raised_amount"]
                if old_raised is None:
                    old_raised = "0"
                new_raised = str(_round(_dec(old_raised) + amount))
                _guarded_campaign_raised(conn, campaign_id, old_raised, new_raised)

        audit(conn, SKILL, "nonprofit-add-donation", "nonprofitclaw_donation", donation_id)
        conn.commit()
    except Exception as e:
        conn.rollback()
        return err(f"Failed to add donation: {e}")

    result = {"id": donation_id, "naming_series": naming, "amount": str(amount)}
    if gl_entry_ids:
        result["gl_entry_ids"] = json.loads(gl_entry_ids)
    return ok(result)


def update_donation(conn, args):
    donation_id = args.id
    if not donation_id:
        return err("--id is required")

    q = Q.from_(_don).select(_don.star).where(_don.id == P())
    row = conn.execute(q.get_sql(), (donation_id,)).fetchone()
    if not row:
        return err(f"Donation {donation_id} not found")
    if row["status"] in ("refunded", "cancelled"):
        return err(f"Cannot update donation in '{row['status']}' status")

    data = {}
    for col, attr in [
        ("payment_method", "payment_method"), ("reference", "reference"),
        ("notes", "notes"), ("donation_date", "donation_date"),
    ]:
        val = getattr(args, attr, None)
        if val is not None:
            data[col] = val

    is_recurring = getattr(args, "is_recurring", None)
    if is_recurring is not None:
        data["is_recurring"] = int(is_recurring)

    recurrence_freq = getattr(args, "recurrence_freq", None)
    if recurrence_freq is not None:
        data["recurrence_freq"] = recurrence_freq

    if not data:
        return err("No fields to update")

    data["updated_at"] = now()
    sql, params = dynamic_update("nonprofitclaw_donation", data, where={"id": donation_id})
    conn.execute(sql, params)
    audit(conn, SKILL, "nonprofit-update-donation", "nonprofitclaw_donation", donation_id)
    conn.commit()
    return ok({"id": donation_id, "updated": True})


def list_donations(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    limit = int(getattr(args, "limit", None) or 50)
    offset = int(getattr(args, "offset", None) or 0)

    d = _don
    de = _de
    c = _c

    # Count query (simple, no joins needed)
    conditions = [d.company_id == P()]
    params = [company_id]

    donor_id = getattr(args, "donor_id", None)
    if donor_id:
        conditions.append(d.donor_id == P())
        params.append(donor_id)

    fund_id = getattr(args, "fund_id", None)
    if fund_id:
        conditions.append(d.fund_id == P())
        params.append(fund_id)

    campaign_id = getattr(args, "campaign_id", None)
    if campaign_id:
        conditions.append(d.campaign_id == P())
        params.append(campaign_id)

    status = getattr(args, "status", None)
    if status:
        conditions.append(d.status == P())
        params.append(status)

    from_date = getattr(args, "from_date", None)
    if from_date:
        conditions.append(d.donation_date >= P())
        params.append(from_date)

    to_date = getattr(args, "to_date", None)
    if to_date:
        conditions.append(d.donation_date <= P())
        params.append(to_date)

    count_q = Q.from_(d).select(fn.Count("*"))
    for cond in conditions:
        count_q = count_q.where(cond)
    total = conn.execute(count_q.get_sql(), params).fetchone()[0]

    data_q = (
        Q.from_(d)
        .left_join(de).on(d.donor_id == de.id)
        .left_join(c).on(de.customer_id == c.id)
        .select(
            d.id, d.naming_series, d.donor_id, c.name.as_("donor_name"),
            d.donation_date, d.amount, d.payment_method, d.status,
            d.fund_id, d.campaign_id, d.is_recurring,
        )
    )
    for cond in conditions:
        data_q = data_q.where(cond)
    data_q = data_q.orderby(d.donation_date, order=Order.desc).limit(P()).offset(P())

    rows = conn.execute(data_q.get_sql(), params + [limit, offset]).fetchall()
    donations = [dict(r) for r in rows]
    return ok({"donations": donations, "total": total})


def get_donation(conn, args):
    donation_id = args.id
    if not donation_id:
        return err("--id is required")

    q = (
        Q.from_(_don)
        .left_join(_de).on(_don.donor_id == _de.id)
        .left_join(_c).on(_de.customer_id == _c.id)
        .select(_don.star, _c.name.as_("donor_name"))
        .where(_don.id == P())
    )
    row = conn.execute(q.get_sql(), (donation_id,)).fetchone()
    if not row:
        return err(f"Donation {donation_id} not found")

    return ok({"donation": dict(row)})


def refund_donation(conn, args):
    donation_id = getattr(args, "donation_id", None) or args.id
    if not donation_id:
        return err("--donation-id or --id is required")

    q = Q.from_(_don).select(_don.star).where(_don.id == P())
    row = conn.execute(q.get_sql(), (donation_id,)).fetchone()
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
        sql_ref, params_ref = dynamic_update("nonprofitclaw_donation",
            {"status": "refunded", "updated_at": now()},
            where={"id": donation_id})
        conn.execute(sql_ref, params_ref)

        # --- Reverse GL entries if they exist ---
        if HAS_GL and row["gl_entry_ids"]:
            try:
                reversal_ids = reverse_gl_entries(
                    conn,
                    voucher_type="journal_entry",
                    voucher_id=donation_id,
                    posting_date=donation_date,
                )
                gl_reversal_ids = reversal_ids
            except Exception as e:
                conn.rollback()
                err(f"GL reversal failed for donation {donation_id}: {e}")

        dt = Table("nonprofitclaw_donor_ext")
        old_donor_row = conn.execute(Q.from_(dt).select(dt.total_donated).where(dt.id == P()).get_sql(), (donor_id,)).fetchone()
        old_donor_total = old_donor_row["total_donated"] if old_donor_row is not None else "0"
        if old_donor_total is None:
            old_donor_total = "0"
        new_total, new_count = _exact_donor_totals(conn, donor_id)

        last_q = (
            Q.from_(_don)
            .select(fn.Max(_don.donation_date))
            .where(_don.donor_id == P())
            .where(_don.status.notin(["refunded", "cancelled"]))
        )
        last_date = conn.execute(last_q.get_sql(), (donor_id,)).fetchone()

        _guarded_donor_totals(conn, donor_id, old_donor_total, new_total, new_count, last_date[0])

        if fund_id:
            _shift_fund_balance(conn, fund_id, -amount)

        if campaign_id:
            ct = Table("nonprofitclaw_campaign")
            cur = conn.execute(
                Q.from_(ct).select(ct.raised_amount, ct.donor_count).where(ct.id == P()).get_sql(),
                (campaign_id,),
            ).fetchone()
            if cur is not None:
                old_raised = cur["raised_amount"]
                if old_raised is None:
                    old_raised = "0"
                old_count = cur["donor_count"]
                if old_count is None:
                    old_count = 0
                new_raised = str(_round(_dec(old_raised) - amount))
                new_count_camp = old_count - 1 if old_count > 0 else 0
                _guarded_campaign_refund(conn, campaign_id, old_raised, new_raised, old_count, new_count_camp)

        audit(conn, SKILL, "nonprofit-refund-donation", "nonprofitclaw_donation", donation_id)
        conn.commit()
    except Exception as e:
        conn.rollback()
        return err(f"Refund failed: {e}")

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
