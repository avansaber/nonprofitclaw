#!/usr/bin/env python3
"""NonprofitClaw grants domain — 12 actions."""
import json
import os
import sys
import uuid
from decimal import Decimal, ROUND_HALF_UP

import importlib.util
if importlib.util.find_spec("erpclaw_lib") is None:
    sys.path.insert(0, os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))
from erpclaw_lib.naming import get_next_name, register_prefix
from erpclaw_lib.response import ok, err
from erpclaw_lib.audit import audit

try:
    from erpclaw_lib.gl_posting import insert_gl_entries, reverse_gl_entries
    from erpclaw_lib.query import (
        Q, P, Table, Field, fn, Order, LiteralValue,
        insert_row, update_row, dynamic_update, now,
    )
    HAS_GL = True
except ImportError:
    HAS_GL = False

from erpclaw_lib.query_helpers import get_fiscal_year

SKILL = "nonprofitclaw"

register_prefix("nonprofitclaw_grant_receipt", "NGR-")

# ── Table aliases ──
_grant = Table("nonprofitclaw_grant")
_ge = Table("nonprofitclaw_grant_expense")
_fund = Table("nonprofitclaw_fund")
_receipt = Table("nonprofitclaw_grant_receipt")
_account = Table("account")


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


def _exact_grant_spent(conn, grant_id):
    ge = Table("nonprofitclaw_grant_expense")
    aq = Q.from_(ge).select(ge.amount).where(ge.grant_id == P()).where(ge.status == "approved")
    rows = conn.execute(aq.get_sql(), (grant_id,)).fetchall()
    if not rows:
        return "0"
    total = sum((_dec(r["amount"]) for r in rows), Decimal("0"))
    return str(_round(total))


def _guarded_grant_totals(conn, grant_id, old_spent, old_remaining, new_spent, new_remaining):
    gt = Table("nonprofitclaw_grant")
    upd = Q.update(gt).set(gt.spent_amount, P()).set(gt.remaining_amount, P()).set(gt.updated_at, now()).where(gt.id == P()).where(gt.spent_amount == P()).where(gt.remaining_amount == P())
    cur = conn.execute(upd.get_sql(), (new_spent, new_remaining, grant_id, old_spent, old_remaining))
    if cur.rowcount != 1:
        raise ValueError("Concurrent change to nonprofitclaw_grant %s: spent_amount is no longer %s; nothing was written" % (grant_id, old_spent))


def _guarded_grant_received(conn, grant_id, old_received, old_remaining, new_received, new_remaining):
    if old_received is None:
        old_received = "0"
    if old_remaining is None:
        old_remaining = "0"
    gt = Table("nonprofitclaw_grant")
    upd = Q.update(gt).set(gt.received_amount, P()).set(gt.remaining_amount, P()).set(gt.updated_at, now()).where(gt.id == P()).where(gt.received_amount == P()).where(gt.remaining_amount == P())
    cur = conn.execute(upd.get_sql(), (new_received, new_remaining, grant_id, old_received, old_remaining))
    if cur.rowcount != 1:
        raise ValueError("Concurrent change to nonprofitclaw_grant %s: received_amount is no longer %s; nothing was written" % (grant_id, old_received))


# ------------------------------------------------------------------
# Grant CRUD
# ------------------------------------------------------------------

def add_grant(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    name = args.name
    if not name:
        return err("--name is required")

    grantor_name = getattr(args, "grantor_name", None)
    if not grantor_name:
        return err("--grantor-name is required")

    amount_str = getattr(args, "amount", None)
    if not amount_str:
        return err("--amount is required")
    amount = _round(_dec(amount_str))
    if amount <= Decimal("0"):
        return err("Amount must be positive")

    grant_id = str(uuid.uuid4())
    naming = get_next_name(conn, "nonprofitclaw_grant", company_id=company_id)
    grantor_type = getattr(args, "grantor_type", None) or "foundation"
    grant_type = getattr(args, "grant_type", None) or "project"
    reporting_freq = getattr(args, "reporting_freq", None) or "quarterly"

    fund_id = getattr(args, "fund_id", None)
    if fund_id:
        fq = Q.from_(_fund).select(_fund.id, _fund.company_id).where(_fund.id == P())
        fund_row = conn.execute(fq.get_sql(), (fund_id,)).fetchone()
        if not fund_row:
            return err(f"Fund {fund_id} not found")
        if fund_row["company_id"] != company_id:
            return err("Fund does not belong to this company")

    sql, _ = insert_row("nonprofitclaw_grant", {
        "id": P(), "naming_series": P(), "name": P(), "grantor_name": P(),
        "grantor_type": P(), "grant_type": P(), "amount": P(),
        "remaining_amount": P(), "fund_id": P(), "start_date": P(),
        "end_date": P(), "reporting_freq": P(), "notes": P(), "status": P(),
        "company_id": P(),
    })
    conn.execute(sql, (
        grant_id, naming, name, grantor_name, grantor_type, grant_type,
        str(amount), str(amount), fund_id,
        getattr(args, "start_date", None),
        getattr(args, "end_date", None),
        reporting_freq,
        getattr(args, "notes", None),
        "applied", company_id,
    ))
    audit(conn, SKILL, "nonprofit-add-grant", "nonprofitclaw_grant", grant_id)
    conn.commit()
    return ok({"id": grant_id, "naming_series": naming, "name": name, "amount": str(amount)})


def update_grant(conn, args):
    grant_id = args.id
    if not grant_id:
        return err("--id is required")

    q = Q.from_(_grant).select(_grant.id, _grant.company_id, _grant.status, _grant.fund_id).where(_grant.id == P())
    row = conn.execute(q.get_sql(), (grant_id,)).fetchone()
    if not row:
        return err(f"Grant {grant_id} not found")
    if row["status"] in ("closed", "rejected"):
        return err(f"Cannot update grant in '{row['status']}' status")

    data = {}
    for col, attr in [
        ("name", "name"), ("grantor_name", "grantor_name"),
        ("grantor_type", "grantor_type"), ("grant_type", "grant_type"),
        ("reporting_freq", "reporting_freq"),
        ("start_date", "start_date"), ("end_date", "end_date"),
        ("notes", "notes"),
    ]:
        val = getattr(args, attr, None)
        if val is not None:
            data[col] = val

    fund_id = getattr(args, "fund_id", None)
    if fund_id is not None:
        if fund_id:
            fq = Q.from_(_fund).select(_fund.id, _fund.company_id).where(_fund.id == P())
            fund_row = conn.execute(fq.get_sql(), (fund_id,)).fetchone()
            if not fund_row:
                return err(f"Fund {fund_id} not found")
            if fund_row["company_id"] != row["company_id"]:
                return err("Fund does not belong to this company")
        new_fund = fund_id if fund_id else None
        if new_fund != row["fund_id"] and row["status"] in ("active", "completed"):
            return err(f"Grant {grant_id} is '{row['status']}'; its fund cannot change after activation")
        data["fund_id"] = new_fund

    if not data:
        return err("No fields to update")

    data["updated_at"] = now()
    sql, params = dynamic_update("nonprofitclaw_grant", data, where={"id": grant_id})
    conn.execute(sql, params)
    audit(conn, SKILL, "nonprofit-update-grant", "nonprofitclaw_grant", grant_id)
    conn.commit()
    return ok({"id": grant_id, "updated": True})


def list_grants(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    limit = int(getattr(args, "limit", None) or 50)
    offset = int(getattr(args, "offset", None) or 0)

    conditions = [_grant.company_id == P()]
    params = [company_id]

    status = getattr(args, "status", None)
    if status:
        conditions.append(_grant.status == P())
        params.append(status)

    grantor_type = getattr(args, "grantor_type", None)
    if grantor_type:
        conditions.append(_grant.grantor_type == P())
        params.append(grantor_type)

    grant_type = getattr(args, "grant_type", None)
    if grant_type:
        conditions.append(_grant.grant_type == P())
        params.append(grant_type)

    search = getattr(args, "search", None)
    if search:
        conditions.append(_grant.name.like(P()) | _grant.grantor_name.like(P()))
        params.extend([f"%{search}%", f"%{search}%"])

    count_q = Q.from_(_grant).select(fn.Count("*"))
    for cond in conditions:
        count_q = count_q.where(cond)
    total = conn.execute(count_q.get_sql(), params).fetchone()[0]

    data_q = Q.from_(_grant).select(
        _grant.id, _grant.naming_series, _grant.name, _grant.grantor_name,
        _grant.grantor_type, _grant.grant_type, _grant.amount,
        _grant.received_amount, _grant.spent_amount, _grant.remaining_amount,
        _grant.status, _grant.start_date, _grant.end_date,
    )
    for cond in conditions:
        data_q = data_q.where(cond)
    data_q = data_q.orderby(_grant.created_at, order=Order.desc).limit(P()).offset(P())

    rows = conn.execute(data_q.get_sql(), params + [limit, offset]).fetchall()
    grants = [dict(r) for r in rows]
    return ok({"grants": grants, "total": total})


def get_grant(conn, args):
    grant_id = args.id
    if not grant_id:
        return err("--id is required")

    q = Q.from_(_grant).select(_grant.star).where(_grant.id == P())
    row = conn.execute(q.get_sql(), (grant_id,)).fetchone()
    if not row:
        return err(f"Grant {grant_id} not found")

    # Also fetch expenses summary
    exp_q = (
        Q.from_(_ge)
        .select(
            _ge.category,
            fn.Count("*").as_("count"),
            LiteralValue("SUM(CAST(amount AS NUMERIC))").as_("total"),
        )
        .where(_ge.grant_id == P())
        .where(_ge.status == "approved")
        .groupby(_ge.category)
    )
    expense_summary = conn.execute(exp_q.get_sql(), (grant_id,)).fetchall()

    grant_data = dict(row)
    grant_data["expense_summary"] = [dict(e) for e in expense_summary]
    return ok({"grant": grant_data})


def activate_grant(conn, args):
    grant_id = args.id
    if not grant_id:
        return err("--id is required")

    q = Q.from_(_grant).select(_grant.star).where(_grant.id == P())
    row = conn.execute(q.get_sql(), (grant_id,)).fetchone()
    if not row:
        return err(f"Grant {grant_id} not found")
    if row["status"] not in ("applied", "awarded"):
        return err(f"Grant must be in 'applied' or 'awarded' status to activate, currently '{row['status']}'")

    received_amount = getattr(args, "amount", None)
    if received_amount:
        received = _round(_dec(received_amount))
    else:
        received = _dec(row["amount"])

    sql, params = dynamic_update("nonprofitclaw_grant",
        {"status": "active", "received_amount": str(received),
         "remaining_amount": str(received), "updated_at": now()},
        where={"id": grant_id})
    conn.execute(sql, params)

    # If linked to a fund, update fund balance
    if row["fund_id"]:
        _shift_fund_balance(conn, row["fund_id"], received)

    audit(conn, SKILL, "nonprofit-activate-grant", "nonprofitclaw_grant", grant_id)
    conn.commit()
    return ok({"id": grant_id, "grant_status": "active", "received_amount": str(received)})


# ------------------------------------------------------------------
# Grant Expenses
# ------------------------------------------------------------------

def add_grant_expense(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    grant_id = getattr(args, "grant_id", None)
    if not grant_id:
        return err("--grant-id is required")

    gq = Q.from_(_grant).select(_grant.id, _grant.company_id, _grant.status, _grant.remaining_amount).where(_grant.id == P())
    grant = conn.execute(gq.get_sql(), (grant_id,)).fetchone()
    if not grant:
        return err(f"Grant {grant_id} not found")
    if grant["company_id"] != company_id:
        return err("Grant does not belong to this company")
    if grant["status"] != "active":
        return err(f"Grant must be 'active' to add expenses, currently '{grant['status']}'")

    amount_str = getattr(args, "amount", None)
    if not amount_str:
        return err("--amount is required")
    amount = _round(_dec(amount_str))
    if amount <= Decimal("0"):
        return err("Amount must be positive")

    expense_id = str(uuid.uuid4())
    naming = get_next_name(conn, "nonprofitclaw_grant_expense", company_id=company_id)
    category = getattr(args, "category", None) or "program"

    sql, _ = insert_row("nonprofitclaw_grant_expense", {
        "id": P(), "naming_series": P(), "grant_id": P(), "expense_date": P(),
        "amount": P(), "category": P(), "description": P(),
        "receipt_reference": P(), "status": P(), "company_id": P(),
    })
    conn.execute(sql, (
        expense_id, naming, grant_id,
        getattr(args, "expense_date", None) or str(__import__("datetime").date.today()),
        str(amount), category,
        getattr(args, "description", None),
        getattr(args, "receipt_reference", None),
        "draft", company_id,
    ))
    audit(conn, SKILL, "nonprofit-add-grant-expense", "nonprofitclaw_grant_expense", expense_id)
    conn.commit()
    return ok({"id": expense_id, "naming_series": naming, "amount": str(amount)})


def list_grant_expenses(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    limit = int(getattr(args, "limit", None) or 50)
    offset = int(getattr(args, "offset", None) or 0)

    ge = _ge
    g = _grant

    conditions = [ge.company_id == P()]
    params = [company_id]

    grant_id = getattr(args, "grant_id", None)
    if grant_id:
        conditions.append(ge.grant_id == P())
        params.append(grant_id)

    status = getattr(args, "status", None)
    if status:
        conditions.append(ge.status == P())
        params.append(status)

    category = getattr(args, "category", None)
    if category:
        conditions.append(ge.category == P())
        params.append(category)

    from_date = getattr(args, "from_date", None)
    if from_date:
        conditions.append(ge.expense_date >= P())
        params.append(from_date)

    to_date = getattr(args, "to_date", None)
    if to_date:
        conditions.append(ge.expense_date <= P())
        params.append(to_date)

    count_q = Q.from_(ge).select(fn.Count("*"))
    for cond in conditions:
        count_q = count_q.where(cond)
    total = conn.execute(count_q.get_sql(), params).fetchone()[0]

    data_q = (
        Q.from_(ge)
        .left_join(g).on(ge.grant_id == g.id)
        .select(
            ge.id, ge.naming_series, ge.grant_id, g.name.as_("grant_name"),
            ge.expense_date, ge.amount, ge.category,
            ge.description, ge.receipt_reference, ge.status,
        )
    )
    for cond in conditions:
        data_q = data_q.where(cond)
    data_q = data_q.orderby(ge.expense_date, order=Order.desc).limit(P()).offset(P())

    rows = conn.execute(data_q.get_sql(), params + [limit, offset]).fetchall()
    expenses = [dict(r) for r in rows]
    return ok({"grant_expenses": expenses, "total": total})


def approve_grant_expense(conn, args):
    expense_id = args.id
    if not expense_id:
        return err("--id is required")

    q = Q.from_(_ge).select(_ge.star).where(_ge.id == P())
    row = conn.execute(q.get_sql(), (expense_id,)).fetchone()
    if not row:
        return err(f"Grant expense {expense_id} not found")
    if row["status"] != "draft" and row["status"] != "submitted":
        return err(f"Expense must be in 'draft' or 'submitted' status to approve, currently '{row['status']}'")

    amount = _dec(row["amount"])
    grant_id = row["grant_id"]
    expense_date = row["expense_date"]
    company_id = row["company_id"]

    rq = Q.from_(_grant).select(_grant.received_amount, _grant.fund_id, _grant.status).where(_grant.id == P())
    grant = conn.execute(rq.get_sql(), (grant_id,)).fetchone()
    if not grant:
        return err(f"Grant {grant_id} not found")
    if grant["status"] != "active":
        return err(f"Grant {grant_id} must be 'active' to approve an expense, currently '{grant['status']}'")

    received = _dec(grant["received_amount"])
    available = received - _dec(_exact_grant_spent(conn, grant_id))
    if amount > available:
        return err(f"Expense amount ({str(_round(amount))}) exceeds grant remaining ({str(_round(available))})")

    fund_id = grant["fund_id"]
    if fund_id:
        fq = Q.from_(_fund).select(_fund.name, _fund.current_balance, _fund.fund_type).where(_fund.id == P())
        fund = conn.execute(fq.get_sql(), (fund_id,)).fetchone()
        if fund is not None:
            if fund["fund_type"] == "permanently_restricted":
                return err(f"Fund {fund['name']} is permanently restricted; grant expenses cannot be paid from it")
            fund_balance_text = fund["current_balance"] if fund["current_balance"] is not None else "0"
            if amount > _dec(fund["current_balance"]):
                return err(f"Expense amount ({str(_round(amount))}) exceeds the balance of fund {fund['name']} ({fund_balance_text})")

    # GL account IDs (required — approval posts to the ledger)
    expense_account_id = getattr(args, "expense_account_id", None)
    cash_account_id = getattr(args, "cash_account_id", None)
    cost_center_id = getattr(args, "cost_center_id", None)

    if not HAS_GL:
        return err(f"GL posting is not available; grant expense {expense_id} cannot be approved")
    missing = []
    if not expense_account_id:
        missing.append("--expense-account-id")
    if not cash_account_id:
        missing.append("--cash-account-id")
    if missing:
        return err(f"Approving grant expense {expense_id} posts it to the ledger; missing: {', '.join(missing)}")

    # Transaction (implicit)
    gl_entry_ids = None
    try:
        sql_a, params_a = dynamic_update("nonprofitclaw_grant_expense",
            {"status": "approved"}, where={"id": expense_id})
        conn.execute(sql_a, params_a)

        # --- GL Posting: DR Program Expense, CR Cash/Bank (accounts are required) ---
        gl_entries = [
            {
                "account_id": expense_account_id,
                "debit": str(_round(amount)),
                "credit": "0",
                "cost_center_id": cost_center_id,
            },
            {
                "account_id": cash_account_id,
                "debit": "0",
                "credit": str(_round(amount)),
                "cost_center_id": cost_center_id,
            },
        ]
        try:
            ids = insert_gl_entries(
                conn,
                gl_entries,
                voucher_type="journal_entry",
                voucher_id=expense_id,
                posting_date=expense_date,
                company_id=company_id,
                remarks=f"Grant expense {expense_id} for grant {grant_id}",
            )
            gl_entry_ids = json.dumps(ids)
            sql_gl, params_gl = dynamic_update("nonprofitclaw_grant_expense",
                {"gl_entry_ids": gl_entry_ids}, where={"id": expense_id})
            conn.execute(sql_gl, params_gl)
        except Exception as e:
            conn.rollback()
            err(f"GL posting failed for grant expense {expense_id}: {e}")

        gt0 = Table("nonprofitclaw_grant")
        old_g = conn.execute(Q.from_(gt0).select(gt0.spent_amount, gt0.remaining_amount).where(gt0.id == P()).get_sql(), (grant_id,)).fetchone()
        old_spent = old_g["spent_amount"] if old_g is not None else "0"
        old_remaining = old_g["remaining_amount"] if old_g is not None else "0"
        if old_spent is None:
            old_spent = "0"
        if old_remaining is None:
            old_remaining = "0"
        new_spent = _exact_grant_spent(conn, grant_id)

        new_remaining = str(_round(received - _dec(new_spent)))

        _guarded_grant_totals(conn, grant_id, old_spent, old_remaining, new_spent, new_remaining)

        if fund_id:
            _shift_fund_balance(conn, fund_id, -amount)

        audit(conn, SKILL, "nonprofit-approve-grant-expense", "nonprofitclaw_grant_expense", expense_id)
        conn.commit()
    except Exception as e:
        conn.rollback()
        return err(f"Approval failed: {e}")

    result = {
        "id": expense_id,
        "approved": True,
        "amount": str(amount),
        "grant_spent": new_spent,
        "grant_remaining": new_remaining,
    }
    if gl_entry_ids:
        result["gl_entry_ids"] = json.loads(gl_entry_ids)
    return ok(result)


def reject_grant_expense(conn, args):
    expense_id = getattr(args, "id", None)
    if not expense_id:
        return err("--id is required")

    q = Q.from_(_ge).select(_ge.star).where(_ge.id == P())
    row = conn.execute(q.get_sql(), (expense_id,)).fetchone()
    if not row:
        return err(f"Grant expense {expense_id} not found")
    old_status = row["status"]
    if old_status == "approved":
        return err(f"Grant expense {expense_id} is approved; an approved expense cannot be rejected")
    if old_status == "rejected":
        return err(f"Grant expense {expense_id} is already rejected")

    reason = getattr(args, "reason", None) or None
    amount = row["amount"]
    grant_id = row["grant_id"]

    upd = Q.update(_ge).set(_ge.status, P()).where(_ge.id == P()).where(_ge.status == P())
    cur = conn.execute(upd.get_sql(), ("rejected", expense_id, old_status))
    if cur.rowcount != 1:
        conn.rollback()
        return err(f"Grant expense {expense_id} changed while it was being rejected; nothing was written")

    audit(conn, SKILL, "nonprofit-reject-grant-expense", "nonprofitclaw_grant_expense", expense_id,
          old_values={"status": old_status}, new_values={"status": "rejected", "reason": reason})
    conn.commit()
    return ok({"id": expense_id, "status": "rejected", "amount": str(amount), "grant_id": grant_id})


def grant_status_report(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    q = (
        Q.from_(_grant)
        .select(
            _grant.id, _grant.naming_series, _grant.name, _grant.grantor_name,
            _grant.grant_type, _grant.amount, _grant.received_amount,
            _grant.spent_amount, _grant.remaining_amount, _grant.status,
            _grant.start_date, _grant.end_date, _grant.reporting_freq,
            _grant.next_report_due,
        )
        .where(_grant.company_id == P())
        .where(_grant.status.isin(["active", "awarded"]))
        .orderby(LiteralValue("end_date ASC NULLS LAST"))
    )
    rows = conn.execute(q.get_sql(), (company_id,)).fetchall()

    grants = []
    total_awarded = Decimal("0")
    total_spent = Decimal("0")
    total_remaining = Decimal("0")

    for r in rows:
        grant = dict(r)
        amt = _dec(r["amount"])
        spent = _dec(r["spent_amount"])
        total_awarded += amt
        total_spent += spent
        total_remaining += _dec(r["remaining_amount"])
        if amt > Decimal("0"):
            grant["utilization_pct"] = str(_round(spent / amt * Decimal("100")))
        grants.append(grant)

    return ok({
        "grants": grants,
        "total_awarded": str(_round(total_awarded)),
        "total_spent": str(_round(total_spent)),
        "total_remaining": str(_round(total_remaining)),
        "active_grant_count": len(grants),
    })


def close_grant(conn, args):
    grant_id = args.id
    if not grant_id:
        return err("--id is required")

    q = Q.from_(_grant).select(_grant.star).where(_grant.id == P())
    row = conn.execute(q.get_sql(), (grant_id,)).fetchone()
    if not row:
        return err(f"Grant {grant_id} not found")
    if row["status"] in ("closed", "rejected"):
        return err(f"Grant is already '{row['status']}'")

    # Check for pending expenses
    pq = (
        Q.from_(_ge)
        .select(fn.Count("*"))
        .where(_ge.grant_id == P())
        .where(_ge.status.isin(["draft", "submitted"]))
    )
    pending = conn.execute(pq.get_sql(), (grant_id,)).fetchone()[0]
    if pending > 0:
        return err(f"Cannot close grant: {pending} pending expense(s) exist")

    final_status = "completed" if row["status"] == "active" else "closed"
    sql, params = dynamic_update("nonprofitclaw_grant",
        {"status": final_status, "updated_at": now()},
        where={"id": grant_id})
    conn.execute(sql, params)
    audit(conn, SKILL, "nonprofit-close-grant", "nonprofitclaw_grant", grant_id)
    conn.commit()
    return ok({
        "id": grant_id,
        "grant_status": final_status,
        "spent_amount": row["spent_amount"],
        "remaining_amount": row["remaining_amount"],
    })


# ------------------------------------------------------------------
# Grant receipts
# ------------------------------------------------------------------

def record_grant_receipt(conn, args):
    grant_id = getattr(args, "grant_id", None)
    if not grant_id:
        return err("--grant-id is required")
    company_id = getattr(args, "company_id", None)
    if not company_id:
        return err("--company-id is required")

    gq = Q.from_(_grant).select(_grant.star).where(_grant.id == P())
    grant = conn.execute(gq.get_sql(), (grant_id,)).fetchone()
    if not grant:
        return err(f"Grant {grant_id} not found")
    if grant["company_id"] != company_id:
        return err("Grant does not belong to this company")
    if grant["status"] not in ("active", "completed"):
        return err(f"Grant must be 'active' or 'completed' to record a receipt, currently '{grant['status']}'")

    amount_str = getattr(args, "amount", None)
    if not amount_str:
        return err("--amount is required")
    amount = _round(_dec(amount_str))
    if amount <= Decimal("0"):
        return err("Amount must be positive")

    receipt_date = getattr(args, "receipt_date", None)
    if not receipt_date:
        return err("--receipt-date is required")

    if not HAS_GL:
        return err("GL posting is not available; grant receipt cannot be recorded")

    cash_account_id = getattr(args, "cash_account_id", None)
    revenue_account_id = getattr(args, "revenue_account_id", None)
    missing = []
    if not cash_account_id:
        missing.append("--cash-account-id")
    if not revenue_account_id:
        missing.append("--revenue-account-id")
    if missing:
        return err(f"Recording a grant receipt posts it to the ledger; missing: {', '.join(missing)}")

    award = _dec(grant["amount"])
    old_received_text = grant["received_amount"]
    if old_received_text is None:
        old_received_text = "0"
    old_remaining_text = grant["remaining_amount"]
    if old_remaining_text is None:
        old_remaining_text = "0"
    old_received = _dec(old_received_text)
    available = award - old_received
    if amount > available:
        return err(f"Receipt amount ({str(_round(amount))}) exceeds the grant's award less received ({str(_round(available))})")

    cost_center_id = getattr(args, "cost_center_id", None)
    reference = getattr(args, "reference", None)
    fund_id = grant["fund_id"]
    amount_text = str(_round(amount))
    gl_entry_ids = None
    try:
        receipt_id = str(uuid.uuid4())
        naming = get_next_name(conn, "nonprofitclaw_grant_receipt", company_id=company_id)
        sql, _ = insert_row("nonprofitclaw_grant_receipt", {
            "id": P(), "naming_series": P(), "grant_id": P(), "fund_id": P(),
            "receipt_date": P(), "amount": P(), "reference": P(),
            "cash_account_id": P(), "credit_account_id": P(),
            "cost_center_id": P(), "status": P(), "company_id": P(),
        })
        conn.execute(sql, (
            receipt_id, naming, grant_id, fund_id,
            receipt_date, amount_text, reference,
            cash_account_id, revenue_account_id,
            cost_center_id, "received", company_id,
        ))

        # --- GL Posting: DR Cash/Bank, CR the credit account (caller's choice) ---
        gl_entries = [
            {
                "account_id": cash_account_id,
                "debit": amount_text,
                "credit": "0",
                "cost_center_id": cost_center_id,
            },
            {
                "account_id": revenue_account_id,
                "debit": "0",
                "credit": amount_text,
                "cost_center_id": cost_center_id,
            },
        ]
        try:
            ids = insert_gl_entries(
                conn,
                gl_entries,
                voucher_type="journal_entry",
                voucher_id=receipt_id,
                posting_date=receipt_date,
                company_id=company_id,
                remarks=f"Grant receipt {naming} for grant {grant_id}",
            )
            gl_entry_ids = json.dumps(ids)
            sql_gl, params_gl = dynamic_update("nonprofitclaw_grant_receipt",
                {"gl_entry_ids": gl_entry_ids}, where={"id": receipt_id})
            conn.execute(sql_gl, params_gl)
        except Exception as e:
            conn.rollback()
            err(f"GL posting failed for grant receipt {naming}: {e}")

        new_received = str(_round(old_received + amount))
        new_remaining = str(_round(_dec(new_received) - _dec(_exact_grant_spent(conn, grant_id))))
        _guarded_grant_received(conn, grant_id, old_received_text, old_remaining_text, new_received, new_remaining)

        if fund_id:
            _shift_fund_balance(conn, fund_id, amount)

        audit(conn, SKILL, "nonprofit-record-grant-receipt", "nonprofitclaw_grant_receipt", receipt_id,
            new_values={"grant_id": grant_id, "amount": amount_text, "receipt_date": receipt_date})
        conn.commit()
    except Exception as e:
        conn.rollback()
        return err(f"Recording grant receipt failed: {e}")

    return ok({
        "id": receipt_id,
        "naming_series": naming,
        "grant_id": grant_id,
        "amount": amount_text,
        "grant_received": new_received,
        "grant_remaining": new_remaining,
        "gl_entry_ids": json.loads(gl_entry_ids),
    })


def cancel_grant_receipt(conn, args):
    receipt_id = getattr(args, "id", None)
    if not receipt_id:
        return err("--id is required")

    rq = Q.from_(_receipt).select(_receipt.star).where(_receipt.id == P())
    row = conn.execute(rq.get_sql(), (receipt_id,)).fetchone()
    if not row:
        return err(f"Grant receipt {receipt_id} not found")
    if row["status"] == "cancelled":
        return err(f"Grant receipt {receipt_id} is already cancelled")
    if not HAS_GL:
        return err(f"GL posting is not available; grant receipt {receipt_id} cannot be cancelled")

    grant_id = row["grant_id"]
    receipt_date = row["receipt_date"]
    receipt_amount = _dec(row["amount"])

    gq = Q.from_(_grant).select(_grant.received_amount, _grant.remaining_amount).where(_grant.id == P())
    grow = conn.execute(gq.get_sql(), (grant_id,)).fetchone()
    old_received_text = grow["received_amount"] if grow is not None else "0"
    if old_received_text is None:
        old_received_text = "0"
    old_remaining_text = grow["remaining_amount"] if grow is not None else "0"
    if old_remaining_text is None:
        old_remaining_text = "0"

    if get_fiscal_year(conn, receipt_date, company_id=row["company_id"]) is None:
        return err(f"Cannot cancel grant receipt {receipt_id}: no open fiscal year covers its date {receipt_date}")

    new_received = str(_round(_dec(old_received_text) - receipt_amount))
    spent = _exact_grant_spent(conn, grant_id)
    if _dec(spent) > _dec(new_received):
        return err(f"Cancelling grant receipt {receipt_id} would leave grant {grant_id} with approved expenses ({str(_round(_dec(spent)))}) above its receipts ({new_received})")

    new_remaining = str(_round(_dec(new_received) - _dec(spent)))
    fund_id = row["fund_id"]
    reversal_ids = None
    try:
        upd = Q.update(_receipt).set(_receipt.status, P()).set(_receipt.cancelled_at, now()).set(_receipt.updated_at, now()).where(_receipt.id == P()).where(_receipt.status == P())
        cur = conn.execute(upd.get_sql(), ("cancelled", receipt_id, "received"))
        if cur.rowcount != 1:
            raise ValueError("Concurrent change to nonprofitclaw_grant_receipt %s: status is no longer received; nothing was written" % receipt_id)

        try:
            reversal_ids = reverse_gl_entries(
                conn,
                voucher_type="journal_entry",
                voucher_id=receipt_id,
                posting_date=receipt_date,
            )
        except Exception as e:
            conn.rollback()
            err(f"GL reversal failed for grant receipt {receipt_id}: {e}")

        _guarded_grant_received(conn, grant_id, old_received_text, old_remaining_text, new_received, new_remaining)

        if fund_id:
            _shift_fund_balance(conn, fund_id, -receipt_amount)

        audit(conn, SKILL, "nonprofit-cancel-grant-receipt", "nonprofitclaw_grant_receipt", receipt_id,
            old_values={"status": "received"}, new_values={"status": "cancelled"})
        conn.commit()
    except Exception as e:
        conn.rollback()
        return err(f"Cancelling grant receipt failed: {e}")

    return ok({
        "id": receipt_id,
        "receipt_status": "cancelled",
        "grant_received": new_received,
        "grant_remaining": new_remaining,
        "reversal_gl_entry_ids": reversal_ids,
    })




def _account_refusal(row, account_id, company_id, expected_root, label):
    if row is None:
        return "%s account %s not found" % (label, account_id)
    if row["company_id"] != company_id:
        return "%s account %s does not belong to this company" % (label, account_id)
    if row["is_group"]:
        return "%s account '%s' is a group account: cannot post to group accounts" % (label, row["name"])
    if row["disabled"]:
        return "%s account '%s' is disabled" % (label, row["name"])
    if row["root_type"] != expected_root:
        return "%s account '%s' must be a %s account, currently '%s'" % (label, row["name"], expected_root, row["root_type"])
    return None


def classify_conditional_contribution(conn, args):
    company_id = getattr(args, "company_id", None)
    if not company_id:
        return err("--company-id is required")
    grant_id = getattr(args, "grant_id", None)
    if not grant_id:
        return err("--grant-id is required")

    gq = Q.from_(_grant).select(_grant.star).where(_grant.id == P())
    grant = conn.execute(gq.get_sql(), (grant_id,)).fetchone()
    if not grant:
        return err("Grant %s not found" % grant_id)
    if grant["company_id"] != company_id:
        return err("Grant does not belong to this company")
    if grant["status"] not in ("active", "completed"):
        return err("Grant must be 'active' or 'completed' to classify a conditional contribution, currently '%s'" % grant["status"])

    amount_str = getattr(args, "amount", None)
    if not amount_str:
        return err("--amount is required")
    try:
        amount = _round(_dec(amount_str))
    except Exception:
        return err("Amount '%s' is not a valid Decimal" % amount_str)
    if not amount.is_finite():
        return err("Amount '%s' is not a valid Decimal" % amount_str)
    if amount <= Decimal("0"):
        return err("Amount must be positive")

    receipt_date = getattr(args, "receipt_date", None)
    if not receipt_date:
        return err("--receipt-date is required")

    cash_account_id = getattr(args, "cash_account_id", None)
    revenue_account_id = getattr(args, "revenue_account_id", None)
    advance_account_id = (
        getattr(args, "refundable_advance_account_id", None)
        or getattr(args, "advance_account_id", None)
        or getattr(args, "refundable_account_id", None)
        or getattr(args, "liability_account_id", None)
    )
    missing = []
    if not cash_account_id:
        missing.append("--cash-account-id")
    if not revenue_account_id:
        missing.append("--revenue-account-id")
    if not advance_account_id:
        missing.append("--refundable-advance-account-id")
    if missing:
        return err("Classifying a conditional contribution posts it to the ledger; missing: %s" % ", ".join(missing))

    condition_text = (
        getattr(args, "condition_text", None)
        or getattr(args, "condition", None)
        or getattr(args, "donor_condition", None)
    )
    if condition_text is None or str(condition_text).strip() == "":
        return err("Explicit donor condition text is required (--condition-text); the condition must be supplied, never inferred")

    raw_met = getattr(args, "condition_met", None)
    if raw_met is None or (isinstance(raw_met, str) and raw_met.strip() == ""):
        return err("Explicit --condition-met true/false is required; whether the condition was met must be supplied, never inferred")
    if isinstance(raw_met, bool):
        condition_met = raw_met
    elif isinstance(raw_met, (int, float)):
        if raw_met == 1:
            condition_met = True
        elif raw_met == 0:
            condition_met = False
        else:
            return err("--condition-met must be true or false; whether the condition was met must be supplied, never inferred")
    else:
        text = str(raw_met).strip().lower()
        if text in ("true", "t", "1", "yes", "y"):
            condition_met = True
        elif text in ("false", "f", "0", "no", "n"):
            condition_met = False
        else:
            return err("--condition-met must be true or false; whether the condition was met must be supplied, never inferred")

    if not HAS_GL:
        return err("GL posting is not available; conditional contribution cannot be classified")

    aq = Q.from_(_account).select(_account.star).where(_account.id == P())
    cash_row = conn.execute(aq.get_sql(), (cash_account_id,)).fetchone()
    revenue_row = conn.execute(aq.get_sql(), (revenue_account_id,)).fetchone()
    advance_row = conn.execute(aq.get_sql(), (advance_account_id,)).fetchone()
    for row, aid, expected, label in (
        (cash_row, cash_account_id, "asset", "Cash"),
        (revenue_row, revenue_account_id, "income", "Revenue"),
        (advance_row, advance_account_id, "liability", "Refundable advance"),
    ):
        refusal = _account_refusal(row, aid, company_id, expected, label)
        if refusal:
            return err(refusal)

    if condition_met:
        classification = "contribution_revenue"
        credit_account_id = revenue_account_id
    else:
        classification = "refundable_advance"
        credit_account_id = advance_account_id

    cost_center_id = getattr(args, "cost_center_id", None)
    reference = getattr(args, "reference", None)
    fund_id = grant["fund_id"]
    amount_text = str(amount)
    condition_text = str(condition_text).strip()
    gl_entry_ids = None
    try:
        receipt_id = str(uuid.uuid4())
        naming = get_next_name(conn, "nonprofitclaw_grant_receipt", company_id=company_id)
        sql, _ = insert_row("nonprofitclaw_grant_receipt", {
            "id": P(), "naming_series": P(), "grant_id": P(), "fund_id": P(),
            "receipt_date": P(), "amount": P(), "reference": P(),
            "cash_account_id": P(), "credit_account_id": P(),
            "cost_center_id": P(), "status": P(), "company_id": P(),
        })
        conn.execute(sql, (
            receipt_id, naming, grant_id, fund_id,
            receipt_date, amount_text, reference,
            cash_account_id, credit_account_id,
            cost_center_id, "received", company_id,
        ))

        gl_entries = [
            {
                "account_id": cash_account_id,
                "debit": amount_text,
                "credit": "0",
                "cost_center_id": cost_center_id,
            },
            {
                "account_id": credit_account_id,
                "debit": "0",
                "credit": amount_text,
                "cost_center_id": cost_center_id,
            },
        ]
        try:
            ids = insert_gl_entries(
                conn,
                gl_entries,
                voucher_type="journal_entry",
                voucher_id=receipt_id,
                posting_date=receipt_date,
                company_id=company_id,
                remarks="Conditional contribution %s for grant %s (%s)" % (naming, grant_id, classification),
            )
            gl_entry_ids = json.dumps(ids)
            sql_gl, params_gl = dynamic_update("nonprofitclaw_grant_receipt",
                {"gl_entry_ids": gl_entry_ids}, where={"id": receipt_id})
            conn.execute(sql_gl, params_gl)
        except Exception as e:
            conn.rollback()
            err("GL posting failed for conditional contribution %s: %s" % (naming, e))

        old_received_text = grant["received_amount"]
        if old_received_text is None:
            old_received_text = "0"
        old_remaining_text = grant["remaining_amount"]
        if old_remaining_text is None:
            old_remaining_text = "0"
        new_received = str(_round(_dec(old_received_text) + amount))
        new_remaining = str(_round(_dec(new_received) - _dec(_exact_grant_spent(conn, grant_id))))
        _guarded_grant_received(conn, grant_id, old_received_text, old_remaining_text, new_received, new_remaining)

        if fund_id:
            _shift_fund_balance(conn, fund_id, amount)

        audit(conn, SKILL, "nonprofit-classify-conditional-contribution", "nonprofitclaw_grant_receipt", receipt_id,
            new_values={"grant_id": grant_id, "amount": amount_text, "receipt_date": receipt_date,
                        "classification": classification, "condition_met": condition_met,
                        "condition_text": condition_text, "credit_account_id": credit_account_id})
        conn.commit()
    except Exception as e:
        conn.rollback()
        return err("Classifying conditional contribution failed: %s" % e)

    return ok({
        "id": receipt_id,
        "receipt_id": receipt_id,
        "naming_series": naming,
        "grant_id": grant_id,
        "amount": amount_text,
        "classification": classification,
        "condition_met": condition_met,
        "condition_text": condition_text,
        "credit_account_id": credit_account_id,
        "gl_entry_ids": json.loads(gl_entry_ids),
        "grant_received": new_received,
        "grant_remaining": new_remaining,
    })


ACTIONS = {
    "nonprofit-add-grant": add_grant,
    "nonprofit-update-grant": update_grant,
    "nonprofit-list-grants": list_grants,
    "nonprofit-get-grant": get_grant,
    "nonprofit-activate-grant": activate_grant,
    "nonprofit-add-grant-expense": add_grant_expense,
    "nonprofit-list-grant-expenses": list_grant_expenses,
    "nonprofit-approve-grant-expense": approve_grant_expense,
    "nonprofit-reject-grant-expense": reject_grant_expense,
    "nonprofit-grant-status-report": grant_status_report,
    "nonprofit-close-grant": close_grant,
    "nonprofit-record-grant-receipt": record_grant_receipt,
    "nonprofit-cancel-grant-receipt": cancel_grant_receipt,
    "nonprofit-classify-conditional-contribution": classify_conditional_contribution,
}
