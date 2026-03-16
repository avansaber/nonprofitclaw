#!/usr/bin/env python3
"""NonprofitClaw grants domain — 10 actions."""
import json
import os
import sys
import uuid
from decimal import Decimal, ROUND_HALF_UP

sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
from erpclaw_lib.naming import get_next_name
from erpclaw_lib.response import ok, err
from erpclaw_lib.audit import audit

try:
    from erpclaw_lib.gl_posting import insert_gl_entries
    from erpclaw_lib.query import (
        Q, P, Table, Field, fn, Order, LiteralValue,
        insert_row, update_row, dynamic_update,
    )
    HAS_GL = True
except ImportError:
    HAS_GL = False

SKILL = "nonprofitclaw"

# ── Table aliases ──
_grant = Table("nonprofitclaw_grant")
_ge = Table("nonprofitclaw_grant_expense")
_fund = Table("nonprofitclaw_fund")


def _dec(val):
    if val is None:
        return Decimal("0")
    return Decimal(str(val))


def _round(val):
    return val.quantize(Decimal("0.01"), ROUND_HALF_UP)


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
        fq = Q.from_(_fund).select(_fund.id).where(_fund.id == P())
        if not conn.execute(fq.get_sql(), (fund_id,)).fetchone():
            return err(f"Fund {fund_id} not found")

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
    conn.commit()
    audit(conn, SKILL, "nonprofit-add-grant", grant_id, company_id)
    return ok({"id": grant_id, "naming_series": naming, "name": name, "amount": str(amount)})


def update_grant(conn, args):
    grant_id = args.id
    if not grant_id:
        return err("--id is required")

    q = Q.from_(_grant).select(_grant.id, _grant.company_id, _grant.status).where(_grant.id == P())
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
            fq = Q.from_(_fund).select(_fund.id).where(_fund.id == P())
            if not conn.execute(fq.get_sql(), (fund_id,)).fetchone():
                return err(f"Fund {fund_id} not found")
        data["fund_id"] = fund_id if fund_id else None

    if not data:
        return err("No fields to update")

    data["updated_at"] = LiteralValue("datetime('now')")
    sql, params = dynamic_update("nonprofitclaw_grant", data, where={"id": grant_id})
    conn.execute(sql, params)
    conn.commit()
    audit(conn, SKILL, "nonprofit-update-grant", grant_id, row["company_id"])
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
            LiteralValue("SUM(CAST(amount AS REAL))").as_("total"),
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
         "remaining_amount": str(received), "updated_at": LiteralValue("datetime('now')")},
        where={"id": grant_id})
    conn.execute(sql, params)

    # If linked to a fund, update fund balance
    if row["fund_id"]:
        ft = Table("nonprofitclaw_fund")
        fund_upd = (
            Q.update(ft)
            .set(ft.current_balance, LiteralValue("CAST(CAST(current_balance AS REAL) + ? AS TEXT)"))
            .set(ft.updated_at, LiteralValue("datetime('now')"))
            .where(ft.id == P())
        )
        conn.execute(fund_upd.get_sql(), (float(received), row["fund_id"]))

    conn.commit()
    audit(conn, SKILL, "nonprofit-activate-grant", grant_id, row["company_id"])
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
    conn.commit()
    audit(conn, SKILL, "nonprofit-add-grant-expense", expense_id, company_id)
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

    rq = Q.from_(_grant).select(_grant.remaining_amount).where(_grant.id == P())
    grant = conn.execute(rq.get_sql(), (grant_id,)).fetchone()
    if not grant:
        return err(f"Grant {grant_id} not found")

    remaining = _dec(grant["remaining_amount"])
    if amount > remaining:
        return err(f"Expense amount ({str(amount)}) exceeds grant remaining ({str(remaining)})")

    # GL account IDs (optional — graceful degradation)
    expense_account_id = getattr(args, "expense_account_id", None)
    cash_account_id = getattr(args, "cash_account_id", None)
    cost_center_id = getattr(args, "cost_center_id", None)

    # Transaction (implicit)
    gl_entry_ids = None
    try:
        sql_a, params_a = dynamic_update("nonprofitclaw_grant_expense",
            {"status": "approved"}, where={"id": expense_id})
        conn.execute(sql_a, params_a)

        # --- GL Posting: DR Program Expense, CR Cash/Bank ---
        if HAS_GL and expense_account_id and cash_account_id:
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
                    voucher_type="grant_expense",
                    voucher_id=expense_id,
                    posting_date=expense_date,
                    company_id=company_id,
                    remarks=f"Grant expense {expense_id} for grant {grant_id}",
                )
                gl_entry_ids = json.dumps(ids)
                sql_gl, params_gl = dynamic_update("nonprofitclaw_grant_expense",
                    {"gl_entry_ids": gl_entry_ids}, where={"id": expense_id})
                conn.execute(sql_gl, params_gl)
            except (ValueError, Exception):
                # GL posting failed — expense approval still proceeds
                pass

        spent_q = (
            Q.from_(_ge)
            .select(LiteralValue("SUM(CAST(amount AS REAL))"))
            .where(_ge.grant_id == P())
            .where(_ge.status == "approved")
        )
        new_spent = str(_round(_dec(
            conn.execute(spent_q.get_sql(), (grant_id,)).fetchone()[0]
        )))

        aq = Q.from_(_grant).select(_grant.amount).where(_grant.id == P())
        grant_full = conn.execute(aq.get_sql(), (grant_id,)).fetchone()
        new_remaining = str(_round(_dec(grant_full["amount"]) - _dec(new_spent)))

        sql_g, params_g = dynamic_update("nonprofitclaw_grant",
            {"spent_amount": new_spent, "remaining_amount": new_remaining,
             "updated_at": LiteralValue("datetime('now')")},
            where={"id": grant_id})
        conn.execute(sql_g, params_g)

        conn.commit()
    except Exception as e:
        conn.rollback()
        return err(f"Approval failed: {e}")

    audit(conn, SKILL, "nonprofit-approve-grant-expense", expense_id, company_id)
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
        {"status": final_status, "updated_at": LiteralValue("datetime('now')")},
        where={"id": grant_id})
    conn.execute(sql, params)
    conn.commit()
    audit(conn, SKILL, "nonprofit-close-grant", grant_id, row["company_id"])
    return ok({
        "id": grant_id,
        "grant_status": final_status,
        "spent_amount": row["spent_amount"],
        "remaining_amount": row["remaining_amount"],
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
    "nonprofit-grant-status-report": grant_status_report,
    "nonprofit-close-grant": close_grant,
}
