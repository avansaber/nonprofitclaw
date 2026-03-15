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
    from erpclaw_lib.query import Q, P, Table, Field, fn, Order, insert_row, update_row
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
        fund = conn.execute(Q.from_(Table("nonprofitclaw_fund")).select(Field("id")).where(Field("id") == P()).get_sql(), (fund_id,)).fetchone()
        if not fund:
            return err(f"Fund {fund_id} not found")

    conn.execute(
        """INSERT INTO nonprofitclaw_grant
           (id, naming_series, name, grantor_name, grantor_type, grant_type,
            amount, remaining_amount, fund_id,
            start_date, end_date, reporting_freq,
            notes, status, company_id)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            grant_id, naming, name, grantor_name, grantor_type, grant_type,
            str(amount), str(amount), fund_id,
            getattr(args, "start_date", None),
            getattr(args, "end_date", None),
            reporting_freq,
            getattr(args, "notes", None),
            "applied", company_id,
        ),
    )
    conn.commit()
    audit(conn, SKILL, "nonprofit-add-grant", grant_id, company_id)
    return ok({"id": grant_id, "naming_series": naming, "name": name, "amount": str(amount)})


def update_grant(conn, args):
    grant_id = args.id
    if not grant_id:
        return err("--id is required")

    row = conn.execute(Q.from_(Table("nonprofitclaw_grant")).select(Field("id"), Field("company_id"), Field("status")).where(Field("id") == P()).get_sql(), (grant_id,)).fetchone()
    if not row:
        return err(f"Grant {grant_id} not found")
    if row["status"] in ("closed", "rejected"):
        return err(f"Cannot update grant in '{row['status']}' status")

    fields = []
    values = []
    for col, attr in [
        ("name", "name"), ("grantor_name", "grantor_name"),
        ("grantor_type", "grantor_type"), ("grant_type", "grant_type"),
        ("reporting_freq", "reporting_freq"),
        ("start_date", "start_date"), ("end_date", "end_date"),
        ("notes", "notes"),
    ]:
        val = getattr(args, attr, None)
        if val is not None:
            fields.append(f"{col}=?")
            values.append(val)

    fund_id = getattr(args, "fund_id", None)
    if fund_id is not None:
        if fund_id:
            fund = conn.execute(Q.from_(Table("nonprofitclaw_fund")).select(Field("id")).where(Field("id") == P()).get_sql(), (fund_id,)).fetchone()
            if not fund:
                return err(f"Fund {fund_id} not found")
        fields.append("fund_id=?")
        values.append(fund_id if fund_id else None)

    if not fields:
        return err("No fields to update")

    fields.append("updated_at=datetime('now')")
    values.append(grant_id)
    conn.execute(f"UPDATE nonprofitclaw_grant SET {', '.join(fields)} WHERE id=?", values)
    conn.commit()
    audit(conn, SKILL, "nonprofit-update-grant", grant_id, row["company_id"])
    return ok({"id": grant_id, "updated": True})


def list_grants(conn, args):
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

    grantor_type = getattr(args, "grantor_type", None)
    if grantor_type:
        where.append("grantor_type=?")
        params.append(grantor_type)

    grant_type = getattr(args, "grant_type", None)
    if grant_type:
        where.append("grant_type=?")
        params.append(grant_type)

    search = getattr(args, "search", None)
    if search:
        where.append("(name LIKE ? OR grantor_name LIKE ?)")
        params.extend([f"%{search}%", f"%{search}%"])

    where_sql = " AND ".join(where)

    total = conn.execute(
        f"SELECT COUNT(*) FROM nonprofitclaw_grant WHERE {where_sql}", params
    ).fetchone()[0]

    rows = conn.execute(
        f"""SELECT id, naming_series, name, grantor_name, grantor_type, grant_type,
                   amount, received_amount, spent_amount, remaining_amount,
                   status, start_date, end_date
            FROM nonprofitclaw_grant WHERE {where_sql}
            ORDER BY created_at DESC LIMIT ? OFFSET ?""",
        params + [limit, offset],
    ).fetchall()

    grants = [dict(r) for r in rows]
    return ok({"grants": grants, "total": total})


def get_grant(conn, args):
    grant_id = args.id
    if not grant_id:
        return err("--id is required")

    row = conn.execute(Q.from_(Table("nonprofitclaw_grant")).select(Table("nonprofitclaw_grant").star).where(Field("id") == P()).get_sql(), (grant_id,)).fetchone()
    if not row:
        return err(f"Grant {grant_id} not found")

    # Also fetch expenses summary
    expense_summary = conn.execute(
        """SELECT category,
                  COUNT(*) as count,
                  SUM(CAST(amount AS REAL)) as total
           FROM nonprofitclaw_grant_expense
           WHERE grant_id=? AND status='approved'
           GROUP BY category""",
        (grant_id,),
    ).fetchall()

    grant_data = dict(row)
    grant_data["expense_summary"] = [dict(e) for e in expense_summary]
    return ok({"grant": grant_data})


def activate_grant(conn, args):
    grant_id = args.id
    if not grant_id:
        return err("--id is required")

    row = conn.execute(Q.from_(Table("nonprofitclaw_grant")).select(Table("nonprofitclaw_grant").star).where(Field("id") == P()).get_sql(), (grant_id,)).fetchone()
    if not row:
        return err(f"Grant {grant_id} not found")
    if row["status"] not in ("applied", "awarded"):
        return err(f"Grant must be in 'applied' or 'awarded' status to activate, currently '{row['status']}'")

    received_amount = getattr(args, "amount", None)
    if received_amount:
        received = _round(_dec(received_amount))
    else:
        received = _dec(row["amount"])

    conn.execute(
        """UPDATE nonprofitclaw_grant SET
            status='active',
            received_amount=?,
            remaining_amount=?,
            updated_at=datetime('now')
           WHERE id=?""",
        (str(received), str(received), grant_id),
    )

    # If linked to a fund, update fund balance
    if row["fund_id"]:
        conn.execute(
            """UPDATE nonprofitclaw_fund SET
                current_balance = CAST(
                    CAST(current_balance AS REAL) + ? AS TEXT
                ), updated_at=datetime('now')
               WHERE id=?""",
            (float(received), row["fund_id"]),
        )

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

    grant = conn.execute(
        "SELECT id, company_id, status, remaining_amount FROM nonprofitclaw_grant WHERE id=?",
        (grant_id,),
    ).fetchone()
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

    conn.execute(
        """INSERT INTO nonprofitclaw_grant_expense
           (id, naming_series, grant_id, expense_date, amount, category,
            description, receipt_reference, status, company_id)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (
            expense_id, naming, grant_id,
            getattr(args, "expense_date", None) or str(__import__("datetime").date.today()),
            str(amount), category,
            getattr(args, "description", None),
            getattr(args, "receipt_reference", None),
            "draft", company_id,
        ),
    )
    conn.commit()
    audit(conn, SKILL, "nonprofit-add-grant-expense", expense_id, company_id)
    return ok({"id": expense_id, "naming_series": naming, "amount": str(amount)})


def list_grant_expenses(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    limit = int(getattr(args, "limit", None) or 50)
    offset = int(getattr(args, "offset", None) or 0)

    where = ["ge.company_id=?"]
    params = [company_id]

    grant_id = getattr(args, "grant_id", None)
    if grant_id:
        where.append("ge.grant_id=?")
        params.append(grant_id)

    status = getattr(args, "status", None)
    if status:
        where.append("ge.status=?")
        params.append(status)

    category = getattr(args, "category", None)
    if category:
        where.append("ge.category=?")
        params.append(category)

    from_date = getattr(args, "from_date", None)
    if from_date:
        where.append("ge.expense_date >= ?")
        params.append(from_date)

    to_date = getattr(args, "to_date", None)
    if to_date:
        where.append("ge.expense_date <= ?")
        params.append(to_date)

    where_sql = " AND ".join(where)

    total = conn.execute(
        f"SELECT COUNT(*) FROM nonprofitclaw_grant_expense ge WHERE {where_sql}", params
    ).fetchone()[0]

    rows = conn.execute(
        f"""SELECT ge.id, ge.naming_series, ge.grant_id, g.name as grant_name,
                   ge.expense_date, ge.amount, ge.category,
                   ge.description, ge.receipt_reference, ge.status
            FROM nonprofitclaw_grant_expense ge
            LEFT JOIN nonprofitclaw_grant g ON ge.grant_id = g.id
            WHERE {where_sql}
            ORDER BY ge.expense_date DESC LIMIT ? OFFSET ?""",
        params + [limit, offset],
    ).fetchall()

    expenses = [dict(r) for r in rows]
    return ok({"grant_expenses": expenses, "total": total})


def approve_grant_expense(conn, args):
    expense_id = args.id
    if not expense_id:
        return err("--id is required")

    row = conn.execute(Q.from_(Table("nonprofitclaw_grant_expense")).select(Table("nonprofitclaw_grant_expense").star).where(Field("id") == P()).get_sql(), (expense_id,)).fetchone()
    if not row:
        return err(f"Grant expense {expense_id} not found")
    if row["status"] != "draft" and row["status"] != "submitted":
        return err(f"Expense must be in 'draft' or 'submitted' status to approve, currently '{row['status']}'")

    amount = _dec(row["amount"])
    grant_id = row["grant_id"]
    expense_date = row["expense_date"]
    company_id = row["company_id"]

    grant = conn.execute(Q.from_(Table("nonprofitclaw_grant")).select(Field("remaining_amount")).where(Field("id") == P()).get_sql(), (grant_id,)).fetchone()
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
        conn.execute(
            "UPDATE nonprofitclaw_grant_expense SET status='approved' WHERE id=?",
            (expense_id,),
        )

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
                conn.execute(
                    "UPDATE nonprofitclaw_grant_expense SET gl_entry_ids=? WHERE id=?",
                    (gl_entry_ids, expense_id),
                )
            except (ValueError, Exception):
                # GL posting failed — expense approval still proceeds
                pass

        new_spent = str(_round(_dec(
            conn.execute(
                "SELECT SUM(CAST(amount AS REAL)) FROM nonprofitclaw_grant_expense WHERE grant_id=? AND status='approved'",
                (grant_id,),
            ).fetchone()[0]
        )))

        grant_full = conn.execute(Q.from_(Table("nonprofitclaw_grant")).select(Field("amount")).where(Field("id") == P()).get_sql(), (grant_id,)).fetchone()
        new_remaining = str(_round(_dec(grant_full["amount"]) - _dec(new_spent)))

        conn.execute(
            """UPDATE nonprofitclaw_grant SET
                spent_amount=?, remaining_amount=?,
                updated_at=datetime('now')
               WHERE id=?""",
            (new_spent, new_remaining, grant_id),
        )

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

    rows = conn.execute(
        """SELECT id, naming_series, name, grantor_name, grant_type,
                  amount, received_amount, spent_amount, remaining_amount,
                  status, start_date, end_date, reporting_freq, next_report_due
           FROM nonprofitclaw_grant
           WHERE company_id=? AND status IN ('active','awarded')
           ORDER BY end_date ASC NULLS LAST""",
        (company_id,),
    ).fetchall()

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

    row = conn.execute(Q.from_(Table("nonprofitclaw_grant")).select(Table("nonprofitclaw_grant").star).where(Field("id") == P()).get_sql(), (grant_id,)).fetchone()
    if not row:
        return err(f"Grant {grant_id} not found")
    if row["status"] in ("closed", "rejected"):
        return err(f"Grant is already '{row['status']}'")

    # Check for pending expenses
    pending = conn.execute(
        "SELECT COUNT(*) FROM nonprofitclaw_grant_expense WHERE grant_id=? AND status IN ('draft','submitted')",
        (grant_id,),
    ).fetchone()[0]
    if pending > 0:
        return err(f"Cannot close grant: {pending} pending expense(s) exist")

    final_status = "completed" if row["status"] == "active" else "closed"
    conn.execute(
        "UPDATE nonprofitclaw_grant SET status=?, updated_at=datetime('now') WHERE id=?",
        (final_status, grant_id),
    )
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
