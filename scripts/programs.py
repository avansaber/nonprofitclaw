#!/usr/bin/env python3
"""NonprofitClaw programs domain — 5 actions."""
import os
import sys
import uuid
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
# Program CRUD
# ------------------------------------------------------------------

def add_program(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    name = args.name
    if not name:
        return err("--name is required")

    program_id = str(uuid.uuid4())
    naming = get_next_name(conn, "nonprofitclaw_program", company_id=company_id)

    budget = getattr(args, "budget", None)
    if budget:
        budget = str(_round(_dec(budget)))
    else:
        budget = "0"

    fund_id = getattr(args, "fund_id", None)
    if fund_id:
        fund = conn.execute("SELECT id FROM nonprofitclaw_fund WHERE id=?", (fund_id,)).fetchone()
        if not fund:
            return err(f"Fund {fund_id} not found")

    conn.execute(
        """INSERT INTO nonprofitclaw_program
           (id, naming_series, name, description, fund_id, budget,
            beneficiary_count, start_date, end_date,
            outcome_metrics, is_active, company_id)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            program_id, naming, name,
            getattr(args, "description", None),
            fund_id, budget,
            int(getattr(args, "beneficiary_count", None) or 0),
            getattr(args, "start_date", None),
            getattr(args, "end_date", None),
            getattr(args, "outcome_metrics", None),
            1, company_id,
        ),
    )
    conn.commit()
    audit(conn, SKILL, "nonprofit-add-program", program_id, company_id)
    return ok({"id": program_id, "naming_series": naming, "name": name})


def update_program(conn, args):
    program_id = args.id
    if not program_id:
        return err("--id is required")

    row = conn.execute("SELECT id, company_id FROM nonprofitclaw_program WHERE id=?", (program_id,)).fetchone()
    if not row:
        return err(f"Program {program_id} not found")

    fields = []
    values = []
    for col, attr in [
        ("name", "name"), ("description", "description"),
        ("start_date", "start_date"), ("end_date", "end_date"),
        ("outcome_metrics", "outcome_metrics"),
    ]:
        val = getattr(args, attr, None)
        if val is not None:
            fields.append(f"{col}=?")
            values.append(val)

    budget = getattr(args, "budget", None)
    if budget is not None:
        fields.append("budget=?")
        values.append(str(_round(_dec(budget))))

    fund_id = getattr(args, "fund_id", None)
    if fund_id is not None:
        if fund_id:
            fund = conn.execute("SELECT id FROM nonprofitclaw_fund WHERE id=?", (fund_id,)).fetchone()
            if not fund:
                return err(f"Fund {fund_id} not found")
        fields.append("fund_id=?")
        values.append(fund_id if fund_id else None)

    is_active = getattr(args, "is_active", None)
    if is_active is not None:
        fields.append("is_active=?")
        values.append(int(is_active))

    beneficiary_count = getattr(args, "beneficiary_count", None)
    if beneficiary_count is not None:
        fields.append("beneficiary_count=?")
        values.append(int(beneficiary_count))

    if not fields:
        return err("No fields to update")

    fields.append("updated_at=datetime('now')")
    values.append(program_id)
    conn.execute(f"UPDATE nonprofitclaw_program SET {', '.join(fields)} WHERE id=?", values)
    conn.commit()
    audit(conn, SKILL, "nonprofit-update-program", program_id, row["company_id"])
    return ok({"id": program_id, "updated": True})


def list_programs(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    limit = int(getattr(args, "limit", None) or 50)
    offset = int(getattr(args, "offset", None) or 0)

    where = ["p.company_id=?"]
    params = [company_id]

    is_active = getattr(args, "is_active", None)
    if is_active is not None:
        where.append("p.is_active=?")
        params.append(int(is_active))

    search = getattr(args, "search", None)
    if search:
        where.append("p.name LIKE ?")
        params.append(f"%{search}%")

    fund_id = getattr(args, "fund_id", None)
    if fund_id:
        where.append("p.fund_id=?")
        params.append(fund_id)

    where_sql = " AND ".join(where)

    total = conn.execute(
        f"SELECT COUNT(*) FROM nonprofitclaw_program p WHERE {where_sql}", params
    ).fetchone()[0]

    rows = conn.execute(
        f"""SELECT p.id, p.naming_series, p.name, p.description,
                   p.fund_id, f.name as fund_name,
                   p.budget, p.spent, p.beneficiary_count,
                   p.start_date, p.end_date, p.is_active
            FROM nonprofitclaw_program p
            LEFT JOIN nonprofitclaw_fund f ON p.fund_id = f.id
            WHERE {where_sql}
            ORDER BY p.created_at DESC LIMIT ? OFFSET ?""",
        params + [limit, offset],
    ).fetchall()

    programs = [dict(r) for r in rows]
    return ok({"programs": programs, "total": total})


def get_program(conn, args):
    program_id = args.id
    if not program_id:
        return err("--id is required")

    row = conn.execute(
        """SELECT p.*, f.name as fund_name
           FROM nonprofitclaw_program p
           LEFT JOIN nonprofitclaw_fund f ON p.fund_id = f.id
           WHERE p.id=?""",
        (program_id,),
    ).fetchone()
    if not row:
        return err(f"Program {program_id} not found")

    # Get volunteer shift count for this program
    shift_stats = conn.execute(
        """SELECT COUNT(*) as shift_count,
                  SUM(CAST(hours AS REAL)) as total_hours
           FROM nonprofitclaw_volunteer_shift
           WHERE program_id=? AND status='completed'""",
        (program_id,),
    ).fetchone()

    program_data = dict(row)
    program_data["volunteer_shifts"] = shift_stats["shift_count"] or 0
    program_data["volunteer_hours"] = str(_round(_dec(shift_stats["total_hours"]))) if shift_stats["total_hours"] else "0.00"
    return ok({"program": program_data})


def update_program_outcomes(conn, args):
    program_id = args.id
    if not program_id:
        return err("--id is required")

    row = conn.execute("SELECT id, company_id FROM nonprofitclaw_program WHERE id=?", (program_id,)).fetchone()
    if not row:
        return err(f"Program {program_id} not found")

    fields = []
    values = []

    beneficiary_count = getattr(args, "beneficiary_count", None)
    if beneficiary_count is not None:
        fields.append("beneficiary_count=?")
        values.append(int(beneficiary_count))

    outcome_metrics = getattr(args, "outcome_metrics", None)
    if outcome_metrics is not None:
        fields.append("outcome_metrics=?")
        values.append(outcome_metrics)

    if not fields:
        return err("Provide --beneficiary-count and/or --outcome-metrics")

    fields.append("updated_at=datetime('now')")
    values.append(program_id)
    conn.execute(f"UPDATE nonprofitclaw_program SET {', '.join(fields)} WHERE id=?", values)
    conn.commit()
    audit(conn, SKILL, "nonprofit-update-program-outcomes", program_id, row["company_id"])
    return ok({"id": program_id, "outcomes_updated": True})


ACTIONS = {
    "nonprofit-add-program": add_program,
    "nonprofit-update-program": update_program,
    "nonprofit-list-programs": list_programs,
    "nonprofit-get-program": get_program,
    "nonprofit-update-program-outcomes": update_program_outcomes,
}
