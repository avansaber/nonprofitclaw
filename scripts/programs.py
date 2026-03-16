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
from erpclaw_lib.query import (
    Q, P, Table, Field, fn, Order, LiteralValue,
    insert_row, update_row, dynamic_update,
)

SKILL = "nonprofitclaw"

# ── Table aliases ──
_prog = Table("nonprofitclaw_program")
_fund = Table("nonprofitclaw_fund")
_vs = Table("nonprofitclaw_volunteer_shift")


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
        fq = Q.from_(_fund).select(_fund.id).where(_fund.id == P())
        if not conn.execute(fq.get_sql(), (fund_id,)).fetchone():
            return err(f"Fund {fund_id} not found")

    sql, _ = insert_row("nonprofitclaw_program", {
        "id": P(), "naming_series": P(), "name": P(), "description": P(),
        "fund_id": P(), "budget": P(), "beneficiary_count": P(),
        "start_date": P(), "end_date": P(), "outcome_metrics": P(),
        "is_active": P(), "company_id": P(),
    })
    conn.execute(sql, (
        program_id, naming, name,
        getattr(args, "description", None),
        fund_id, budget,
        int(getattr(args, "beneficiary_count", None) or 0),
        getattr(args, "start_date", None),
        getattr(args, "end_date", None),
        getattr(args, "outcome_metrics", None),
        1, company_id,
    ))
    conn.commit()
    audit(conn, SKILL, "nonprofit-add-program", program_id, company_id)
    return ok({"id": program_id, "naming_series": naming, "name": name})


def update_program(conn, args):
    program_id = args.id
    if not program_id:
        return err("--id is required")

    q = Q.from_(_prog).select(_prog.id, _prog.company_id).where(_prog.id == P())
    row = conn.execute(q.get_sql(), (program_id,)).fetchone()
    if not row:
        return err(f"Program {program_id} not found")

    data = {}
    for col, attr in [
        ("name", "name"), ("description", "description"),
        ("start_date", "start_date"), ("end_date", "end_date"),
        ("outcome_metrics", "outcome_metrics"),
    ]:
        val = getattr(args, attr, None)
        if val is not None:
            data[col] = val

    budget = getattr(args, "budget", None)
    if budget is not None:
        data["budget"] = str(_round(_dec(budget)))

    fund_id = getattr(args, "fund_id", None)
    if fund_id is not None:
        if fund_id:
            fq = Q.from_(_fund).select(_fund.id).where(_fund.id == P())
            if not conn.execute(fq.get_sql(), (fund_id,)).fetchone():
                return err(f"Fund {fund_id} not found")
        data["fund_id"] = fund_id if fund_id else None

    is_active = getattr(args, "is_active", None)
    if is_active is not None:
        data["is_active"] = int(is_active)

    beneficiary_count = getattr(args, "beneficiary_count", None)
    if beneficiary_count is not None:
        data["beneficiary_count"] = int(beneficiary_count)

    if not data:
        return err("No fields to update")

    data["updated_at"] = LiteralValue("datetime('now')")
    sql, params = dynamic_update("nonprofitclaw_program", data, where={"id": program_id})
    conn.execute(sql, params)
    conn.commit()
    audit(conn, SKILL, "nonprofit-update-program", program_id, row["company_id"])
    return ok({"id": program_id, "updated": True})


def list_programs(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    limit = int(getattr(args, "limit", None) or 50)
    offset = int(getattr(args, "offset", None) or 0)

    p = _prog
    f = _fund

    conditions = [p.company_id == P()]
    params = [company_id]

    is_active = getattr(args, "is_active", None)
    if is_active is not None:
        conditions.append(p.is_active == P())
        params.append(int(is_active))

    search = getattr(args, "search", None)
    if search:
        conditions.append(p.name.like(P()))
        params.append(f"%{search}%")

    fund_id = getattr(args, "fund_id", None)
    if fund_id:
        conditions.append(p.fund_id == P())
        params.append(fund_id)

    count_q = Q.from_(p).select(fn.Count("*"))
    for cond in conditions:
        count_q = count_q.where(cond)
    total = conn.execute(count_q.get_sql(), params).fetchone()[0]

    data_q = (
        Q.from_(p)
        .left_join(f).on(p.fund_id == f.id)
        .select(
            p.id, p.naming_series, p.name, p.description,
            p.fund_id, f.name.as_("fund_name"),
            p.budget, p.spent, p.beneficiary_count,
            p.start_date, p.end_date, p.is_active,
        )
    )
    for cond in conditions:
        data_q = data_q.where(cond)
    data_q = data_q.orderby(p.created_at, order=Order.desc).limit(P()).offset(P())

    rows = conn.execute(data_q.get_sql(), params + [limit, offset]).fetchall()
    programs = [dict(r) for r in rows]
    return ok({"programs": programs, "total": total})


def get_program(conn, args):
    program_id = args.id
    if not program_id:
        return err("--id is required")

    p = _prog
    f = _fund
    q = (
        Q.from_(p)
        .left_join(f).on(p.fund_id == f.id)
        .select(p.star, f.name.as_("fund_name"))
        .where(p.id == P())
    )
    row = conn.execute(q.get_sql(), (program_id,)).fetchone()
    if not row:
        return err(f"Program {program_id} not found")

    # Get volunteer shift count for this program
    shift_q = (
        Q.from_(_vs)
        .select(
            fn.Count("*").as_("shift_count"),
            LiteralValue("SUM(CAST(hours AS REAL))").as_("total_hours"),
        )
        .where(_vs.program_id == P())
        .where(_vs.status == "completed")
    )
    shift_stats = conn.execute(shift_q.get_sql(), (program_id,)).fetchone()

    program_data = dict(row)
    program_data["volunteer_shifts"] = shift_stats["shift_count"] or 0
    program_data["volunteer_hours"] = str(_round(_dec(shift_stats["total_hours"]))) if shift_stats["total_hours"] else "0.00"
    return ok({"program": program_data})


def update_program_outcomes(conn, args):
    program_id = args.id
    if not program_id:
        return err("--id is required")

    q = Q.from_(_prog).select(_prog.id, _prog.company_id).where(_prog.id == P())
    row = conn.execute(q.get_sql(), (program_id,)).fetchone()
    if not row:
        return err(f"Program {program_id} not found")

    data = {}

    beneficiary_count = getattr(args, "beneficiary_count", None)
    if beneficiary_count is not None:
        data["beneficiary_count"] = int(beneficiary_count)

    outcome_metrics = getattr(args, "outcome_metrics", None)
    if outcome_metrics is not None:
        data["outcome_metrics"] = outcome_metrics

    if not data:
        return err("Provide --beneficiary-count and/or --outcome-metrics")

    data["updated_at"] = LiteralValue("datetime('now')")
    sql, params = dynamic_update("nonprofitclaw_program", data, where={"id": program_id})
    conn.execute(sql, params)
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
