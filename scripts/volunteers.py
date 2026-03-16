#!/usr/bin/env python3
"""NonprofitClaw volunteers domain — 8 actions."""
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
_vol = Table("nonprofitclaw_volunteer")
_vs = Table("nonprofitclaw_volunteer_shift")
_prog = Table("nonprofitclaw_program")


def _dec(val):
    if val is None:
        return Decimal("0")
    return Decimal(str(val))


def _round(val):
    return val.quantize(Decimal("0.01"), ROUND_HALF_UP)


# ------------------------------------------------------------------
# Volunteer CRUD
# ------------------------------------------------------------------

def add_volunteer(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    name = args.name
    if not name:
        return err("--name is required")

    volunteer_id = str(uuid.uuid4())
    naming = get_next_name(conn, "nonprofitclaw_volunteer", company_id=company_id)

    sql, _ = insert_row("nonprofitclaw_volunteer", {
        "id": P(), "naming_series": P(), "name": P(), "email": P(),
        "phone": P(), "skills": P(), "availability": P(),
        "start_date": P(), "is_active": P(), "company_id": P(),
    })
    conn.execute(sql, (
        volunteer_id, naming, name,
        getattr(args, "email", None),
        getattr(args, "phone", None),
        getattr(args, "skills", None),
        getattr(args, "availability", None),
        getattr(args, "start_date", None) or str(date.today()),
        1, company_id,
    ))
    conn.commit()
    audit(conn, SKILL, "nonprofit-add-volunteer", volunteer_id, company_id)
    return ok({"id": volunteer_id, "naming_series": naming, "name": name})


def update_volunteer(conn, args):
    volunteer_id = args.id
    if not volunteer_id:
        return err("--id is required")

    q = Q.from_(_vol).select(_vol.id, _vol.company_id).where(_vol.id == P())
    row = conn.execute(q.get_sql(), (volunteer_id,)).fetchone()
    if not row:
        return err(f"Volunteer {volunteer_id} not found")

    data = {}
    for col, attr in [
        ("name", "name"), ("email", "email"), ("phone", "phone"),
        ("skills", "skills"), ("availability", "availability"),
        ("start_date", "start_date"),
    ]:
        val = getattr(args, attr, None)
        if val is not None:
            data[col] = val

    is_active = getattr(args, "is_active", None)
    if is_active is not None:
        data["is_active"] = int(is_active)

    if not data:
        return err("No fields to update")

    data["updated_at"] = LiteralValue("datetime('now')")
    sql, params = dynamic_update("nonprofitclaw_volunteer", data, where={"id": volunteer_id})
    conn.execute(sql, params)
    conn.commit()
    audit(conn, SKILL, "nonprofit-update-volunteer", volunteer_id, row["company_id"])
    return ok({"id": volunteer_id, "updated": True})


def list_volunteers(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    limit = int(getattr(args, "limit", None) or 50)
    offset = int(getattr(args, "offset", None) or 0)

    conditions = [_vol.company_id == P()]
    params = [company_id]

    is_active = getattr(args, "is_active", None)
    if is_active is not None:
        conditions.append(_vol.is_active == P())
        params.append(int(is_active))

    search = getattr(args, "search", None)
    if search:
        conditions.append(
            _vol.name.like(P()) | _vol.email.like(P()) | _vol.skills.like(P())
        )
        params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])

    count_q = Q.from_(_vol).select(fn.Count("*"))
    for cond in conditions:
        count_q = count_q.where(cond)
    total = conn.execute(count_q.get_sql(), params).fetchone()[0]

    data_q = Q.from_(_vol).select(
        _vol.id, _vol.naming_series, _vol.name, _vol.email, _vol.phone,
        _vol.skills, _vol.availability, _vol.total_hours, _vol.shift_count,
        _vol.is_active, _vol.start_date,
    )
    for cond in conditions:
        data_q = data_q.where(cond)
    data_q = data_q.orderby(_vol.created_at, order=Order.desc).limit(P()).offset(P())

    rows = conn.execute(data_q.get_sql(), params + [limit, offset]).fetchall()
    volunteers = [dict(r) for r in rows]
    return ok({"volunteers": volunteers, "total": total})


def get_volunteer(conn, args):
    volunteer_id = args.id
    if not volunteer_id:
        return err("--id is required")

    q = Q.from_(_vol).select(_vol.star).where(_vol.id == P())
    row = conn.execute(q.get_sql(), (volunteer_id,)).fetchone()
    if not row:
        return err(f"Volunteer {volunteer_id} not found")

    # Get recent shifts
    shift_q = (
        Q.from_(_vs)
        .select(
            _vs.id, _vs.naming_series, _vs.shift_date, _vs.hours,
            _vs.description, _vs.program_id, _vs.status,
        )
        .where(_vs.volunteer_id == P())
        .orderby(_vs.shift_date, order=Order.desc)
        .limit(10)
    )
    shifts = conn.execute(shift_q.get_sql(), (volunteer_id,)).fetchall()

    vol_data = dict(row)
    vol_data["recent_shifts"] = [dict(s) for s in shifts]
    return ok({"volunteer": vol_data})


# ------------------------------------------------------------------
# Volunteer Shifts
# ------------------------------------------------------------------

def add_volunteer_shift(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    volunteer_id = getattr(args, "volunteer_id", None)
    if not volunteer_id:
        return err("--volunteer-id is required")

    vq = Q.from_(_vol).select(_vol.id, _vol.company_id).where(_vol.id == P())
    volunteer = conn.execute(vq.get_sql(), (volunteer_id,)).fetchone()
    if not volunteer:
        return err(f"Volunteer {volunteer_id} not found")
    if volunteer["company_id"] != company_id:
        return err("Volunteer does not belong to this company")

    hours_str = getattr(args, "hours", None)
    if not hours_str:
        return err("--hours is required")
    hours = _round(_dec(hours_str))
    if hours <= Decimal("0"):
        return err("Hours must be positive")

    program_id = getattr(args, "program_id", None)
    if program_id:
        pq = Q.from_(_prog).select(_prog.id).where(_prog.id == P())
        if not conn.execute(pq.get_sql(), (program_id,)).fetchone():
            return err(f"Program {program_id} not found")

    shift_id = str(uuid.uuid4())
    naming = get_next_name(conn, "nonprofitclaw_volunteer_shift", company_id=company_id)

    sql, _ = insert_row("nonprofitclaw_volunteer_shift", {
        "id": P(), "naming_series": P(), "volunteer_id": P(), "program_id": P(),
        "shift_date": P(), "hours": P(), "description": P(), "status": P(),
        "company_id": P(),
    })
    conn.execute(sql, (
        shift_id, naming, volunteer_id, program_id,
        getattr(args, "shift_date", None) or str(date.today()),
        str(hours),
        getattr(args, "description", None),
        "scheduled", company_id,
    ))
    conn.commit()
    audit(conn, SKILL, "nonprofit-add-volunteer-shift", shift_id, company_id)
    return ok({"id": shift_id, "naming_series": naming, "hours": str(hours)})


def list_volunteer_shifts(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    limit = int(getattr(args, "limit", None) or 50)
    offset = int(getattr(args, "offset", None) or 0)

    vs = _vs
    v = _vol
    p = _prog

    conditions = [vs.company_id == P()]
    params = [company_id]

    volunteer_id = getattr(args, "volunteer_id", None)
    if volunteer_id:
        conditions.append(vs.volunteer_id == P())
        params.append(volunteer_id)

    program_id = getattr(args, "program_id", None)
    if program_id:
        conditions.append(vs.program_id == P())
        params.append(program_id)

    status = getattr(args, "status", None)
    if status:
        conditions.append(vs.status == P())
        params.append(status)

    from_date = getattr(args, "from_date", None)
    if from_date:
        conditions.append(vs.shift_date >= P())
        params.append(from_date)

    to_date = getattr(args, "to_date", None)
    if to_date:
        conditions.append(vs.shift_date <= P())
        params.append(to_date)

    count_q = Q.from_(vs).select(fn.Count("*"))
    for cond in conditions:
        count_q = count_q.where(cond)
    total = conn.execute(count_q.get_sql(), params).fetchone()[0]

    data_q = (
        Q.from_(vs)
        .left_join(v).on(vs.volunteer_id == v.id)
        .left_join(p).on(vs.program_id == p.id)
        .select(
            vs.id, vs.naming_series, vs.volunteer_id, v.name.as_("volunteer_name"),
            vs.program_id, p.name.as_("program_name"),
            vs.shift_date, vs.hours, vs.description, vs.status,
        )
    )
    for cond in conditions:
        data_q = data_q.where(cond)
    data_q = data_q.orderby(vs.shift_date, order=Order.desc).limit(P()).offset(P())

    rows = conn.execute(data_q.get_sql(), params + [limit, offset]).fetchall()
    shifts = [dict(r) for r in rows]
    return ok({"volunteer_shifts": shifts, "total": total})


def complete_volunteer_shift(conn, args):
    shift_id = args.id
    if not shift_id:
        return err("--id is required")

    q = Q.from_(_vs).select(_vs.star).where(_vs.id == P())
    row = conn.execute(q.get_sql(), (shift_id,)).fetchone()
    if not row:
        return err(f"Volunteer shift {shift_id} not found")
    if row["status"] == "completed":
        return err("Shift is already completed")
    if row["status"] == "cancelled":
        return err("Cannot complete a cancelled shift")

    # Allow updating hours at completion time
    hours_str = getattr(args, "hours", None)
    if hours_str:
        hours = _round(_dec(hours_str))
        if hours <= Decimal("0"):
            return err("Hours must be positive")
    else:
        hours = _dec(row["hours"])

    volunteer_id = row["volunteer_id"]

    # Transaction (implicit)
    try:
        sql_c, params_c = dynamic_update("nonprofitclaw_volunteer_shift",
            {"status": "completed", "hours": str(hours)},
            where={"id": shift_id})
        conn.execute(sql_c, params_c)

        # Recalculate volunteer totals from completed shifts
        stats_q = (
            Q.from_(_vs)
            .select(fn.Count("*").as_("cnt"), LiteralValue("SUM(CAST(hours AS REAL))").as_("total"))
            .where(_vs.volunteer_id == P())
            .where(_vs.status == "completed")
        )
        stats = conn.execute(stats_q.get_sql(), (volunteer_id,)).fetchone()

        new_total = str(_round(_dec(stats["total"]))) if stats["total"] else "0"
        new_count = stats["cnt"] or 0

        upd_vol = {
            "total_hours": new_total,
            "shift_count": new_count,
            "updated_at": LiteralValue("datetime('now')"),
        }
        sql_v, params_v = dynamic_update("nonprofitclaw_volunteer", upd_vol, where={"id": volunteer_id})
        conn.execute(sql_v, params_v)

        conn.commit()
    except Exception as e:
        conn.rollback()
        return err(f"Failed to complete shift: {e}")

    audit(conn, SKILL, "nonprofit-complete-volunteer-shift", shift_id, row["company_id"])
    return ok({
        "id": shift_id,
        "completed": True,
        "hours": str(hours),
        "volunteer_total_hours": new_total,
        "volunteer_shift_count": new_count,
    })


def volunteer_hours_report(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    from_date = getattr(args, "from_date", None)
    to_date = getattr(args, "to_date", None)

    # Build date filter for shifts
    date_where = ""
    date_params = []
    if from_date:
        date_where += " AND vs.shift_date >= ?"
        date_params.append(from_date)
    if to_date:
        date_where += " AND vs.shift_date <= ?"
        date_params.append(to_date)

    # Per-volunteer summary — complex query with conditional JOINs, keep as LiteralValue
    rows = conn.execute(
        f"""SELECT v.id, v.naming_series, v.name,
                   COUNT(vs.id) as shifts_completed,
                   SUM(CAST(vs.hours AS REAL)) as hours_worked
            FROM nonprofitclaw_volunteer v
            LEFT JOIN nonprofitclaw_volunteer_shift vs
                ON v.id = vs.volunteer_id AND vs.status='completed' {date_where}
            WHERE v.company_id=? AND v.is_active=1
            GROUP BY v.id, v.naming_series, v.name
            HAVING shifts_completed > 0
            ORDER BY hours_worked DESC""",
        date_params + [company_id],
    ).fetchall()

    volunteers = []
    total_hours = Decimal("0")
    total_shifts = 0
    for r in rows:
        vol = dict(r)
        vol["hours_worked"] = str(_round(_dec(r["hours_worked"]))) if r["hours_worked"] else "0.00"
        total_hours += _dec(r["hours_worked"])
        total_shifts += r["shifts_completed"] or 0
        volunteers.append(vol)

    # Per-program summary
    program_rows = conn.execute(
        f"""SELECT p.id, p.name as program_name,
                   COUNT(vs.id) as shifts_completed,
                   SUM(CAST(vs.hours AS REAL)) as hours_worked,
                   COUNT(DISTINCT vs.volunteer_id) as volunteer_count
            FROM nonprofitclaw_program p
            LEFT JOIN nonprofitclaw_volunteer_shift vs
                ON p.id = vs.program_id AND vs.status='completed' {date_where}
            WHERE p.company_id=? AND p.is_active=1
            GROUP BY p.id, p.name
            HAVING shifts_completed > 0
            ORDER BY hours_worked DESC""",
        date_params + [company_id],
    ).fetchall()

    programs = []
    for r in program_rows:
        prog = dict(r)
        prog["hours_worked"] = str(_round(_dec(r["hours_worked"]))) if r["hours_worked"] else "0.00"
        programs.append(prog)

    return ok({
        "volunteers": volunteers,
        "programs": programs,
        "total_hours": str(_round(total_hours)),
        "total_shifts": total_shifts,
        "active_volunteer_count": len(volunteers),
        "from_date": from_date,
        "to_date": to_date,
    })


ACTIONS = {
    "nonprofit-add-volunteer": add_volunteer,
    "nonprofit-update-volunteer": update_volunteer,
    "nonprofit-list-volunteers": list_volunteers,
    "nonprofit-get-volunteer": get_volunteer,
    "nonprofit-add-volunteer-shift": add_volunteer_shift,
    "nonprofit-list-volunteer-shifts": list_volunteer_shifts,
    "nonprofit-complete-volunteer-shift": complete_volunteer_shift,
    "nonprofit-volunteer-hours-report": volunteer_hours_report,
}
