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

SKILL = "nonprofitclaw"


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

    conn.execute(
        """INSERT INTO nonprofitclaw_volunteer
           (id, naming_series, name, email, phone, skills, availability,
            start_date, is_active, company_id)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (
            volunteer_id, naming, name,
            getattr(args, "email", None),
            getattr(args, "phone", None),
            getattr(args, "skills", None),
            getattr(args, "availability", None),
            getattr(args, "start_date", None) or str(date.today()),
            1, company_id,
        ),
    )
    conn.commit()
    audit(conn, SKILL, "nonprofit-add-volunteer", volunteer_id, company_id)
    return ok({"id": volunteer_id, "naming_series": naming, "name": name})


def update_volunteer(conn, args):
    volunteer_id = args.id
    if not volunteer_id:
        return err("--id is required")

    row = conn.execute("SELECT id, company_id FROM nonprofitclaw_volunteer WHERE id=?", (volunteer_id,)).fetchone()
    if not row:
        return err(f"Volunteer {volunteer_id} not found")

    fields = []
    values = []
    for col, attr in [
        ("name", "name"), ("email", "email"), ("phone", "phone"),
        ("skills", "skills"), ("availability", "availability"),
        ("start_date", "start_date"),
    ]:
        val = getattr(args, attr, None)
        if val is not None:
            fields.append(f"{col}=?")
            values.append(val)

    is_active = getattr(args, "is_active", None)
    if is_active is not None:
        fields.append("is_active=?")
        values.append(int(is_active))

    if not fields:
        return err("No fields to update")

    fields.append("updated_at=datetime('now')")
    values.append(volunteer_id)
    conn.execute(f"UPDATE nonprofitclaw_volunteer SET {', '.join(fields)} WHERE id=?", values)
    conn.commit()
    audit(conn, SKILL, "nonprofit-update-volunteer", volunteer_id, row["company_id"])
    return ok({"id": volunteer_id, "updated": True})


def list_volunteers(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    limit = int(getattr(args, "limit", None) or 50)
    offset = int(getattr(args, "offset", None) or 0)

    where = ["company_id=?"]
    params = [company_id]

    is_active = getattr(args, "is_active", None)
    if is_active is not None:
        where.append("is_active=?")
        params.append(int(is_active))

    search = getattr(args, "search", None)
    if search:
        where.append("(name LIKE ? OR email LIKE ? OR skills LIKE ?)")
        params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])

    where_sql = " AND ".join(where)

    total = conn.execute(
        f"SELECT COUNT(*) FROM nonprofitclaw_volunteer WHERE {where_sql}", params
    ).fetchone()[0]

    rows = conn.execute(
        f"""SELECT id, naming_series, name, email, phone, skills,
                   availability, total_hours, shift_count, is_active,
                   start_date
            FROM nonprofitclaw_volunteer WHERE {where_sql}
            ORDER BY created_at DESC LIMIT ? OFFSET ?""",
        params + [limit, offset],
    ).fetchall()

    volunteers = [dict(r) for r in rows]
    return ok({"volunteers": volunteers, "total": total})


def get_volunteer(conn, args):
    volunteer_id = args.id
    if not volunteer_id:
        return err("--id is required")

    row = conn.execute("SELECT * FROM nonprofitclaw_volunteer WHERE id=?", (volunteer_id,)).fetchone()
    if not row:
        return err(f"Volunteer {volunteer_id} not found")

    # Get recent shifts
    shifts = conn.execute(
        """SELECT id, naming_series, shift_date, hours, description,
                  program_id, status
           FROM nonprofitclaw_volunteer_shift
           WHERE volunteer_id=?
           ORDER BY shift_date DESC LIMIT 10""",
        (volunteer_id,),
    ).fetchall()

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

    volunteer = conn.execute(
        "SELECT id, company_id FROM nonprofitclaw_volunteer WHERE id=?", (volunteer_id,)
    ).fetchone()
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
        program = conn.execute("SELECT id FROM nonprofitclaw_program WHERE id=?", (program_id,)).fetchone()
        if not program:
            return err(f"Program {program_id} not found")

    shift_id = str(uuid.uuid4())
    naming = get_next_name(conn, "nonprofitclaw_volunteer_shift", company_id=company_id)

    conn.execute(
        """INSERT INTO nonprofitclaw_volunteer_shift
           (id, naming_series, volunteer_id, program_id, shift_date,
            hours, description, status, company_id)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (
            shift_id, naming, volunteer_id, program_id,
            getattr(args, "shift_date", None) or str(date.today()),
            str(hours),
            getattr(args, "description", None),
            "scheduled", company_id,
        ),
    )
    conn.commit()
    audit(conn, SKILL, "nonprofit-add-volunteer-shift", shift_id, company_id)
    return ok({"id": shift_id, "naming_series": naming, "hours": str(hours)})


def list_volunteer_shifts(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    limit = int(getattr(args, "limit", None) or 50)
    offset = int(getattr(args, "offset", None) or 0)

    where = ["vs.company_id=?"]
    params = [company_id]

    volunteer_id = getattr(args, "volunteer_id", None)
    if volunteer_id:
        where.append("vs.volunteer_id=?")
        params.append(volunteer_id)

    program_id = getattr(args, "program_id", None)
    if program_id:
        where.append("vs.program_id=?")
        params.append(program_id)

    status = getattr(args, "status", None)
    if status:
        where.append("vs.status=?")
        params.append(status)

    from_date = getattr(args, "from_date", None)
    if from_date:
        where.append("vs.shift_date >= ?")
        params.append(from_date)

    to_date = getattr(args, "to_date", None)
    if to_date:
        where.append("vs.shift_date <= ?")
        params.append(to_date)

    where_sql = " AND ".join(where)

    total = conn.execute(
        f"SELECT COUNT(*) FROM nonprofitclaw_volunteer_shift vs WHERE {where_sql}", params
    ).fetchone()[0]

    rows = conn.execute(
        f"""SELECT vs.id, vs.naming_series, vs.volunteer_id, v.name as volunteer_name,
                   vs.program_id, p.name as program_name,
                   vs.shift_date, vs.hours, vs.description, vs.status
            FROM nonprofitclaw_volunteer_shift vs
            LEFT JOIN nonprofitclaw_volunteer v ON vs.volunteer_id = v.id
            LEFT JOIN nonprofitclaw_program p ON vs.program_id = p.id
            WHERE {where_sql}
            ORDER BY vs.shift_date DESC LIMIT ? OFFSET ?""",
        params + [limit, offset],
    ).fetchall()

    shifts = [dict(r) for r in rows]
    return ok({"volunteer_shifts": shifts, "total": total})


def complete_volunteer_shift(conn, args):
    shift_id = args.id
    if not shift_id:
        return err("--id is required")

    row = conn.execute(
        "SELECT * FROM nonprofitclaw_volunteer_shift WHERE id=?", (shift_id,)
    ).fetchone()
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
        conn.execute(
            "UPDATE nonprofitclaw_volunteer_shift SET status='completed', hours=? WHERE id=?",
            (str(hours), shift_id),
        )

        # Recalculate volunteer totals from completed shifts
        stats = conn.execute(
            """SELECT COUNT(*) as cnt,
                      SUM(CAST(hours AS REAL)) as total
               FROM nonprofitclaw_volunteer_shift
               WHERE volunteer_id=? AND status='completed'""",
            (volunteer_id,),
        ).fetchone()

        new_total = str(_round(_dec(stats["total"]))) if stats["total"] else "0"
        new_count = stats["cnt"] or 0

        conn.execute(
            """UPDATE nonprofitclaw_volunteer SET
                total_hours=?, shift_count=?,
                updated_at=datetime('now')
               WHERE id=?""",
            (new_total, new_count, volunteer_id),
        )

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

    # Per-volunteer summary
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
