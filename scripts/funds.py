#!/usr/bin/env python3
"""NonprofitClaw funds domain — 8 actions."""
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
# Fund CRUD
# ------------------------------------------------------------------

def add_fund(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    name = args.name
    if not name:
        return err("--name is required")

    fund_id = str(uuid.uuid4())
    naming = get_next_name(conn, "fund", company_id=company_id)
    fund_type = getattr(args, "fund_type", None) or "unrestricted"

    target_amount = getattr(args, "target_amount", None)
    if target_amount:
        target_amount = str(_round(_dec(target_amount)))

    conn.execute(
        """INSERT INTO nonprofitclaw_fund
           (id, naming_series, name, fund_type, description,
            target_amount, start_date, end_date, is_active, company_id)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (
            fund_id, naming, name, fund_type,
            getattr(args, "description", None),
            target_amount,
            getattr(args, "start_date", None),
            getattr(args, "end_date", None),
            1, company_id,
        ),
    )
    conn.commit()
    audit(conn, SKILL, "nonprofit-add-fund", fund_id, company_id)
    return ok({"id": fund_id, "naming_series": naming, "name": name})


def update_fund(conn, args):
    fund_id = args.id
    if not fund_id:
        return err("--id is required")

    row = conn.execute("SELECT id, company_id FROM nonprofitclaw_fund WHERE id=?", (fund_id,)).fetchone()
    if not row:
        return err(f"Fund {fund_id} not found")

    fields = []
    values = []
    for col, attr in [
        ("name", "name"), ("fund_type", "fund_type"),
        ("description", "description"),
        ("start_date", "start_date"), ("end_date", "end_date"),
    ]:
        val = getattr(args, attr, None)
        if val is not None:
            fields.append(f"{col}=?")
            values.append(val)

    target_amount = getattr(args, "target_amount", None)
    if target_amount is not None:
        fields.append("target_amount=?")
        values.append(str(_round(_dec(target_amount))))

    is_active = getattr(args, "is_active", None)
    if is_active is not None:
        fields.append("is_active=?")
        values.append(int(is_active))

    if not fields:
        return err("No fields to update")

    fields.append("updated_at=datetime('now')")
    values.append(fund_id)
    conn.execute(f"UPDATE nonprofitclaw_fund SET {', '.join(fields)} WHERE id=?", values)
    conn.commit()
    audit(conn, SKILL, "nonprofit-update-fund", fund_id, row["company_id"])
    return ok({"id": fund_id, "updated": True})


def list_funds(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    limit = int(getattr(args, "limit", None) or 50)
    offset = int(getattr(args, "offset", None) or 0)

    where = ["company_id=?"]
    params = [company_id]

    fund_type = getattr(args, "fund_type", None)
    if fund_type:
        where.append("fund_type=?")
        params.append(fund_type)

    is_active = getattr(args, "is_active", None)
    if is_active is not None:
        where.append("is_active=?")
        params.append(int(is_active))

    search = getattr(args, "search", None)
    if search:
        where.append("name LIKE ?")
        params.append(f"%{search}%")

    where_sql = " AND ".join(where)

    total = conn.execute(
        f"SELECT COUNT(*) FROM nonprofitclaw_fund WHERE {where_sql}", params
    ).fetchone()[0]

    rows = conn.execute(
        f"""SELECT id, naming_series, name, fund_type, description,
                   target_amount, current_balance, is_active,
                   start_date, end_date
            FROM nonprofitclaw_fund WHERE {where_sql}
            ORDER BY created_at DESC LIMIT ? OFFSET ?""",
        params + [limit, offset],
    ).fetchall()

    funds = [dict(r) for r in rows]
    return ok({"funds": funds, "total": total})


def get_fund(conn, args):
    fund_id = args.id
    if not fund_id:
        return err("--id is required")

    row = conn.execute("SELECT * FROM nonprofitclaw_fund WHERE id=?", (fund_id,)).fetchone()
    if not row:
        return err(f"Fund {fund_id} not found")

    return ok({"fund": dict(row)})


# ------------------------------------------------------------------
# Fund Transfers
# ------------------------------------------------------------------

def add_fund_transfer(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    from_fund_id = getattr(args, "from_fund_id", None)
    to_fund_id = getattr(args, "to_fund_id", None)
    if not from_fund_id or not to_fund_id:
        return err("--from-fund-id and --to-fund-id are required")
    if from_fund_id == to_fund_id:
        return err("Source and destination fund must be different")

    from_fund = conn.execute("SELECT id, company_id, current_balance FROM nonprofitclaw_fund WHERE id=?", (from_fund_id,)).fetchone()
    to_fund = conn.execute("SELECT id, company_id FROM nonprofitclaw_fund WHERE id=?", (to_fund_id,)).fetchone()
    if not from_fund:
        return err(f"Source fund {from_fund_id} not found")
    if not to_fund:
        return err(f"Destination fund {to_fund_id} not found")
    if from_fund["company_id"] != company_id or to_fund["company_id"] != company_id:
        return err("Both funds must belong to the specified company")

    amount_str = getattr(args, "amount", None)
    if not amount_str:
        return err("--amount is required")
    amount = _round(_dec(amount_str))
    if amount <= Decimal("0"):
        return err("Amount must be positive")

    transfer_id = str(uuid.uuid4())
    naming = get_next_name(conn, "fund_transfer", company_id=company_id)

    conn.execute(
        """INSERT INTO nonprofitclaw_fund_transfer
           (id, naming_series, from_fund_id, to_fund_id, amount,
            transfer_date, reason, status, company_id)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (
            transfer_id, naming, from_fund_id, to_fund_id,
            str(amount),
            getattr(args, "transfer_date", None) or str(__import__("datetime").date.today()),
            getattr(args, "reason", None),
            "draft", company_id,
        ),
    )
    conn.commit()
    audit(conn, SKILL, "nonprofit-add-fund-transfer", transfer_id, company_id)
    return ok({"id": transfer_id, "naming_series": naming, "amount": str(amount)})


def list_fund_transfers(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    limit = int(getattr(args, "limit", None) or 50)
    offset = int(getattr(args, "offset", None) or 0)

    where = ["ft.company_id=?"]
    params = [company_id]

    status = getattr(args, "status", None)
    if status:
        where.append("ft.status=?")
        params.append(status)

    fund_id = getattr(args, "fund_id", None)
    if fund_id:
        where.append("(ft.from_fund_id=? OR ft.to_fund_id=?)")
        params.extend([fund_id, fund_id])

    where_sql = " AND ".join(where)

    total = conn.execute(
        f"SELECT COUNT(*) FROM nonprofitclaw_fund_transfer ft WHERE {where_sql}", params
    ).fetchone()[0]

    rows = conn.execute(
        f"""SELECT ft.id, ft.naming_series, ft.from_fund_id, ff.name as from_fund_name,
                   ft.to_fund_id, tf.name as to_fund_name,
                   ft.amount, ft.transfer_date, ft.reason, ft.approved_by, ft.status
            FROM nonprofitclaw_fund_transfer ft
            LEFT JOIN nonprofitclaw_fund ff ON ft.from_fund_id = ff.id
            LEFT JOIN nonprofitclaw_fund tf ON ft.to_fund_id = tf.id
            WHERE {where_sql}
            ORDER BY ft.created_at DESC LIMIT ? OFFSET ?""",
        params + [limit, offset],
    ).fetchall()

    transfers = [dict(r) for r in rows]
    return ok({"fund_transfers": transfers, "total": total})


def approve_fund_transfer(conn, args):
    transfer_id = args.id
    if not transfer_id:
        return err("--id is required")

    row = conn.execute("SELECT * FROM nonprofitclaw_fund_transfer WHERE id=?", (transfer_id,)).fetchone()
    if not row:
        return err(f"Fund transfer {transfer_id} not found")
    if row["status"] != "draft":
        return err(f"Transfer is in '{row['status']}' status, can only approve 'draft' transfers")

    amount = _dec(row["amount"])
    from_fund_id = row["from_fund_id"]
    to_fund_id = row["to_fund_id"]

    # Check source fund has sufficient balance
    from_fund = conn.execute("SELECT current_balance FROM nonprofitclaw_fund WHERE id=?", (from_fund_id,)).fetchone()
    if _dec(from_fund["current_balance"]) < amount:
        return err(f"Insufficient balance in source fund. Available: {from_fund['current_balance']}, Required: {str(amount)}")

    approved_by = getattr(args, "approved_by", None)

    # Transaction (implicit)
    try:
        # Debit source fund
        conn.execute(
            """UPDATE nonprofitclaw_fund SET
                current_balance = CAST(
                    CAST(current_balance AS REAL) - ? AS TEXT
                ), updated_at=datetime('now')
               WHERE id=?""",
            (float(amount), from_fund_id),
        )

        # Credit destination fund
        conn.execute(
            """UPDATE nonprofitclaw_fund SET
                current_balance = CAST(
                    CAST(current_balance AS REAL) + ? AS TEXT
                ), updated_at=datetime('now')
               WHERE id=?""",
            (float(amount), to_fund_id),
        )

        # Mark transfer as completed
        conn.execute(
            "UPDATE nonprofitclaw_fund_transfer SET status='completed', approved_by=? WHERE id=?",
            (approved_by, transfer_id),
        )

        conn.commit()
    except Exception as e:
        conn.rollback()
        return err(f"Approval failed: {e}")

    audit(conn, SKILL, "nonprofit-approve-fund-transfer", transfer_id, row["company_id"])
    return ok({"id": transfer_id, "approved": True, "amount": str(amount)})


def fund_balance_report(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    rows = conn.execute(
        """SELECT id, naming_series, name, fund_type,
                  target_amount, current_balance, is_active
           FROM nonprofitclaw_fund WHERE company_id=? AND is_active=1
           ORDER BY name""",
        (company_id,),
    ).fetchall()

    funds = []
    total_balance = Decimal("0")
    for r in rows:
        bal = _dec(r["current_balance"])
        total_balance += bal
        fund = dict(r)
        if r["target_amount"]:
            target = _dec(r["target_amount"])
            if target > Decimal("0"):
                fund["percent_of_target"] = str(_round(bal / target * Decimal("100")))
        funds.append(fund)

    return ok({
        "funds": funds,
        "total_balance": str(_round(total_balance)),
        "fund_count": len(funds),
    })


ACTIONS = {
    "nonprofit-add-fund": add_fund,
    "nonprofit-update-fund": update_fund,
    "nonprofit-list-funds": list_funds,
    "nonprofit-get-fund": get_fund,
    "nonprofit-add-fund-transfer": add_fund_transfer,
    "nonprofit-list-fund-transfers": list_fund_transfers,
    "nonprofit-approve-fund-transfer": approve_fund_transfer,
    "nonprofit-fund-balance-report": fund_balance_report,
}
