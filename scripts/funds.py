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
from erpclaw_lib.query import (
    Q, P, Table, Field, fn, Order, LiteralValue,
    insert_row, update_row, dynamic_update,
)

SKILL = "nonprofitclaw"

# ── Table aliases ──
_fund = Table("nonprofitclaw_fund")
_ft = Table("nonprofitclaw_fund_transfer")
_ff = Table("nonprofitclaw_fund")  # aliased as source in transfer queries


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
    naming = get_next_name(conn, "nonprofitclaw_fund", company_id=company_id)
    fund_type = getattr(args, "fund_type", None) or "unrestricted"

    target_amount = getattr(args, "target_amount", None)
    if target_amount:
        target_amount = str(_round(_dec(target_amount)))

    sql, _ = insert_row("nonprofitclaw_fund", {
        "id": P(), "naming_series": P(), "name": P(), "fund_type": P(),
        "description": P(), "target_amount": P(), "start_date": P(),
        "end_date": P(), "is_active": P(), "company_id": P(),
    })
    conn.execute(sql, (
        fund_id, naming, name, fund_type,
        getattr(args, "description", None),
        target_amount,
        getattr(args, "start_date", None),
        getattr(args, "end_date", None),
        1, company_id,
    ))
    conn.commit()
    audit(conn, SKILL, "nonprofit-add-fund", fund_id, company_id)
    return ok({"id": fund_id, "naming_series": naming, "name": name})


def update_fund(conn, args):
    fund_id = args.id
    if not fund_id:
        return err("--id is required")

    q = Q.from_(_fund).select(_fund.id, _fund.company_id).where(_fund.id == P())
    row = conn.execute(q.get_sql(), (fund_id,)).fetchone()
    if not row:
        return err(f"Fund {fund_id} not found")

    data = {}
    for col, attr in [
        ("name", "name"), ("fund_type", "fund_type"),
        ("description", "description"),
        ("start_date", "start_date"), ("end_date", "end_date"),
    ]:
        val = getattr(args, attr, None)
        if val is not None:
            data[col] = val

    target_amount = getattr(args, "target_amount", None)
    if target_amount is not None:
        data["target_amount"] = str(_round(_dec(target_amount)))

    is_active = getattr(args, "is_active", None)
    if is_active is not None:
        data["is_active"] = int(is_active)

    if not data:
        return err("No fields to update")

    data["updated_at"] = now()
    sql, params = dynamic_update("nonprofitclaw_fund", data, where={"id": fund_id})
    conn.execute(sql, params)
    conn.commit()
    audit(conn, SKILL, "nonprofit-update-fund", fund_id, row["company_id"])
    return ok({"id": fund_id, "updated": True})


def list_funds(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    limit = int(getattr(args, "limit", None) or 50)
    offset = int(getattr(args, "offset", None) or 0)

    conditions = [_fund.company_id == P()]
    params = [company_id]

    fund_type = getattr(args, "fund_type", None)
    if fund_type:
        conditions.append(_fund.fund_type == P())
        params.append(fund_type)

    is_active = getattr(args, "is_active", None)
    if is_active is not None:
        conditions.append(_fund.is_active == P())
        params.append(int(is_active))

    search = getattr(args, "search", None)
    if search:
        conditions.append(_fund.name.like(P()))
        params.append(f"%{search}%")

    count_q = Q.from_(_fund).select(fn.Count("*"))
    for cond in conditions:
        count_q = count_q.where(cond)
    total = conn.execute(count_q.get_sql(), params).fetchone()[0]

    data_q = Q.from_(_fund).select(
        _fund.id, _fund.naming_series, _fund.name, _fund.fund_type,
        _fund.description, _fund.target_amount, _fund.current_balance,
        _fund.is_active, _fund.start_date, _fund.end_date,
    )
    for cond in conditions:
        data_q = data_q.where(cond)
    data_q = data_q.orderby(_fund.created_at, order=Order.desc).limit(P()).offset(P())

    rows = conn.execute(data_q.get_sql(), params + [limit, offset]).fetchall()
    funds = [dict(r) for r in rows]
    return ok({"funds": funds, "total": total})


def get_fund(conn, args):
    fund_id = args.id
    if not fund_id:
        return err("--id is required")

    q = Q.from_(_fund).select(_fund.star).where(_fund.id == P())
    row = conn.execute(q.get_sql(), (fund_id,)).fetchone()
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

    fq = Q.from_(_fund).select(_fund.id, _fund.company_id, _fund.current_balance).where(_fund.id == P())
    from_fund = conn.execute(fq.get_sql(), (from_fund_id,)).fetchone()
    tq = Q.from_(_fund).select(_fund.id, _fund.company_id).where(_fund.id == P())
    to_fund = conn.execute(tq.get_sql(), (to_fund_id,)).fetchone()
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
    naming = get_next_name(conn, "nonprofitclaw_fund_transfer", company_id=company_id)

    sql, _ = insert_row("nonprofitclaw_fund_transfer", {
        "id": P(), "naming_series": P(), "from_fund_id": P(), "to_fund_id": P(),
        "amount": P(), "transfer_date": P(), "reason": P(), "status": P(), "company_id": P(),
    })
    conn.execute(sql, (
        transfer_id, naming, from_fund_id, to_fund_id,
        str(amount),
        getattr(args, "transfer_date", None) or str(__import__("datetime").date.today()),
        getattr(args, "reason", None),
        "draft", company_id,
    ))
    conn.commit()
    audit(conn, SKILL, "nonprofit-add-fund-transfer", transfer_id, company_id)
    return ok({"id": transfer_id, "naming_series": naming, "amount": str(amount)})


def list_fund_transfers(conn, args):
    company_id = args.company_id
    if not company_id:
        return err("--company-id is required")

    limit = int(getattr(args, "limit", None) or 50)
    offset = int(getattr(args, "offset", None) or 0)

    ft = _ft
    ff = Table("nonprofitclaw_fund").as_("ff")
    tf = Table("nonprofitclaw_fund").as_("tf")

    conditions = [ft.company_id == P()]
    params = [company_id]

    status = getattr(args, "status", None)
    if status:
        conditions.append(ft.status == P())
        params.append(status)

    fund_id = getattr(args, "fund_id", None)
    if fund_id:
        conditions.append((ft.from_fund_id == P()) | (ft.to_fund_id == P()))
        params.extend([fund_id, fund_id])

    count_q = Q.from_(ft).select(fn.Count("*"))
    for cond in conditions:
        count_q = count_q.where(cond)
    total = conn.execute(count_q.get_sql(), params).fetchone()[0]

    # Data query with JOINs — use raw aliases for the two fund joins
    data_q = (
        Q.from_(ft)
        .left_join(ff).on(ft.from_fund_id == ff.id)
        .left_join(tf).on(ft.to_fund_id == tf.id)
        .select(
            ft.id, ft.naming_series, ft.from_fund_id,
            ff.name.as_("from_fund_name"),
            ft.to_fund_id, tf.name.as_("to_fund_name"),
            ft.amount, ft.transfer_date, ft.reason, ft.approved_by, ft.status,
        )
    )
    for cond in conditions:
        data_q = data_q.where(cond)
    data_q = data_q.orderby(ft.created_at, order=Order.desc).limit(P()).offset(P())

    rows = conn.execute(data_q.get_sql(), params + [limit, offset]).fetchall()
    transfers = [dict(r) for r in rows]
    return ok({"fund_transfers": transfers, "total": total})


def approve_fund_transfer(conn, args):
    transfer_id = args.id
    if not transfer_id:
        return err("--id is required")

    q = Q.from_(_ft).select(_ft.star).where(_ft.id == P())
    row = conn.execute(q.get_sql(), (transfer_id,)).fetchone()
    if not row:
        return err(f"Fund transfer {transfer_id} not found")
    if row["status"] != "draft":
        return err(f"Transfer is in '{row['status']}' status, can only approve 'draft' transfers")

    amount = _dec(row["amount"])
    from_fund_id = row["from_fund_id"]
    to_fund_id = row["to_fund_id"]

    # Check source fund has sufficient balance
    bq = Q.from_(_fund).select(_fund.current_balance).where(_fund.id == P())
    from_fund = conn.execute(bq.get_sql(), (from_fund_id,)).fetchone()
    if _dec(from_fund["current_balance"]) < amount:
        return err(f"Insufficient balance in source fund. Available: {from_fund['current_balance']}, Required: {str(amount)}")

    approved_by = getattr(args, "approved_by", None)

    # Transaction (implicit)
    try:
        # Debit source fund
        ft = Table("nonprofitclaw_fund")
        debit_upd = (
            Q.update(ft)
            .set(ft.current_balance, LiteralValue("CAST(CAST(current_balance AS NUMERIC) - ? AS TEXT)"))
            .set(ft.updated_at, now())
            .where(ft.id == P())
        )
        conn.execute(debit_upd.get_sql(), (float(amount), from_fund_id))

        # Credit destination fund
        credit_upd = (
            Q.update(ft)
            .set(ft.current_balance, LiteralValue("CAST(CAST(current_balance AS NUMERIC) + ? AS TEXT)"))
            .set(ft.updated_at, now())
            .where(ft.id == P())
        )
        conn.execute(credit_upd.get_sql(), (float(amount), to_fund_id))

        # Mark transfer as completed
        sql_c, params_c = dynamic_update("nonprofitclaw_fund_transfer",
            {"status": "completed", "approved_by": approved_by},
            where={"id": transfer_id})
        conn.execute(sql_c, params_c)

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

    q = (
        Q.from_(_fund)
        .select(
            _fund.id, _fund.naming_series, _fund.name, _fund.fund_type,
            _fund.target_amount, _fund.current_balance, _fund.is_active,
        )
        .where(_fund.company_id == P())
        .where(_fund.is_active == 1)
        .orderby(_fund.name)
    )
    rows = conn.execute(q.get_sql(), (company_id,)).fetchall()

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
