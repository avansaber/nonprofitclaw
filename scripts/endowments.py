#!/usr/bin/env python3
"""NonprofitClaw endowments domain: 1 action.

Board-approved endowment appropriation (v1). Records a board-approved
appropriation of one explicit positive amount from one existing endowment
fund, posts the balanced GL movement, and reports the remaining corpus.
Version 1 offers no legal prudence conclusion and no pool investment
accounting.
"""
import json
import os
import sys
import uuid
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, timezone

import importlib.util
if importlib.util.find_spec("erpclaw_lib") is None:
    sys.path.insert(0, os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))
from erpclaw_lib.naming import get_next_name, register_prefix
from erpclaw_lib.response import ok, err
from erpclaw_lib.audit import audit
from erpclaw_lib.query import (
    Q, P, Table, Field, fn, Order, LiteralValue,
    insert_row, update_row, dynamic_update, now,
)

try:
    from erpclaw_lib.gl_posting import insert_gl_entries
    HAS_GL = True
except ImportError:
    HAS_GL = False

SKILL = "nonprofitclaw"
ACTION = "nonprofit-appropriate-endowment"
ENTITY = "nonprofitclaw_endowment_appropriation"

register_prefix(ENTITY, "NEA-")

# Table aliases
_fund = Table("nonprofitclaw_fund")
_account = Table("account")
_appropriation = Table(ENTITY)

# Stored fund types that count as an endowment corpus. The fund table keeps
# permanently restricted money as `permanently_restricted`; `endowment` is
# accepted too so a chart that names the type directly reads the same way.
ENDOWMENT_FUND_TYPES = ("permanently_restricted", "endowment")


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


def _replay_of(row):
    return {
        "id": row["id"],
        "appropriation_id": row["id"],
        "naming_series": row["naming_series"],
        "company_id": row["company_id"],
        "endowment_fund_id": row["endowment_fund_id"],
        "decision_date": row["decision_date"],
        "amount": row["amount"],
        "decision_reference": row["decision_reference"],
        "cash_account_id": row["cash_account_id"],
        "spendable_account_id": row["spendable_account_id"],
        "gl_entry_ids": json.loads(row["gl_entry_ids"]) if row["gl_entry_ids"] else [],
    }


def appropriate_endowment(conn, args):
    company_id = getattr(args, "company_id", None)
    if not company_id:
        return err("--company-id is required")

    endowment_fund_id = (
        getattr(args, "endowment_fund_id", None)
        or getattr(args, "fund_id", None)
        or getattr(args, "endowment_id", None)
        or getattr(args, "from_fund_id", None)
    )
    if not endowment_fund_id:
        return err("--endowment-fund-id is required")

    decision_date = (
        getattr(args, "decision_date", None)
        or getattr(args, "appropriation_date", None)
        or getattr(args, "transfer_date", None)
    )
    if not decision_date:
        return err("--decision-date is required")

    amount_str = getattr(args, "amount", None)
    if not amount_str:
        return err("--amount is required")
    try:
        amount = _round(_dec(amount_str))
    except Exception:
        return err("Amount must be finite and positive")
    if not amount.is_finite():
        return err("Amount must be finite and positive")
    if amount <= Decimal("0"):
        return err("Amount must be positive")
    amount_text = str(amount)

    decision_reference = (
        getattr(args, "decision_reference", None)
        or getattr(args, "reference", None)
        or getattr(args, "decision_ref", None)
    )
    if decision_reference is None or str(decision_reference).strip() == "":
        return err("--decision-reference is required; the board decision reference must be supplied, never inferred")
    decision_reference = str(decision_reference).strip()

    cash_account_id = (
        getattr(args, "cash_account_id", None)
        or getattr(args, "investment_account_id", None)
    )
    spendable_account_id = (
        getattr(args, "spendable_account_id", None)
        or getattr(args, "spendable_fund_account_id", None)
        or getattr(args, "equity_account_id", None)
    )
    missing = []
    if not cash_account_id:
        missing.append("--cash-account-id")
    if not spendable_account_id:
        missing.append("--spendable-account-id")
    if missing:
        return err("Appropriating from an endowment posts it to the ledger; missing: %s" % ", ".join(missing))
    if not HAS_GL:
        return err("GL posting is not available; endowment appropriation cannot be recorded")


    fq = Q.from_(_fund).select(
        _fund.id, _fund.company_id, _fund.name, _fund.fund_type,
        _fund.current_balance, _fund.is_active,
    ).where(_fund.id == P())
    fund = conn.execute(fq.get_sql(), (endowment_fund_id,)).fetchone()
    if not fund:
        return err("Endowment fund %s not found" % endowment_fund_id)
    if fund["company_id"] != company_id:
        return err("Endowment fund %s does not belong to this company" % endowment_fund_id)
    if not fund["is_active"]:
        return err("Endowment fund '%s' is not active" % fund["name"])
    if fund["fund_type"] not in ENDOWMENT_FUND_TYPES:
        return err(
            "Fund '%s' is %s; appropriation requires a permanently restricted "
            "(endowment) fund" % (fund["name"], fund["fund_type"])
        )

    iq = (
        Q.from_(_appropriation).select(_appropriation.star)
        .where(_appropriation.endowment_fund_id == P())
        .where(_appropriation.decision_reference == P())
    )
    existing = conn.execute(iq.get_sql(), (endowment_fund_id, decision_reference)).fetchone()
    if existing is not None:
        if (
            existing["company_id"] == company_id
            and existing["amount"] == amount_text
            and existing["decision_date"] == decision_date
            and existing["cash_account_id"] == cash_account_id
            and existing["spendable_account_id"] == spendable_account_id
        ):
            result = _replay_of(existing)
            bq = Q.from_(_fund).select(_fund.current_balance).where(_fund.id == P())
            result["remaining_balance"] = conn.execute(bq.get_sql(), (endowment_fund_id,)).fetchone()["current_balance"]
            return ok(result)
        return err(
            "Decision reference '%s' was already used for this endowment fund "
            "with different details; nothing was written" % decision_reference
        )

    aq = Q.from_(_account).select(_account.star).where(_account.id == P())
    cash_row = conn.execute(aq.get_sql(), (cash_account_id,)).fetchone()
    spendable_row = conn.execute(aq.get_sql(), (spendable_account_id,)).fetchone()
    for row, aid, expected, label in (
        (cash_row, cash_account_id, "asset", "Cash"),
        (spendable_row, spendable_account_id, "equity", "Spendable"),
    ):
        refusal = _account_refusal(row, aid, company_id, expected, label)
        if refusal:
            return err(refusal)
    if cash_account_id == spendable_account_id:
        return err("Cash and spendable accounts must be different")

    balance_text = fund["current_balance"] if fund["current_balance"] is not None else "0"
    if amount > _dec(balance_text):
        return err(
            "Appropriation amount (%s) exceeds the endowment balance (%s); "
            "Available: %s, Required: %s"
            % (amount_text, balance_text, balance_text, amount_text)
        )

    cost_center_id = getattr(args, "cost_center_id", None)

    try:
        appropriation_id = str(uuid.uuid4())
        naming = get_next_name(conn, ENTITY, company_id=company_id)
        created_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        sql, _ = insert_row(ENTITY, {
            "id": P(), "naming_series": P(), "company_id": P(),
            "endowment_fund_id": P(), "decision_date": P(), "amount": P(),
            "decision_reference": P(), "cash_account_id": P(),
            "spendable_account_id": P(), "created_at": P(),
        })
        conn.execute(sql, (
            appropriation_id, naming, company_id,
            endowment_fund_id, decision_date, amount_text,
            decision_reference, cash_account_id,
            spendable_account_id, created_at,
        ))

        # GL posting: debit spendable fund balance, credit cash or investment.
        gl_entries = [
            {
                "account_id": spendable_account_id,
                "debit": amount_text,
                "credit": "0",
                "cost_center_id": cost_center_id,
            },
            {
                "account_id": cash_account_id,
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
                voucher_id=appropriation_id,
                posting_date=decision_date,
                company_id=company_id,
                remarks="Endowment appropriation %s from fund %s" % (naming, endowment_fund_id),
            )
        except Exception as e:
            raise ValueError("GL posting failed for endowment appropriation %s: %s" % (naming, e))
        gl_entry_ids = json.dumps(ids)
        sql_gl, params_gl = dynamic_update(ENTITY,
            {"gl_entry_ids": gl_entry_ids}, where={"id": appropriation_id})
        conn.execute(sql_gl, params_gl)

        _shift_fund_balance(conn, endowment_fund_id, -amount)

        audit(conn, SKILL, ACTION, ENTITY, appropriation_id,
            new_values={"endowment_fund_id": endowment_fund_id,
                        "amount": amount_text, "decision_date": decision_date,
                        "decision_reference": decision_reference,
                        "cash_account_id": cash_account_id,
                        "spendable_account_id": spendable_account_id})
        conn.commit()
    except Exception as e:
        conn.rollback()
        return err("Appropriation failed: %s" % e)

    bq = Q.from_(_fund).select(_fund.current_balance).where(_fund.id == P())
    remaining = conn.execute(bq.get_sql(), (endowment_fund_id,)).fetchone()["current_balance"]
    return ok({
        "id": appropriation_id,
        "appropriation_id": appropriation_id,
        "naming_series": naming,
        "company_id": company_id,
        "endowment_fund_id": endowment_fund_id,
        "decision_date": decision_date,
        "amount": amount_text,
        "decision_reference": decision_reference,
        "remaining_balance": remaining,
        "gl_entry_ids": ids,
    })


ACTIONS = {
    "nonprofit-appropriate-endowment": appropriate_endowment,
}
