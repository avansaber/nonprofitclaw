#!/usr/bin/env python3
"""NonprofitClaw Form 990 preparation worksheet (v1): 1 action.

A deterministic, read-only local preparation worksheet built from recorded
company books for one company fiscal year. It files no return, transmits no
data, chooses no tax position, and provides no legal or tax advice. Review
and filing remain outside ERPClaw.

Mapping (v1, documented so the numbers are reproducible):
  - contributions come from nonprofitclaw_donation rows in the fiscal-year
    bounds whose status is neither refunded nor cancelled.
  - grants revenue comes from nonprofitclaw_grant_receipt rows in bounds
    with status received. The grant table itself carries cumulative award
    figures, not period attribution, so it feeds counts and details only.
  - program service revenue has no dedicated local source and is reported
    as 0.00 with a deterministic warning.
  - revenue is contributions + grants + program service revenue.
  - program expenses come from approved nonprofitclaw_grant_expense rows in
    bounds with a program-side category (program, personnel, travel,
    equipment, supplies); administrative expenses from approved rows with
    category overhead or other. Every approved grant expense lands in
    exactly one of those two buckets.
  - fundraising expenses have no dedicated local source and are reported
    as 0.00 with a deterministic warning.
  - expenses is program + administrative + fundraising expenses.
  - ending net assets is the sum of current_balance over the company's
    fund rows.
  - GL income/expense postings in bounds are read company-scoped through
    the account table and reported as memo totals only, so recorded
    nonprofit transactions are never double-counted.
  - programs feed counts and details; budgets are planning figures, never
    expenses.

All money is TEXT in storage and Python Decimal in memory, summed in
Python (never binary float, never float SQL arithmetic). Every query is
built with PyPika and bound parameters. The action performs no writes:
no inserts, updates, deletes, naming-series bumps, or audit rows.
"""
import os
import sys
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

import importlib.util
if importlib.util.find_spec("erpclaw_lib") is None:
    sys.path.insert(0, os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))
from erpclaw_lib.response import ok, err
from erpclaw_lib.dependencies import table_exists
from erpclaw_lib.query import Q, P, Table, Order

SKILL = "nonprofitclaw"
ACTION = "nonprofit-prepare-form-990"

# ── Table aliases ──
_fy = Table("fiscal_year")
_acct = Table("account")
_gl = Table("gl_entry")
_don = Table("nonprofitclaw_donation")
_grant = Table("nonprofitclaw_grant")
_gr = Table("nonprofitclaw_grant_receipt")
_ge = Table("nonprofitclaw_grant_expense")
_prog = Table("nonprofitclaw_program")
_fund = Table("nonprofitclaw_fund")

PROGRAM_CATEGORIES = ("program", "personnel", "travel", "equipment", "supplies")
ADMIN_CATEGORIES = ("overhead", "other")

NOTICE = (
    "Preparation worksheet only. This deterministic summary of recorded "
    "company books does not file a return, transmit data, choose a tax "
    "position, or provide legal or tax advice. "
    "Review and filing remain outside ERPClaw."
)

STANDING_WARNINGS = (
    "program_service_revenue cannot be mapped from recorded local data "
    "and is reported as 0.00; review required",
    "fundraising_expenses cannot be mapped from recorded local data "
    "and are reported as 0.00; review required",
    "program budgets are planning figures and are not reported as expenses; "
    "expenses come from approved grant expenses",
    "officer compensation, governance, and other compliance lines are out "
    "of scope for v1; review required",
)


class _BadMoney(Exception):
    """A stored money TEXT value that is not a parseable Decimal."""


def _money(value, label):
    if value is None:
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, ArithmeticError):
        raise _BadMoney(
            "Malformed stored money in %s: %r; nothing was written" % (label, value)
        )


def _round(val):
    return val.quantize(Decimal("0.01"), ROUND_HALF_UP)


def _str(val):
    return str(_round(val))


def prepare_form_990(conn, args):
    company_id = getattr(args, "company_id", None)
    if not company_id:
        return err("--company-id is required")
    fiscal_year_id = getattr(args, "fiscal_year_id", None)
    if not fiscal_year_id:
        return err("--fiscal-year-id is required")
    try:
        return _worksheet(conn, company_id, fiscal_year_id)
    except _BadMoney as exc:
        return err(str(exc))


def _worksheet(conn, company_id, fiscal_year_id):
    if not table_exists(conn, "fiscal_year"):
        return err("Required table 'fiscal_year' not found")

    fq = (
        Q.from_(_fy)
        .select(_fy.id, _fy.name, _fy.start_date, _fy.end_date, _fy.company_id)
        .where(_fy.id == P())
    )
    fy = conn.execute(fq.get_sql(), (fiscal_year_id,)).fetchone()
    if not fy:
        return err("Fiscal year %s not found" % fiscal_year_id)
    if fy["company_id"] != company_id:
        return err(
            "Fiscal year %s does not belong to this company" % fiscal_year_id
        )
    start_date = fy["start_date"]
    end_date = fy["end_date"]

    warnings = list(STANDING_WARNINGS)
    sources = {"fiscal_year": True}
    counts = {}
    details = {}

    # ── Contributions from donations ──
    contributions = Decimal("0")
    donation_rows = []
    if table_exists(conn, "nonprofitclaw_donation"):
        sources["nonprofitclaw_donation"] = True
        dq = (
            Q.from_(_don)
            .select(
                _don.id, _don.donation_date, _don.amount,
                _don.donor_id, _don.fund_id, _don.status,
            )
            .where(_don.company_id == P())
            .where(_don.donation_date >= P())
            .where(_don.donation_date <= P())
            .where(_don.status != P())
            .where(_don.status != P())
            .orderby(_don.donation_date)
            .orderby(_don.id)
        )
        for row in conn.execute(
            dq.get_sql(),
            (company_id, start_date, end_date, "refunded", "cancelled"),
        ).fetchall():
            amount = _money(row["amount"], "nonprofitclaw_donation.amount")
            contributions += amount
            donation_rows.append({
                "id": row["id"],
                "donation_date": row["donation_date"],
                "amount": _str(amount),
                "donor_id": row["donor_id"],
                "fund_id": row["fund_id"],
                "status": row["status"],
            })
    else:
        sources["nonprofitclaw_donation"] = False
        warnings.append(
            "source table 'nonprofitclaw_donation' is not present; "
            "contributions are reported as 0.00"
        )
    counts["donations"] = len(donation_rows)
    details["donations"] = donation_rows

    # ── Grants revenue from grant receipts ──
    grants_total = Decimal("0")
    receipt_rows = []
    if table_exists(conn, "nonprofitclaw_grant_receipt"):
        sources["nonprofitclaw_grant_receipt"] = True
        rq = (
            Q.from_(_gr)
            .select(
                _gr.id, _gr.receipt_date, _gr.amount,
                _gr.grant_id, _gr.status,
            )
            .where(_gr.company_id == P())
            .where(_gr.receipt_date >= P())
            .where(_gr.receipt_date <= P())
            .where(_gr.status == P())
            .orderby(_gr.receipt_date)
            .orderby(_gr.id)
        )
        for row in conn.execute(
            rq.get_sql(), (company_id, start_date, end_date, "received")
        ).fetchall():
            amount = _money(row["amount"], "nonprofitclaw_grant_receipt.amount")
            grants_total += amount
            receipt_rows.append({
                "id": row["id"],
                "receipt_date": row["receipt_date"],
                "amount": _str(amount),
                "grant_id": row["grant_id"],
                "status": row["status"],
            })
    else:
        sources["nonprofitclaw_grant_receipt"] = False
        warnings.append(
            "source table 'nonprofitclaw_grant_receipt' is not present; "
            "grants are reported as 0.00"
        )
    counts["grant_receipts"] = len(receipt_rows)
    details["grant_receipts"] = receipt_rows

    # ── Expenses from approved grant expenses ──
    program_expenses = Decimal("0")
    admin_expenses = Decimal("0")
    expense_rows = []
    if table_exists(conn, "nonprofitclaw_grant_expense"):
        sources["nonprofitclaw_grant_expense"] = True
        eq = (
            Q.from_(_ge)
            .select(
                _ge.id, _ge.expense_date, _ge.amount,
                _ge.category, _ge.grant_id, _ge.status,
            )
            .where(_ge.company_id == P())
            .where(_ge.expense_date >= P())
            .where(_ge.expense_date <= P())
            .where(_ge.status == P())
            .orderby(_ge.expense_date)
            .orderby(_ge.id)
        )
        for row in conn.execute(
            eq.get_sql(), (company_id, start_date, end_date, "approved")
        ).fetchall():
            amount = _money(row["amount"], "nonprofitclaw_grant_expense.amount")
            category = row["category"]
            if category in PROGRAM_CATEGORIES:
                program_expenses += amount
            else:
                admin_expenses += amount
            expense_rows.append({
                "id": row["id"],
                "expense_date": row["expense_date"],
                "amount": _str(amount),
                "category": category,
                "grant_id": row["grant_id"],
                "status": row["status"],
            })
    else:
        sources["nonprofitclaw_grant_expense"] = False
        warnings.append(
            "source table 'nonprofitclaw_grant_expense' is not present; "
            "program and administrative expenses are reported as 0.00"
        )
    counts["grant_expenses"] = len(expense_rows)
    details["grant_expenses"] = expense_rows

    fundraising_expenses = Decimal("0")
    program_service_revenue = Decimal("0")

    # ── Grants table: counts and details (cumulative awards, not period revenue) ──
    grant_rows = []
    if table_exists(conn, "nonprofitclaw_grant"):
        sources["nonprofitclaw_grant"] = True
        gq = (
            Q.from_(_grant)
            .select(
                _grant.id, _grant.name, _grant.grantor_name,
                _grant.amount, _grant.status,
            )
            .where(_grant.company_id == P())
            .orderby(_grant.name)
            .orderby(_grant.id)
        )
        for row in conn.execute(gq.get_sql(), (company_id,)).fetchall():
            amount = _money(row["amount"], "nonprofitclaw_grant.amount")
            grant_rows.append({
                "id": row["id"],
                "name": row["name"],
                "grantor_name": row["grantor_name"],
                "amount": _str(amount),
                "status": row["status"],
            })
    else:
        sources["nonprofitclaw_grant"] = False
        warnings.append(
            "source table 'nonprofitclaw_grant' is not present; "
            "grant details are empty"
        )
    counts["grants"] = len(grant_rows)
    details["grants"] = grant_rows

    # ── Programs: counts and details (budgets are planning figures) ──
    program_rows = []
    if table_exists(conn, "nonprofitclaw_program"):
        sources["nonprofitclaw_program"] = True
        pq = (
            Q.from_(_prog)
            .select(_prog.id, _prog.name, _prog.budget, _prog.spent)
            .where(_prog.company_id == P())
            .orderby(_prog.name)
            .orderby(_prog.id)
        )
        for row in conn.execute(pq.get_sql(), (company_id,)).fetchall():
            budget = _money(row["budget"], "nonprofitclaw_program.budget")
            spent = _money(row["spent"], "nonprofitclaw_program.spent")
            program_rows.append({
                "id": row["id"],
                "name": row["name"],
                "budget": _str(budget),
                "spent": _str(spent),
            })
    else:
        sources["nonprofitclaw_program"] = False
        warnings.append(
            "source table 'nonprofitclaw_program' is not present; "
            "program details are empty"
        )
    counts["programs"] = len(program_rows)
    details["programs"] = program_rows

    # ── Ending net assets from fund balances ──
    ending_net_assets = Decimal("0")
    fund_rows = []
    if table_exists(conn, "nonprofitclaw_fund"):
        sources["nonprofitclaw_fund"] = True
        fq2 = (
            Q.from_(_fund)
            .select(
                _fund.id, _fund.name, _fund.fund_type, _fund.current_balance
            )
            .where(_fund.company_id == P())
            .orderby(_fund.name)
            .orderby(_fund.id)
        )
        for row in conn.execute(fq2.get_sql(), (company_id,)).fetchall():
            balance = _money(row["current_balance"], "nonprofitclaw_fund.current_balance")
            ending_net_assets += balance
            fund_rows.append({
                "id": row["id"],
                "name": row["name"],
                "fund_type": row["fund_type"],
                "current_balance": _str(balance),
            })
    else:
        sources["nonprofitclaw_fund"] = False
        warnings.append(
            "source table 'nonprofitclaw_fund' is not present; "
            "ending net assets are reported as 0.00"
        )
    counts["funds"] = len(fund_rows)
    details["funds"] = fund_rows

    # ── GL memo totals (read-only; never double-counted into revenue) ──
    gl_income = Decimal("0")
    gl_expense = Decimal("0")
    gl_count = 0
    if table_exists(conn, "account") and table_exists(conn, "gl_entry"):
        sources["account"] = True
        sources["gl_entry"] = True
        gq2 = (
            Q.from_(_gl)
            .join(_acct).on(_gl.account_id == _acct.id)
            .select(_gl.debit, _gl.credit, _acct.root_type)
            .where(_acct.company_id == P())
            .where(_gl.posting_date >= P())
            .where(_gl.posting_date <= P())
            .where(_gl.is_cancelled == P())
            .where((_acct.root_type == P()) | (_acct.root_type == P()))
        )
        for row in conn.execute(
            gq2.get_sql(),
            (company_id, start_date, end_date, 0, "income", "expense"),
        ).fetchall():
            debit = _money(row["debit"], "gl_entry.debit")
            credit = _money(row["credit"], "gl_entry.credit")
            if row["root_type"] == "income":
                gl_income += credit - debit
            else:
                gl_expense += debit - credit
            gl_count += 1
    else:
        if not table_exists(conn, "account"):
            sources["account"] = False
            warnings.append(
                "source table 'account' is not present; "
                "GL memo totals are reported as 0.00"
            )
        else:
            sources["account"] = True
        if not table_exists(conn, "gl_entry"):
            sources["gl_entry"] = False
            warnings.append(
                "source table 'gl_entry' is not present; "
                "GL memo totals are reported as 0.00"
            )
        else:
            sources["gl_entry"] = True
    counts["gl_entries"] = gl_count

    revenue = contributions + grants_total + program_service_revenue
    expenses = program_expenses + admin_expenses + fundraising_expenses

    return ok({
        "report": "preparation_worksheet",
        "form": "form_990",
        "worksheet_version": "v1",
        "company_id": company_id,
        "fiscal_year_id": fiscal_year_id,
        "fiscal_year_name": fy["name"],
        "period_start": start_date,
        "period_end": end_date,
        "totals": {
            "revenue": _str(revenue),
            "contributions": _str(contributions),
            "grants": _str(grants_total),
            "program_service_revenue": _str(program_service_revenue),
            "expenses": _str(expenses),
            "program_expenses": _str(program_expenses),
            "administrative_expenses": _str(admin_expenses),
            "fundraising_expenses": _str(fundraising_expenses),
            "ending_net_assets": _str(ending_net_assets),
            "gl_income_total": _str(gl_income),
            "gl_expense_total": _str(gl_expense),
        },
        "counts": counts,
        "sources": sources,
        "warnings": sorted(set(warnings)),
        "details": details,
        "notice": NOTICE,
        "read_only": True,
    })


ACTIONS = {
    "nonprofit-prepare-form-990": prepare_form_990,
}
