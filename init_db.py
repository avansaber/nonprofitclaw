#!/usr/bin/env python3
"""NonprofitClaw schema — non-profit management tables.

13 tables: donors, donations, funds and fund transfers, grants, grant receipts and grant
expenses, programs, volunteers and shifts, pledges, campaigns, tax receipts.

Prerequisite: ERPClaw init_db.py must have run first (creates foundation tables).
Run: python3 init_db.py [db_path]

ADR-0034 phase 2 bulk-39. Schema declared as metadata and provisioned through
`erpclaw_lib.seam`, which emits dialect-correct DDL, replacing a hand-written
``CREATE TABLE`` block opened with ``sqlite3.connect`` that could not run on
PostgreSQL at all. Conversion rules are the pilot's (`erpclaw-esign`): seam
vocabulary only, and every amount a non-profit answers for — donation amounts and
in-kind fair values, grant awarded/received/spent/remaining, grant expenses,
pledge and fulfilled amounts, campaign goals and raised totals, program budgets,
volunteer hours, tax-receipt amounts, and above all a fund's `current_balance`,
which is what "temporarily restricted" and "permanently restricted" actually mean
to an auditor — stays TEXT.
"""
import importlib.util
import os
import sys

# Bootstrap the shared lib only when it is not already reachable — an
# unconditional insert at position 0 overrides a caller that deliberately bound a
# different tree (ADR-0034 phase 2 step 2d).
if importlib.util.find_spec("erpclaw_lib") is None:
    sys.path.insert(0, os.path.join(os.path.expanduser(
        os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))

from erpclaw_lib.seam import (  # noqa: E402
    CheckConstraint, Column, ForeignKey, Index, Integer, MetaData, Table, Text,
    provision, reference_table, text,
)

DEFAULT_DB_PATH = os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "data.sqlite")

METADATA = MetaData()

# Foundation tables this module points at but does not own. Declared for foreign
# key resolution only and never created here — see `seam.reference_table`.
reference_table("company", METADATA)
reference_table("customer", METADATA)

# ---------------------------------------------------------------------------
# 1. nonprofitclaw_donor_ext
#
# Extension table — core donor data lives in customer(id). Fields removed (live
# in core customer): name, email, phone, address, city, state, zip_code, tax_id.
#
# Its two foreign keys carry NO ``ON DELETE`` while every other table in this
# module spells ``ON DELETE RESTRICT``. That asymmetry shipped; it is transcribed
# rather than tidied.
# ---------------------------------------------------------------------------
DONOR_EXT = Table(
    "nonprofitclaw_donor_ext", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text, server_default=text("'NDNR-'")),
    Column("customer_id", Text, ForeignKey("customer.id"), nullable=False),
    Column("donor_type", Text, server_default=text("'individual'")),
    Column("donor_level", Text, server_default=text("'standard'")),
    Column("first_donation_date", Text),
    Column("last_donation_date", Text),
    Column("total_donated", Text, server_default=text("'0'")),
    Column("donation_count", Integer, server_default=text("0")),
    Column("notes", Text),
    Column("is_active", Integer, server_default=text("1")),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "donor_type IN ('individual','corporate','foundation','government',"
        "'anonymous')",
        name="ck_nonprofitclaw_donor_ext_donor_type"),
    CheckConstraint(
        "donor_level IN ('standard','bronze','silver','gold','platinum','major')",
        name="ck_nonprofitclaw_donor_ext_donor_level"),
)

Index("idx_nonprofitclaw_donor_ext_company", DONOR_EXT.c.company_id)
Index("idx_nonprofitclaw_donor_ext_customer", DONOR_EXT.c.customer_id)
Index("idx_nonprofitclaw_donor_ext_type", DONOR_EXT.c.donor_type)
Index("idx_nonprofitclaw_donor_ext_level", DONOR_EXT.c.donor_level)

# ---------------------------------------------------------------------------
# 2. nonprofitclaw_donation
# ---------------------------------------------------------------------------
DONATION = Table(
    "nonprofitclaw_donation", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("donor_id", Text,
           ForeignKey("nonprofitclaw_donor_ext.id", ondelete="RESTRICT"),
           nullable=False),
    Column("fund_id", Text,
           ForeignKey("nonprofitclaw_fund.id", ondelete="RESTRICT")),
    Column("campaign_id", Text,
           ForeignKey("nonprofitclaw_campaign.id", ondelete="RESTRICT")),
    Column("donation_date", Text, nullable=False,
           server_default=text("CURRENT_DATE")),
    Column("amount", Text, nullable=False, server_default=text("'0'")),
    Column("payment_method", Text, nullable=False,
           server_default=text("'check'")),
    Column("reference", Text),
    Column("is_recurring", Integer, nullable=False, server_default=text("0")),
    Column("recurrence_freq", Text),
    Column("in_kind_description", Text),
    Column("in_kind_fair_value", Text),
    Column("tax_deductible", Integer, nullable=False, server_default=text("1")),
    Column("receipt_sent", Integer, nullable=False, server_default=text("0")),
    Column("gl_entry_ids", Text),
    Column("notes", Text),
    Column("status", Text, nullable=False, server_default=text("'received'")),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "payment_method IN ('cash','check','credit_card','bank_transfer',"
        "'online','in_kind','stock','crypto','other')",
        name="ck_nonprofitclaw_donation_payment_method"),
    CheckConstraint("is_recurring IN (0,1)",
                    name="ck_nonprofitclaw_donation_is_recurring"),
    # NULL is a member of the shipped IN list. It makes the predicate unknown
    # rather than false for a NULL value, which SQLite treats as satisfied — the
    # same thing the constraint's absence would do for that row, and exactly what
    # shipped. Transcribed, not simplified.
    CheckConstraint("recurrence_freq IN ('monthly','quarterly','annually',NULL)",
                    name="ck_nonprofitclaw_donation_recurrence_freq"),
    CheckConstraint("tax_deductible IN (0,1)",
                    name="ck_nonprofitclaw_donation_tax_deductible"),
    CheckConstraint("receipt_sent IN (0,1)",
                    name="ck_nonprofitclaw_donation_receipt_sent"),
    CheckConstraint(
        "status IN ('pledged','received','deposited','refunded','cancelled')",
        name="ck_nonprofitclaw_donation_status"),
)

Index("idx_nonprofitclaw_donation_donor", DONATION.c.donor_id)
Index("idx_nonprofitclaw_donation_fund", DONATION.c.fund_id)
Index("idx_nonprofitclaw_donation_date", DONATION.c.donation_date)
Index("idx_nonprofitclaw_donation_status", DONATION.c.status)

# ---------------------------------------------------------------------------
# 3. nonprofitclaw_fund
# ---------------------------------------------------------------------------
FUND = Table(
    "nonprofitclaw_fund", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("name", Text, nullable=False),
    Column("fund_type", Text, nullable=False,
           server_default=text("'unrestricted'")),
    Column("description", Text),
    Column("target_amount", Text),
    Column("current_balance", Text, nullable=False, server_default=text("'0'")),
    Column("start_date", Text),
    Column("end_date", Text),
    Column("is_active", Integer, nullable=False, server_default=text("1")),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "fund_type IN ('unrestricted','temporarily_restricted',"
        "'permanently_restricted')",
        name="ck_nonprofitclaw_fund_fund_type"),
    CheckConstraint("is_active IN (0,1)",
                    name="ck_nonprofitclaw_fund_is_active"),
)

Index("idx_nonprofitclaw_fund_company", FUND.c.company_id)
Index("idx_nonprofitclaw_fund_type", FUND.c.fund_type)

# ---------------------------------------------------------------------------
# 4. nonprofitclaw_fund_transfer
# ---------------------------------------------------------------------------
FUND_TRANSFER = Table(
    "nonprofitclaw_fund_transfer", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("from_fund_id", Text,
           ForeignKey("nonprofitclaw_fund.id", ondelete="RESTRICT"),
           nullable=False),
    Column("to_fund_id", Text,
           ForeignKey("nonprofitclaw_fund.id", ondelete="RESTRICT"),
           nullable=False),
    Column("amount", Text, nullable=False, server_default=text("'0'")),
    Column("transfer_date", Text, nullable=False,
           server_default=text("CURRENT_DATE")),
    Column("reason", Text),
    Column("approved_by", Text),
    Column("status", Text, nullable=False, server_default=text("'draft'")),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "status IN ('draft','approved','completed','cancelled')",
        name="ck_nonprofitclaw_fund_transfer_status"),
)

Index("idx_nonprofitclaw_ft_company", FUND_TRANSFER.c.company_id)

# ---------------------------------------------------------------------------
# 5. nonprofitclaw_grant
# ---------------------------------------------------------------------------
GRANT = Table(
    "nonprofitclaw_grant", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("name", Text, nullable=False),
    Column("grantor_name", Text, nullable=False),
    Column("grantor_type", Text, nullable=False,
           server_default=text("'foundation'")),
    Column("grant_type", Text, nullable=False, server_default=text("'project'")),
    Column("amount", Text, nullable=False, server_default=text("'0'")),
    Column("received_amount", Text, nullable=False, server_default=text("'0'")),
    Column("spent_amount", Text, nullable=False, server_default=text("'0'")),
    Column("remaining_amount", Text, nullable=False, server_default=text("'0'")),
    Column("fund_id", Text,
           ForeignKey("nonprofitclaw_fund.id", ondelete="RESTRICT")),
    Column("start_date", Text),
    Column("end_date", Text),
    Column("reporting_freq", Text, server_default=text("'quarterly'")),
    Column("next_report_due", Text),
    Column("status", Text, nullable=False, server_default=text("'applied'")),
    Column("notes", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    # How the grant is funded; advance is the default and what every grant created before this column reads.
    Column("funding_basis", Text, CheckConstraint("funding_basis IN ('advance','reimbursement')", name="ck_nonprofitclaw_grant_funding_basis"), nullable=False, server_default=text("'advance'")),
    CheckConstraint(
        "grantor_type IN ('foundation','government','corporate','individual',"
        "'other')",
        name="ck_nonprofitclaw_grant_grantor_type"),
    CheckConstraint(
        "grant_type IN ('project','operating','capital','capacity_building',"
        "'other')",
        name="ck_nonprofitclaw_grant_grant_type"),
    CheckConstraint(
        "reporting_freq IN ('monthly','quarterly','semi_annual','annual',"
        "'final_only')",
        name="ck_nonprofitclaw_grant_reporting_freq"),
    CheckConstraint(
        "status IN ('applied','awarded','active','completed','closed',"
        "'rejected')",
        name="ck_nonprofitclaw_grant_status"),
)

Index("idx_nonprofitclaw_grant_company", GRANT.c.company_id)
Index("idx_nonprofitclaw_grant_status", GRANT.c.status)

# ---------------------------------------------------------------------------
# 6. nonprofitclaw_grant_expense
# ---------------------------------------------------------------------------
GRANT_EXPENSE = Table(
    "nonprofitclaw_grant_expense", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("grant_id", Text,
           ForeignKey("nonprofitclaw_grant.id", ondelete="RESTRICT"),
           nullable=False),
    Column("expense_date", Text, nullable=False,
           server_default=text("CURRENT_DATE")),
    Column("amount", Text, nullable=False, server_default=text("'0'")),
    Column("category", Text, nullable=False, server_default=text("'program'")),
    Column("description", Text),
    Column("receipt_reference", Text),
    Column("gl_entry_ids", Text),
    Column("status", Text, nullable=False, server_default=text("'draft'")),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "category IN ('program','personnel','overhead','travel','equipment',"
        "'supplies','other')",
        name="ck_nonprofitclaw_grant_expense_category"),
    CheckConstraint(
        "status IN ('draft','submitted','approved','rejected')",
        name="ck_nonprofitclaw_grant_expense_status"),
)

Index("idx_nonprofitclaw_gexp_grant", GRANT_EXPENSE.c.grant_id)

# ---------------------------------------------------------------------------
# 6b. nonprofitclaw_grant_receipt
#
# One row per amount received on a grant; credit_account_id is the account the
# receipt credits (the record action's --revenue-account-id), the caller's choice
# (contribution revenue, a refundable-advance liability, or grants receivable);
# cancel reverses, never edits; reference is the grantor's payment reference.
# ---------------------------------------------------------------------------
GRANT_RECEIPT = Table(
    "nonprofitclaw_grant_receipt", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("grant_id", Text, ForeignKey("nonprofitclaw_grant.id", ondelete="RESTRICT"), nullable=False),
    Column("fund_id", Text, ForeignKey("nonprofitclaw_fund.id", ondelete="RESTRICT")),
    Column("receipt_date", Text, nullable=False),
    Column("amount", Text, nullable=False),
    Column("reference", Text),
    Column("cash_account_id", Text, nullable=False),
    Column("credit_account_id", Text, nullable=False),
    Column("cost_center_id", Text),
    Column("gl_entry_ids", Text),
    Column("status", Text, nullable=False, server_default=text("'received'")),
    Column("cancelled_at", Text),
    Column("company_id", Text, ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("status IN ('received','cancelled')", name="ck_nonprofitclaw_grant_receipt_status"),
)
Index("idx_nonprofitclaw_grant_receipt_grant", GRANT_RECEIPT.c.grant_id)
Index("idx_nonprofitclaw_grant_receipt_company", GRANT_RECEIPT.c.company_id)

# ---------------------------------------------------------------------------
# 7. nonprofitclaw_program
# ---------------------------------------------------------------------------
PROGRAM = Table(
    "nonprofitclaw_program", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("name", Text, nullable=False),
    Column("description", Text),
    Column("fund_id", Text,
           ForeignKey("nonprofitclaw_fund.id", ondelete="RESTRICT")),
    Column("budget", Text, nullable=False, server_default=text("'0'")),
    Column("spent", Text, nullable=False, server_default=text("'0'")),
    Column("beneficiary_count", Integer, nullable=False,
           server_default=text("0")),
    Column("start_date", Text),
    Column("end_date", Text),
    Column("outcome_metrics", Text),
    Column("is_active", Integer, nullable=False, server_default=text("1")),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("is_active IN (0,1)",
                    name="ck_nonprofitclaw_program_is_active"),
)

Index("idx_nonprofitclaw_program_company", PROGRAM.c.company_id)

# ---------------------------------------------------------------------------
# 8. nonprofitclaw_volunteer
# ---------------------------------------------------------------------------
VOLUNTEER = Table(
    "nonprofitclaw_volunteer", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("name", Text, nullable=False),
    Column("email", Text),
    Column("phone", Text),
    Column("skills", Text),
    Column("availability", Text),
    Column("total_hours", Text, nullable=False, server_default=text("'0'")),
    Column("shift_count", Integer, nullable=False, server_default=text("0")),
    Column("start_date", Text),
    Column("is_active", Integer, nullable=False, server_default=text("1")),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("is_active IN (0,1)",
                    name="ck_nonprofitclaw_volunteer_is_active"),
)

Index("idx_nonprofitclaw_vol_company", VOLUNTEER.c.company_id)

# ---------------------------------------------------------------------------
# 9. nonprofitclaw_volunteer_shift
# ---------------------------------------------------------------------------
VOLUNTEER_SHIFT = Table(
    "nonprofitclaw_volunteer_shift", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("volunteer_id", Text,
           ForeignKey("nonprofitclaw_volunteer.id", ondelete="RESTRICT"),
           nullable=False),
    Column("program_id", Text,
           ForeignKey("nonprofitclaw_program.id", ondelete="RESTRICT")),
    Column("shift_date", Text, nullable=False,
           server_default=text("CURRENT_DATE")),
    Column("hours", Text, nullable=False, server_default=text("'0'")),
    Column("description", Text),
    Column("status", Text, nullable=False, server_default=text("'scheduled'")),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "status IN ('scheduled','completed','cancelled','no_show')",
        name="ck_nonprofitclaw_volunteer_shift_status"),
)

Index("idx_nonprofitclaw_vshift_vol", VOLUNTEER_SHIFT.c.volunteer_id)
Index("idx_nonprofitclaw_vshift_date", VOLUNTEER_SHIFT.c.shift_date)

# ---------------------------------------------------------------------------
# 10. nonprofitclaw_pledge
# ---------------------------------------------------------------------------
PLEDGE = Table(
    "nonprofitclaw_pledge", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("donor_id", Text,
           ForeignKey("nonprofitclaw_donor_ext.id", ondelete="RESTRICT"),
           nullable=False),
    Column("campaign_id", Text,
           ForeignKey("nonprofitclaw_campaign.id", ondelete="RESTRICT")),
    Column("fund_id", Text,
           ForeignKey("nonprofitclaw_fund.id", ondelete="RESTRICT")),
    Column("pledge_date", Text, nullable=False,
           server_default=text("CURRENT_DATE")),
    Column("amount", Text, nullable=False, server_default=text("'0'")),
    Column("fulfilled_amount", Text, nullable=False, server_default=text("'0'")),
    Column("frequency", Text, nullable=False,
           server_default=text("'one_time'")),
    Column("next_due_date", Text),
    Column("end_date", Text),
    Column("notes", Text),
    Column("status", Text, nullable=False, server_default=text("'active'")),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "frequency IN ('one_time','monthly','quarterly','annually')",
        name="ck_nonprofitclaw_pledge_frequency"),
    CheckConstraint(
        "status IN ('active','fulfilled','partially_fulfilled','cancelled',"
        "'lapsed')",
        name="ck_nonprofitclaw_pledge_status"),
)

Index("idx_nonprofitclaw_pledge_donor", PLEDGE.c.donor_id)
Index("idx_nonprofitclaw_pledge_status", PLEDGE.c.status)

# ---------------------------------------------------------------------------
# 11. nonprofitclaw_campaign
# ---------------------------------------------------------------------------
CAMPAIGN = Table(
    "nonprofitclaw_campaign", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("name", Text, nullable=False),
    Column("description", Text),
    Column("fund_id", Text,
           ForeignKey("nonprofitclaw_fund.id", ondelete="RESTRICT")),
    Column("goal_amount", Text, nullable=False, server_default=text("'0'")),
    Column("raised_amount", Text, nullable=False, server_default=text("'0'")),
    Column("donor_count", Integer, nullable=False, server_default=text("0")),
    Column("start_date", Text),
    Column("end_date", Text),
    Column("status", Text, nullable=False, server_default=text("'draft'")),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "status IN ('draft','active','completed','cancelled')",
        name="ck_nonprofitclaw_campaign_status"),
)

Index("idx_nonprofitclaw_campaign_company", CAMPAIGN.c.company_id)
Index("idx_nonprofitclaw_campaign_status", CAMPAIGN.c.status)

# ---------------------------------------------------------------------------
# 12. nonprofitclaw_tax_receipt
# ---------------------------------------------------------------------------
TAX_RECEIPT = Table(
    "nonprofitclaw_tax_receipt", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("donor_id", Text,
           ForeignKey("nonprofitclaw_donor_ext.id", ondelete="RESTRICT"),
           nullable=False),
    Column("donation_id", Text,
           ForeignKey("nonprofitclaw_donation.id", ondelete="RESTRICT")),
    Column("receipt_date", Text, nullable=False,
           server_default=text("CURRENT_DATE")),
    Column("amount", Text, nullable=False, server_default=text("'0'")),
    Column("tax_year", Text, nullable=False),
    Column("receipt_type", Text, nullable=False, server_default=text("'single'")),
    Column("sent_date", Text),
    Column("sent_method", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "receipt_type IN ('single','annual_summary')",
        name="ck_nonprofitclaw_tax_receipt_receipt_type"),
    # NULL is a member of this shipped IN list too — see the note on
    # `nonprofitclaw_donation.recurrence_freq`.
    CheckConstraint("sent_method IN ('email','mail','both',NULL)",
                    name="ck_nonprofitclaw_tax_receipt_sent_method"),
)

Index("idx_nonprofitclaw_trec_donor", TAX_RECEIPT.c.donor_id)
Index("idx_nonprofitclaw_trec_year", TAX_RECEIPT.c.tax_year)


def _require_foundation(db_path):
    """The pre-conversion installer's foundation probe, asked through the seam.

    The original read ``sqlite_master`` directly, so the guard that exists to
    produce a friendly error was itself SQLite-only — on PostgreSQL it raised
    instead of printing. ``seam.table_exists`` answers on both backends
    (ADR-0034 bulk-39). The table it probes and the wording are this module's
    own, unchanged.
    """
    from erpclaw_lib import seam

    if not seam.table_exists("company", db_path):
        print("ERROR: Foundation tables not found. Run erpclaw-setup first.")
        sys.exit(1)


def create_nonprofitclaw_tables(db_path):
    """Create NonprofitClaw tables and indexes on whichever backend is configured.

    Same contract as before the ADR-0034 conversion: idempotent, and the counts
    it returns are what was ACTUALLY created rather than what was declared.
    """
    _require_foundation(db_path)
    result = provision(METADATA, db_path)
    print(f"NonprofitClaw tables created in {db_path}")
    return {
        "database": db_path,
        "tables": result["tables"],
        "indexes": result["indexes"],
    }


if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB_PATH
    parent = os.path.dirname(db_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    create_nonprofitclaw_tables(db_path)
