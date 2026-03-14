#!/usr/bin/env python3
"""NonprofitClaw — Non-Profit Management unified router.

Routes all --action calls to the appropriate domain handler.
58 total actions across 7 domain modules.
"""
import argparse
import os
import sqlite3
import sys

sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from erpclaw_lib.response import err
from erpclaw_lib.args import SafeArgumentParser

from donors import ACTIONS as DONORS_ACTIONS
from funds import ACTIONS as FUNDS_ACTIONS
from grants import ACTIONS as GRANTS_ACTIONS
from programs import ACTIONS as PROGRAMS_ACTIONS
from volunteers import ACTIONS as VOLUNTEERS_ACTIONS
from campaigns import ACTIONS as CAMPAIGNS_ACTIONS
from compliance import ACTIONS as COMPLIANCE_ACTIONS

SKILL = "nonprofitclaw"
DB_PATH = os.path.expanduser("~/.openclaw/erpclaw/data.sqlite")
REQUIRED_TABLES = ["company", "customer", "nonprofitclaw_donor_ext"]

# Merge all domain action dicts
ACTIONS = {}
ACTIONS.update(DONORS_ACTIONS)
ACTIONS.update(FUNDS_ACTIONS)
ACTIONS.update(GRANTS_ACTIONS)
ACTIONS.update(PROGRAMS_ACTIONS)
ACTIONS.update(VOLUNTEERS_ACTIONS)
ACTIONS.update(CAMPAIGNS_ACTIONS)
ACTIONS.update(COMPLIANCE_ACTIONS)


def get_connection():
    if not os.path.exists(DB_PATH):
        err(f"Database not found at {DB_PATH}. Run erpclaw-setup initialize-database first.")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def check_tables(conn):
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    for t in REQUIRED_TABLES:
        if t not in tables:
            err(f"Required table '{t}' not found. Run nonprofitclaw init_db.py first.")


def build_parser():
    parser = SafeArgumentParser(description="NonprofitClaw Non-Profit Management")
    parser.add_argument("--action", required=True, choices=sorted(ACTIONS.keys()),
                        help="Action to perform")

    # --- Common identifiers ---
    parser.add_argument("--id", help="Primary entity ID")
    parser.add_argument("--company-id", dest="company_id", help="Company ID")
    parser.add_argument("--donor-id", dest="donor_id", help="Donor ID")
    parser.add_argument("--donation-id", dest="donation_id", help="Donation ID")
    parser.add_argument("--fund-id", dest="fund_id", help="Fund ID")
    parser.add_argument("--grant-id", dest="grant_id", help="Grant ID")
    parser.add_argument("--program-id", dest="program_id", help="Program ID")
    parser.add_argument("--volunteer-id", dest="volunteer_id", help="Volunteer ID")
    parser.add_argument("--campaign-id", dest="campaign_id", help="Campaign ID")
    parser.add_argument("--pledge-id", dest="pledge_id", help="Pledge ID")

    # --- Merge / transfer IDs ---
    parser.add_argument("--source-donor-id", dest="source_donor_id", help="Source donor ID for merge")
    parser.add_argument("--target-donor-id", dest="target_donor_id", help="Target donor ID for merge")
    parser.add_argument("--from-fund-id", dest="from_fund_id", help="Source fund ID for transfer")
    parser.add_argument("--to-fund-id", dest="to_fund_id", help="Destination fund ID for transfer")

    # --- Donor fields ---
    parser.add_argument("--name", help="Name")
    parser.add_argument("--email", help="Email address")
    parser.add_argument("--phone", help="Phone number")
    parser.add_argument("--address", help="Street address")
    parser.add_argument("--city", help="City")
    parser.add_argument("--state", help="State")
    parser.add_argument("--zip-code", dest="zip_code", help="ZIP code")
    parser.add_argument("--tax-id", dest="tax_id", help="Tax ID / EIN")
    parser.add_argument("--donor-type", dest="donor_type",
                        choices=["individual", "corporate", "foundation", "government", "anonymous"],
                        help="Donor type")
    parser.add_argument("--donor-level", dest="donor_level",
                        choices=["standard", "bronze", "silver", "gold", "platinum", "major"],
                        help="Donor level")
    parser.add_argument("--is-active", dest="is_active", help="Active flag (0 or 1)")

    # --- Donation fields ---
    parser.add_argument("--amount", help="Amount (monetary)")
    parser.add_argument("--payment-method", dest="payment_method",
                        choices=["cash", "check", "credit_card", "bank_transfer",
                                 "online", "in_kind", "stock", "crypto", "other"],
                        help="Payment method")
    parser.add_argument("--reference", help="Reference / check number")
    parser.add_argument("--donation-date", dest="donation_date", help="Donation date (YYYY-MM-DD)")
    parser.add_argument("--is-recurring", dest="is_recurring", help="Recurring flag (0 or 1)")
    parser.add_argument("--recurrence-freq", dest="recurrence_freq",
                        choices=["monthly", "quarterly", "annually"],
                        help="Recurrence frequency")

    # --- Fund fields ---
    parser.add_argument("--fund-type", dest="fund_type",
                        choices=["unrestricted", "temporarily_restricted", "permanently_restricted"],
                        help="Fund type")
    parser.add_argument("--description", help="Description")
    parser.add_argument("--target-amount", dest="target_amount", help="Target amount")
    parser.add_argument("--start-date", dest="start_date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", dest="end_date", help="End date (YYYY-MM-DD)")

    # --- Fund transfer fields ---
    parser.add_argument("--transfer-date", dest="transfer_date", help="Transfer date (YYYY-MM-DD)")
    parser.add_argument("--reason", help="Reason / justification")
    parser.add_argument("--approved-by", dest="approved_by", help="Approved by (name/ID)")

    # --- Grant fields ---
    parser.add_argument("--grantor-name", dest="grantor_name", help="Grantor name")
    parser.add_argument("--grantor-type", dest="grantor_type",
                        choices=["foundation", "government", "corporate", "individual", "other"],
                        help="Grantor type")
    parser.add_argument("--grant-type", dest="grant_type",
                        choices=["project", "operating", "capital", "capacity_building", "other"],
                        help="Grant type")
    parser.add_argument("--reporting-freq", dest="reporting_freq",
                        choices=["monthly", "quarterly", "semi_annual", "annual", "final_only"],
                        help="Reporting frequency")

    # --- Grant expense fields ---
    parser.add_argument("--expense-date", dest="expense_date", help="Expense date (YYYY-MM-DD)")
    parser.add_argument("--category", dest="category",
                        choices=["program", "personnel", "overhead", "travel",
                                 "equipment", "supplies", "other"],
                        help="Expense category")
    parser.add_argument("--receipt-reference", dest="receipt_reference", help="Receipt reference")

    # --- Program fields ---
    parser.add_argument("--budget", help="Program budget")
    parser.add_argument("--beneficiary-count", dest="beneficiary_count", help="Beneficiary count")
    parser.add_argument("--outcome-metrics", dest="outcome_metrics", help="Outcome metrics (JSON or text)")

    # --- Volunteer fields ---
    parser.add_argument("--skills", help="Skills (comma-separated)")
    parser.add_argument("--availability", help="Availability description")
    parser.add_argument("--shift-date", dest="shift_date", help="Shift date (YYYY-MM-DD)")
    parser.add_argument("--hours", help="Hours worked")

    # --- Campaign fields ---
    parser.add_argument("--goal-amount", dest="goal_amount", help="Campaign goal amount")

    # --- Pledge fields ---
    parser.add_argument("--pledge-date", dest="pledge_date", help="Pledge date (YYYY-MM-DD)")
    parser.add_argument("--frequency", dest="frequency",
                        choices=["one_time", "monthly", "quarterly", "annually"],
                        help="Pledge frequency")
    parser.add_argument("--next-due-date", dest="next_due_date", help="Next due date (YYYY-MM-DD)")

    # --- Tax receipt fields ---
    parser.add_argument("--tax-year", dest="tax_year", help="Tax year (YYYY)")
    parser.add_argument("--receipt-type", dest="receipt_type",
                        choices=["single", "annual_summary"],
                        help="Receipt type")
    parser.add_argument("--sent-method", dest="sent_method",
                        choices=["email", "mail", "both"],
                        help="Receipt delivery method")

    # --- GL posting fields (optional — graceful degradation) ---
    parser.add_argument("--cash-account-id", dest="cash_account_id",
                        help="Cash/Bank GL account ID for donation receipts or grant disbursements")
    parser.add_argument("--revenue-account-id", dest="revenue_account_id",
                        help="Contribution Revenue GL account ID for donation receipts")
    parser.add_argument("--expense-account-id", dest="expense_account_id",
                        help="Program Expense GL account ID for grant disbursements")
    parser.add_argument("--cost-center-id", dest="cost_center_id",
                        help="Cost center ID for GL postings")

    # --- Common filters ---
    parser.add_argument("--notes", help="Notes / comments")
    parser.add_argument("--search", help="Search term")
    parser.add_argument("--status", help="Status filter")
    parser.add_argument("--limit", default="50", help="Result limit (default 50)")
    parser.add_argument("--offset", default="0", help="Result offset (default 0)")
    parser.add_argument("--from-date", dest="from_date", help="From date filter (YYYY-MM-DD)")
    parser.add_argument("--to-date", dest="to_date", help="To date filter (YYYY-MM-DD)")

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    action = args.action
    handler = ACTIONS.get(action)
    if not handler:
        err(f"Unknown action: {action}")

    conn = get_connection()
    check_tables(conn)

    try:
        handler(conn, args)
    except SystemExit:
        raise
    except Exception as e:
        err(f"Action '{action}' failed: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
