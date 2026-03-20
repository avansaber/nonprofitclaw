"""Shared helper functions for NonprofitClaw unit tests.

Provides:
  - DB bootstrap via init_schema.init_db() + create_nonprofitclaw_tables()
  - call_action() / ns() / is_error() / is_ok()
  - Seed functions for company, customer, naming series, donor
  - build_env() for full test environment
  - load_db_query() for explicit module loading (avoids sys.path collisions)
"""
import argparse
import importlib.util
import io
import json
import os
import sqlite3
import sys
import uuid
from decimal import Decimal
from unittest.mock import patch

# ──────────────────────────────────────────────────────────────────────────────
# Paths
# ──────────────────────────────────────────────────────────────────────────────

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
MODULE_DIR = os.path.dirname(TESTS_DIR)                    # scripts/
ROOT_DIR = os.path.dirname(MODULE_DIR)                     # nonprofitclaw/
SRC_DIR = os.path.dirname(ROOT_DIR)                        # src/

# Foundation schema init
SETUP_DIR = os.path.join(SRC_DIR, "erpclaw", "scripts", "erpclaw-setup")
INIT_SCHEMA_PATH = os.path.join(SETUP_DIR, "init_schema.py")

# Vertical schema init
VERTICAL_INIT_PATH = os.path.join(ROOT_DIR, "init_db.py")

# Make erpclaw_lib importable
ERPCLAW_LIB = os.path.expanduser("~/.openclaw/erpclaw/lib")
if ERPCLAW_LIB not in sys.path:
    sys.path.insert(0, ERPCLAW_LIB)

from erpclaw_lib.db import setup_pragmas


def load_db_query():
    """Load nonprofitclaw's db_query.py explicitly to avoid sys.path collisions."""
    db_query_path = os.path.join(MODULE_DIR, "db_query.py")
    spec = importlib.util.spec_from_file_location("db_query_nonprofit", db_query_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    # Attach action functions as underscore-named attributes for convenience
    for action_name, fn in mod.ACTIONS.items():
        setattr(mod, action_name.replace("-", "_"), fn)
    return mod


# ──────────────────────────────────────────────────────────────────────────────
# DB helpers
# ──────────────────────────────────────────────────────────────────────────────

def init_all_tables(db_path: str):
    """Create all foundation + nonprofitclaw tables."""
    # Foundation tables (company, customer, naming_series, audit_log, etc.)
    spec = importlib.util.spec_from_file_location("init_schema", INIT_SCHEMA_PATH)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    m.init_db(db_path)

    # NonprofitClaw vertical tables
    spec2 = importlib.util.spec_from_file_location("nonprofit_init", VERTICAL_INIT_PATH)
    m2 = importlib.util.module_from_spec(spec2)
    spec2.loader.exec_module(m2)
    m2.create_nonprofitclaw_tables(db_path)


class _DecimalSum:
    """Custom SQLite aggregate: SUM using Python Decimal for precision."""
    def __init__(self):
        self.total = Decimal("0")
    def step(self, value):
        if value is not None:
            self.total += Decimal(str(value))
    def finalize(self):
        return str(self.total)


class _ConnWrapper:
    """Wraps a sqlite3.Connection to expose company_id for cross_skill compat."""
    def __init__(self, conn, company_id=None):
        self._conn = conn
        self.company_id = company_id

    def __getattr__(self, name):
        return getattr(self._conn, name)

    def execute(self, *a, **kw):
        return self._conn.execute(*a, **kw)

    def executemany(self, *a, **kw):
        return self._conn.executemany(*a, **kw)

    def executescript(self, *a, **kw):
        return self._conn.executescript(*a, **kw)

    def commit(self):
        return self._conn.commit()

    def rollback(self):
        return self._conn.rollback()

    def close(self):
        return self._conn.close()


def get_conn(db_path: str) -> sqlite3.Connection:
    """Return a sqlite3.Connection with FK enabled and Row factory."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    setup_pragmas(conn)
    conn.create_aggregate("decimal_sum", 1, _DecimalSum)
    return conn


# ──────────────────────────────────────────────────────────────────────────────
# Action invocation helpers
# ──────────────────────────────────────────────────────────────────────────────

def call_action(fn, conn, args) -> dict:
    """Invoke a domain function, capture stdout JSON, return parsed dict."""
    buf = io.StringIO()

    def _fake_exit(code=0):
        raise SystemExit(code)

    try:
        with patch("sys.stdout", buf), patch("sys.exit", side_effect=_fake_exit):
            fn(conn, args)
    except SystemExit:
        pass

    output = buf.getvalue().strip()
    if not output:
        return {"status": "error", "message": "no output captured"}
    return json.loads(output)


def ns(**kwargs) -> argparse.Namespace:
    """Build an argparse.Namespace from keyword args (mimics CLI flags)."""
    return argparse.Namespace(**kwargs)


def is_error(result: dict) -> bool:
    """Check if a call_action result is an error response."""
    return result.get("status") == "error"


def is_ok(result: dict) -> bool:
    """Check if a call_action result is a success response."""
    return result.get("status") == "ok"


# ──────────────────────────────────────────────────────────────────────────────
# Utility
# ──────────────────────────────────────────────────────────────────────────────

def _uuid() -> str:
    return str(uuid.uuid4())


# ──────────────────────────────────────────────────────────────────────────────
# Seed helpers
# ──────────────────────────────────────────────────────────────────────────────

def seed_company(conn, name="Test Nonprofit Org", abbr="TNO") -> str:
    """Insert a test company and return its ID."""
    cid = _uuid()
    conn.execute(
        """INSERT INTO company (id, name, abbr, default_currency, country,
           fiscal_year_start_month)
           VALUES (?, ?, ?, 'USD', 'United States', 1)""",
        (cid, f"{name} {cid[:6]}", f"{abbr}{cid[:4]}")
    )
    conn.commit()
    return cid


def seed_customer(conn, company_id: str, name="Test Donor Customer") -> str:
    """Insert a customer and return its ID."""
    cid = _uuid()
    conn.execute(
        """INSERT INTO customer (id, name, company_id, customer_type, status, credit_limit)
           VALUES (?, ?, ?, 'individual', 'active', '0')""",
        (cid, name, company_id)
    )
    conn.commit()
    return cid


def seed_donor(conn, company_id: str, name="Test Donor",
               donor_type="individual", donor_level="standard") -> dict:
    """Insert a customer + donor extension row. Returns dict with ids."""
    customer_id = seed_customer(conn, company_id, name)
    donor_id = _uuid()
    conn.execute(
        """INSERT INTO nonprofitclaw_donor_ext
           (id, naming_series, customer_id, donor_type, donor_level,
            is_active, company_id)
           VALUES (?, 'NDNR-0001', ?, ?, ?, 1, ?)""",
        (donor_id, customer_id, donor_type, donor_level, company_id)
    )
    conn.commit()
    return {"donor_id": donor_id, "customer_id": customer_id}


def seed_fund(conn, company_id: str, name="General Fund",
              fund_type="unrestricted", balance="0") -> str:
    """Insert a fund and return its ID."""
    fid = _uuid()
    conn.execute(
        """INSERT INTO nonprofitclaw_fund
           (id, naming_series, name, fund_type, current_balance, is_active, company_id)
           VALUES (?, 'FUND-0001', ?, ?, ?, 1, ?)""",
        (fid, name, fund_type, balance, company_id)
    )
    conn.commit()
    return fid


def seed_campaign(conn, company_id: str, name="Annual Giving",
                  goal_amount="10000", status="active",
                  fund_id=None) -> str:
    """Insert a campaign and return its ID."""
    cid = _uuid()
    conn.execute(
        """INSERT INTO nonprofitclaw_campaign
           (id, naming_series, name, fund_id, goal_amount, status, company_id)
           VALUES (?, 'CAMP-0001', ?, ?, ?, ?, ?)""",
        (cid, name, fund_id, goal_amount, status, company_id)
    )
    conn.commit()
    return cid


def seed_program(conn, company_id: str, name="Youth Program",
                 budget="5000", fund_id=None) -> str:
    """Insert a program and return its ID."""
    pid = _uuid()
    conn.execute(
        """INSERT INTO nonprofitclaw_program
           (id, naming_series, name, budget, is_active, company_id, fund_id)
           VALUES (?, 'PROG-0001', ?, ?, 1, ?, ?)""",
        (pid, name, budget, company_id, fund_id)
    )
    conn.commit()
    return pid


def seed_volunteer(conn, company_id: str, name="Jane Smith",
                   email="jane@example.com") -> str:
    """Insert a volunteer and return its ID."""
    vid = _uuid()
    conn.execute(
        """INSERT INTO nonprofitclaw_volunteer
           (id, naming_series, name, email, is_active, company_id)
           VALUES (?, 'VOL-0001', ?, ?, 1, ?)""",
        (vid, name, email, company_id)
    )
    conn.commit()
    return vid


def seed_grant(conn, company_id: str, name="Community Grant",
               grantor_name="Ford Foundation", amount="50000",
               status="applied", fund_id=None) -> str:
    """Insert a grant and return its ID."""
    gid = _uuid()
    conn.execute(
        """INSERT INTO nonprofitclaw_grant
           (id, naming_series, name, grantor_name, grantor_type, grant_type,
            amount, remaining_amount, status, company_id, fund_id)
           VALUES (?, 'GRT-0001', ?, ?, 'foundation', 'project', ?, ?, ?, ?, ?)""",
        (gid, name, grantor_name, amount, amount, status, company_id, fund_id)
    )
    conn.commit()
    return gid


def seed_donation(conn, company_id: str, donor_id: str, amount="100.00",
                  fund_id=None, campaign_id=None, status="received") -> str:
    """Insert a donation and return its ID."""
    did = _uuid()
    conn.execute(
        """INSERT INTO nonprofitclaw_donation
           (id, naming_series, donor_id, fund_id, campaign_id,
            donation_date, amount, payment_method, status, company_id)
           VALUES (?, 'DON-0001', ?, ?, ?, date('now'), ?, 'check', ?, ?)""",
        (did, donor_id, fund_id, campaign_id, amount, status, company_id)
    )
    conn.commit()
    return did


def seed_naming_series(conn, company_id: str):
    """Seed naming series for NonprofitClaw entity types."""
    series = [
        ("donor", "NDNR-", 0),
        ("donation", "DON-", 0),
        ("fund", "FUND-", 0),
        ("fund_transfer", "FT-", 0),
        ("grant", "GRT-", 0),
        ("grant_expense", "GEXP-", 0),
        ("program", "PROG-", 0),
        ("volunteer", "VOL-", 0),
        ("volunteer_shift", "VSHIFT-", 0),
        ("campaign", "CAMP-", 0),
        ("pledge", "PLDG-", 0),
        ("tax_receipt", "TREC-", 0),
    ]
    for entity_type, prefix, current in series:
        conn.execute(
            """INSERT OR IGNORE INTO naming_series
               (id, entity_type, prefix, current_value, company_id)
               VALUES (?, ?, ?, ?, ?)""",
            (_uuid(), entity_type, prefix, current, company_id)
        )
    conn.commit()


def seed_account(conn, company_id: str, name="Test Account",
                 root_type="asset", account_type=None,
                 account_number=None) -> str:
    """Insert a GL account and return its ID."""
    aid = _uuid()
    direction = "debit_normal" if root_type in ("asset", "expense") else "credit_normal"
    conn.execute(
        """INSERT INTO account (id, name, account_number, root_type, account_type,
           balance_direction, company_id, depth)
           VALUES (?, ?, ?, ?, ?, ?, ?, 0)""",
        (aid, name, account_number or f"ACC-{aid[:6]}", root_type,
         account_type, direction, company_id)
    )
    conn.commit()
    return aid


def seed_fiscal_year(conn, company_id: str, name=None,
                     start="2026-01-01", end="2026-12-31") -> str:
    """Insert a fiscal year and return its ID."""
    fid = _uuid()
    conn.execute(
        """INSERT INTO fiscal_year (id, name, start_date, end_date, company_id)
           VALUES (?, ?, ?, ?, ?)""",
        (fid, name or f"FY-{fid[:6]}", start, end, company_id)
    )
    conn.commit()
    return fid


def seed_cost_center(conn, company_id: str, name="Main CC") -> str:
    """Insert a cost center and return its ID."""
    ccid = _uuid()
    conn.execute(
        """INSERT INTO cost_center (id, name, company_id, is_group)
           VALUES (?, ?, ?, 0)""",
        (ccid, name, company_id)
    )
    conn.commit()
    return ccid


def build_env(conn) -> dict:
    """Create a full nonprofit test environment.

    Returns dict with all IDs needed for nonprofit domain tests:
    company_id, donor info, fund, campaign, program, volunteer, naming series.
    """
    cid = seed_company(conn)
    seed_naming_series(conn, cid)
    donor = seed_donor(conn, cid, "Alice Benefactor")
    fund_id = seed_fund(conn, cid, "General Fund", "unrestricted")
    campaign_id = seed_campaign(conn, cid, "Annual Giving", "10000.00", "active", fund_id)
    program_id = seed_program(conn, cid, "Youth Outreach", "5000.00", fund_id)
    volunteer_id = seed_volunteer(conn, cid, "Bob Helper", "bob@example.com")
    grant_id = seed_grant(conn, cid, "Community Grant", "Ford Foundation",
                          "50000.00", "applied", fund_id)

    # GL accounts for posting tests
    fiscal_year_id = seed_fiscal_year(conn, cid)
    cc_id = seed_cost_center(conn, cid)
    cash_acct = seed_account(conn, cid, "Cash", "asset", "cash", "1000")
    revenue_acct = seed_account(conn, cid, "Contribution Revenue", "income",
                                "revenue", "4000")
    expense_acct = seed_account(conn, cid, "Program Expense", "expense",
                                "expense", "5000")

    return {
        "company_id": cid,
        "donor_id": donor["donor_id"],
        "customer_id": donor["customer_id"],
        "fund_id": fund_id,
        "campaign_id": campaign_id,
        "program_id": program_id,
        "volunteer_id": volunteer_id,
        "grant_id": grant_id,
        "fiscal_year_id": fiscal_year_id,
        "cc_id": cc_id,
        "cash_acct": cash_acct,
        "revenue_acct": revenue_acct,
        "expense_acct": expense_acct,
    }
