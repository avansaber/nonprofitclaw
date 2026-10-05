"""NonprofitClaw migration 002: donor substantiation split-receipt columns.

Adds the two ``nonprofitclaw_donation`` split-receipt columns
(``goods_services_fair_value``, ``goods_services_description``) and the four
``nonprofitclaw_tax_receipt`` substantiation columns (``deductible_amount``,
``goods_services_fair_value``, ``goods_services_description``,
``statements``). An install that predates this release acquires them here; a
fresh install already carries them because ``init_db.py`` declares them.

Each addition is a fixed ``ALTER TABLE ... ADD COLUMN`` string in the
constructclaw 001 shape (no name formatted into SQL), run only when the
column is absent. Every column is nullable TEXT with no default on both
backends, exactly as the installer declares, so every pre-existing row
keeps the values it held and reads NULL in the new columns.

WHO CAN NEED IT. Only an install where NonprofitClaw is present runs a
NonprofitClaw migration. The guard is the donation table: where it (and the
tax receipt table) is absent the migration prints and does nothing, so a
foundation-only database is left untouched.

NOTHING ELSE IS TOUCHED. No table is created, no index is added, and no
existing row is read or rewritten. A re-run finds every column present and
adds nothing.

money: none of the six additions changes money handling. The columns are
TEXT carriers the actions fill with Decimal strings, and the module's
Decimal-in-Python discipline is untouched here.

Usage:
    python3 002_donor_substantiation_columns.py [--db-path PATH] [--report-only]
"""
import argparse
import importlib.util
import os
import sys

# Six nullable TEXT columns and nothing else. No row is read, rewritten,
# inserted or deleted; before this run each of these columns held nothing on
# every install. That is the "a new column" case of the definition, not a
# rewrite.
MIGRATION_DATA_CLASS = "none"

# Deployed-lib bootstrap, guarded: production has nothing pre-imported so this
# resolves the installed lib, while a caller that already bound a tree (tests,
# the module runner inside a worktree) keeps its binding (ADR-0034 step 2d).
if importlib.util.find_spec("erpclaw_lib") is None:  # pragma: no cover - env-dependent
    sys.path.insert(0, os.path.join(
        os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))

from erpclaw_lib import seam  # noqa: E402
from erpclaw_lib.db import get_connection, get_dialect  # noqa: E402
from erpclaw_lib.paths import db_default  # noqa: E402

DEFAULT_DB_PATH = db_default()

DONATION_TABLE = "nonprofitclaw_donation"
RECEIPT_TABLE = "nonprofitclaw_tax_receipt"

# (table, column, qualified name, statement). Spelled out in full rather than
# assembled from the names, so no name is ever formatted INTO SQL. The type is
# TEXT and the column is nullable with no default on both backends, exactly
# what the installer declares for each of these six.
ADD_COLUMNS = (
    (DONATION_TABLE,
     "goods_services_fair_value",
     "nonprofitclaw_donation.goods_services_fair_value",
     "ALTER TABLE nonprofitclaw_donation ADD COLUMN goods_services_fair_value TEXT"),
    (DONATION_TABLE,
     "goods_services_description",
     "nonprofitclaw_donation.goods_services_description",
     "ALTER TABLE nonprofitclaw_donation ADD COLUMN goods_services_description TEXT"),
    (RECEIPT_TABLE,
     "deductible_amount",
     "nonprofitclaw_tax_receipt.deductible_amount",
     "ALTER TABLE nonprofitclaw_tax_receipt ADD COLUMN deductible_amount TEXT"),
    (RECEIPT_TABLE,
     "goods_services_fair_value",
     "nonprofitclaw_tax_receipt.goods_services_fair_value",
     "ALTER TABLE nonprofitclaw_tax_receipt ADD COLUMN goods_services_fair_value TEXT"),
    (RECEIPT_TABLE,
     "goods_services_description",
     "nonprofitclaw_tax_receipt.goods_services_description",
     "ALTER TABLE nonprofitclaw_tax_receipt ADD COLUMN goods_services_description TEXT"),
    (RECEIPT_TABLE,
     "statements",
     "nonprofitclaw_tax_receipt.statements",
     "ALTER TABLE nonprofitclaw_tax_receipt ADD COLUMN statements TEXT"),
)

ADDED_ALL = [qualified for _, _, qualified, _ in ADD_COLUMNS]


def _target(db_path):
    """The database to act on.

    On PostgreSQL the runner passes ``ERPCLAW_DB_URL`` when set, else the
    location it was given (from the module manager, the SQLite default file
    path); ``connect.py`` passes ``None`` or the action's ``--db-path``; a URL
    argument is used as given, anything else yields ``None``.
    """
    if get_dialect() == "postgresql":
        if isinstance(db_path, str) and (
                db_path.startswith("postgresql://")
                or db_path.startswith("postgres://")):
            return db_path
        return None
    return db_path or os.environ.get("ERPCLAW_DB_PATH", DEFAULT_DB_PATH)


def run_migration(db_path=None, report_only=False):
    """Add whichever of the six substantiation columns this install lacks.

    Returns ``{"provisioned", "tables", "indexes", "added", "already_present",
    "report_only"}`` with qualified ``"table.column"`` names so a caller can
    tell a real upgrade from a no-op. The runner discards it.
    """
    target = _target(db_path)

    donation_present = seam.table_exists(DONATION_TABLE, target)
    receipt_present = seam.table_exists(RECEIPT_TABLE, target)
    if not donation_present and not receipt_present:
        print("  nonprofitclaw_donation absent on this install. Nothing to do.")
        return {"provisioned": False, "reason": "nonprofitclaw not installed",
                "tables": 0, "indexes": 0, "added": [],
                "already_present": [], "report_only": report_only}

    present_tables = set()
    if donation_present:
        present_tables.add(DONATION_TABLE)
    if receipt_present:
        present_tables.add(RECEIPT_TABLE)

    present_columns = {}
    for table in present_tables:
        present_columns[table] = set(seam.column_names(table, target))

    already_present = []
    pending = []
    for table, column, qualified, statement in ADD_COLUMNS:
        if table not in present_tables:
            continue
        if column in present_columns[table]:
            already_present.append(qualified)
        else:
            pending.append((table, column, qualified, statement))

    if report_only:
        if not pending:
            print("  nonprofitclaw donor substantiation shape already present. "
                  "Nothing to do.")
        else:
            print(f"  report-only: the real run would add "
                  f"{', '.join(q for _, _, q, _ in pending)}. Nothing written.")
        return {"provisioned": False, "tables": 0, "indexes": 0, "added": [],
                "already_present": already_present, "report_only": True}

    added = []
    if pending:
        conn = get_connection(target)
        try:
            for table, column, qualified, statement in pending:
                conn.execute(statement)
                print(f"  {qualified}: added.")
            conn.commit()
        finally:
            conn.close()
        added = [qualified for _, _, qualified, _ in pending]
    for qualified in already_present:
        print(f"  {qualified}: already present.")
    if not pending:
        print("  nonprofitclaw donor substantiation columns already present "
              "(idempotent no-op).")
    return {"provisioned": False, "tables": 0, "indexes": 0,
            "added": added, "already_present": already_present,
            "report_only": False}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Migration 002: add the nonprofitclaw donor substantiation "
                    "split-receipt columns")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    parser.add_argument("--report-only", action="store_true",
                        help="State what the real run would do; write nothing.")
    args = parser.parse_args()
    run_migration(args.db_path, report_only=args.report_only)
    print("nonprofitclaw migration 002 "
          + ("report complete (no writes)." if args.report_only else "complete."))
