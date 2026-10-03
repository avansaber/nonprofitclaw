"""NonprofitClaw migration 001: grant receipt table and grant funding basis.

Adds the ``funding_basis`` column to ``nonprofitclaw_grant`` (``advance`` for
every grant that predates the column, via the column default) and provisions
the ``nonprofitclaw_grant_receipt`` table — one row per amount received on a
grant. An install that predates this release acquires both here; a fresh
install already carries both because ``init_db.py`` declares them.

THE DECLARATION IS NOT REPEATED HERE. The new table is copied from the
module's OWN ``init_db.py`` declaration via ``to_metadata`` into a fresh
MetaData, with the three declarations it points at carried along so its
references resolve — the same load-by-file shape as educlaw's migration 004,
which copies its link table instead of re-typing it. A second hand-typed copy
is the exact drift class that shape exists to repair. The new column is a
fixed ``ALTER TABLE ... ADD COLUMN`` string in the constructclaw 001 shape
(no name formatted into SQL), run only when the column is absent.

WHO CAN NEED IT. Only an install where NonprofitClaw is present runs a
NonprofitClaw migration. The guard is the grant table: where it is absent the
migration prints and does nothing, so a foundation-only database is left
untouched.

NOTHING ELSE IS TOUCHED. The provisioner creates only what is missing, so a
re-run creates nothing, and no existing row is read or rewritten: the column
default supplies ``advance`` for every pre-existing grant row.

Usage:
    python3 001_grant_receipt_and_funding_basis.py [--db-path PATH] [--report-only]
"""
import argparse
import importlib.util
import os
import sys

# A new column (every existing grant gets the column's default) and a new
# table; no value a row held before the run is different afterwards.
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

GRANT_TABLE = "nonprofitclaw_grant"
RECEIPT_TABLE = "nonprofitclaw_grant_receipt"
COLUMN = "funding_basis"
ADDED_COLUMN = "nonprofitclaw_grant.funding_basis"

# Fully-literal DDL (table + column are fixed constants — no interpolation, so
# no injection surface and no f-string scanner flag). The NOT NULL column
# carries its default so the ADD itself supplies every existing row, and the
# CHECK name matches the one the installer declares.
ADD_FUNDING_BASIS = "ALTER TABLE nonprofitclaw_grant ADD COLUMN funding_basis TEXT NOT NULL DEFAULT 'advance' CONSTRAINT ck_nonprofitclaw_grant_funding_basis CHECK (funding_basis IN ('advance','reimbursement'))"


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


def _receipt_metadata():
    """The receipt-table declaration, loaded — never re-typed here.

    Copies ``company``, ``nonprofitclaw_grant`` and ``nonprofitclaw_fund``
    as reference-only declarations plus ``GRANT_RECEIPT`` from the module's
    own ``init_db.py`` into a fresh MetaData that ``seam.provision`` can act
    on. The first three stay reference-only in the copy, so the provisioner
    never creates another table; the receipt table is the only owned table
    in the fresh metadata and the only thing a run can create.
    """
    init_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             "..", "init_db.py")
    spec = importlib.util.spec_from_file_location("nonprofitclaw_init_db_001",
                                                  init_path)
    init_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(init_mod)

    sa = seam._sqlalchemy()
    fresh = sa.MetaData()
    seam.reference_table("company", fresh)
    seam.reference_table("nonprofitclaw_grant", fresh)
    seam.reference_table("nonprofitclaw_fund", fresh)
    init_mod.GRANT_RECEIPT.to_metadata(fresh)
    return fresh


def run_migration(db_path=None, report_only=False):
    """Add the funding-basis column and provision the receipt table.

    Returns ``{"provisioned", "tables", "indexes", "added", "already_present",
    "report_only"}`` with the names ``"nonprofitclaw_grant.funding_basis"``
    and ``"nonprofitclaw_grant_receipt"`` so a caller can tell a real upgrade
    from a no-op. The runner discards it.
    """
    target = _target(db_path)

    if not seam.table_exists(GRANT_TABLE, target):
        print("  nonprofitclaw_grant absent on this install. Nothing to do.")
        return {"provisioned": False, "reason": "nonprofitclaw not installed",
                "tables": 0, "indexes": 0, "added": [],
                "already_present": [], "report_only": report_only}

    column_present = COLUMN in seam.column_names(GRANT_TABLE, target)
    table_present = seam.table_exists(RECEIPT_TABLE, target)

    already_present = []
    if column_present:
        already_present.append(ADDED_COLUMN)
    if table_present:
        already_present.append(RECEIPT_TABLE)

    if report_only:
        if column_present and table_present:
            print("  nonprofitclaw grant receipt shape already present. "
                  "Nothing to do.")
        else:
            missing = []
            if not column_present:
                missing.append(ADDED_COLUMN)
            if not table_present:
                missing.append(RECEIPT_TABLE)
            print(f"  report-only: the real run would add "
                  f"{', '.join(missing)}. Nothing written.")
        return {"provisioned": False, "tables": 0, "indexes": 0, "added": [],
                "already_present": already_present, "report_only": True}

    added = []
    if column_present:
        print(f"  {ADDED_COLUMN}: already present.")
    else:
        conn = get_connection(target)
        try:
            conn.execute(ADD_FUNDING_BASIS)
            conn.commit()
        finally:
            conn.close()
        print(f"  {ADDED_COLUMN}: added.")
        added.append(ADDED_COLUMN)

    metadata = _receipt_metadata()
    created = seam.provision(metadata, target)
    if table_present:
        print(f"  {RECEIPT_TABLE} already present. Nothing to do.")
    else:
        print(f"  provisioned {RECEIPT_TABLE} from the init_db.py "
              f"declaration: {created['tables']} table(s), "
              f"{created['indexes']} index(es).")
        if created["tables"]:
            added.append(RECEIPT_TABLE)
    return {"provisioned": bool(created["tables"]),
            "tables": created["tables"], "indexes": created["indexes"],
            "added": added, "already_present": already_present,
            "report_only": False}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Migration 001: add the nonprofitclaw grant receipt table "
                    "and grant funding basis column")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    parser.add_argument("--report-only", action="store_true",
                        help="State what the real run would do; write nothing.")
    args = parser.parse_args()
    run_migration(args.db_path, report_only=args.report_only)
    print("nonprofitclaw migration 001 "
          + ("report complete (no writes)." if args.report_only else "complete."))
