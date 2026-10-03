"""Migration 001: grant receipt table and grant funding basis (m746a).

Rewinds a fresh install to the pre-change shape (no receipt table, no
funding-basis column) and proves the migration adds exactly those two, changes
no value any row held, and is a no-op everywhere else.
"""
import importlib.util
import os
import sys
import uuid

import nonprofit_helpers  # noqa: F401 — binds erpclaw_lib to the tree under test
from nonprofit_helpers import (  # noqa: E402
    INIT_SCHEMA_PATH,
    build_env,
    call_action,
    get_conn,
    init_all_tables,
    is_ok,
    load_db_query,
    ns,
    seed_company,
    seed_fund,
    seed_grant,
)

import pytest  # noqa: E402

from erpclaw_lib import seam  # noqa: E402
from erpclaw_lib.db import db_integrity_error, get_connection  # noqa: E402

load_db_query()  # puts the module's scripts directory on sys.path

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_MODULE_DIR = os.path.dirname(os.path.dirname(_TESTS_DIR))  # nonprofitclaw/
_MIGRATIONS_DIR = os.path.join(_MODULE_DIR, "migrations")


def _load(name, filename, directory):
    path = os.path.join(directory, filename)
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


mig = _load("nonprofitclaw_migration_001",
            "001_grant_receipt_and_funding_basis.py", _MIGRATIONS_DIR)
installer = _load("nonprofitclaw_installer_001", "init_db.py", _MODULE_DIR)

GRANT_TABLE = "nonprofitclaw_grant"
RECEIPT_TABLE = "nonprofitclaw_grant_receipt"
ADDED_COLUMN = "nonprofitclaw_grant.funding_basis"
ADDED_BOTH = [ADDED_COLUMN, RECEIPT_TABLE]

# Fixed statements: no name is formatted into SQL, in the test either.
_DROP_RECEIPT = "DROP TABLE nonprofitclaw_grant_receipt"
_DROP_FUNDING_BASIS = "ALTER TABLE nonprofitclaw_grant DROP COLUMN funding_basis"

def _rewind(path):
    """Return a fresh database to its pre-change shape."""
    conn = get_connection(path)
    conn.execute(_DROP_RECEIPT)
    conn.commit()
    conn.close()
    assert not seam.table_exists(RECEIPT_TABLE, path)
    conn = get_connection(path)
    conn.execute(_DROP_FUNDING_BASIS)
    conn.commit()
    conn.close()
    assert "funding_basis" not in seam.column_names(GRANT_TABLE, path)


@pytest.fixture
def pre_db(tmp_path):
    """A fresh database rewound to the shape that predates this release."""
    path = str(tmp_path / "pre.sqlite")
    init_all_tables(path)
    _rewind(path)
    return path


def _seed_fund_and_grant(path):
    """One fund and one active grant; returns ids plus the rows as held."""
    conn = get_conn(path)
    try:
        company_id = seed_company(conn)
        fund_id = seed_fund(conn, company_id, "General Fund", "unrestricted", "0")
        grant_id = str(uuid.uuid4())
        conn.execute(
            """INSERT INTO nonprofitclaw_grant
               (id, naming_series, name, grantor_name, grantor_type, grant_type,
                amount, received_amount, spent_amount, remaining_amount,
                status, company_id, fund_id)
               VALUES (?, 'GRT-0001', 'Community Grant', 'Ford Foundation',
                'foundation', 'project', '50000.00', '10000.00', '0',
                '40000.00', 'active', ?, ?)""",
            (grant_id, company_id, fund_id))
        conn.commit()
        grant_before = dict(conn.execute(
            "SELECT * FROM nonprofitclaw_grant WHERE id = ?",
            (grant_id,)).fetchone())
        fund_before = dict(conn.execute(
            "SELECT * FROM nonprofitclaw_fund WHERE id = ?",
            (fund_id,)).fetchone())
    finally:
        conn.close()
    return company_id, fund_id, grant_id, grant_before, fund_before


def test_migration_adds_column_and_table_on_a_pre_change_database(pre_db):
    _company_id, fund_id, grant_id, grant_before, fund_before = \
        _seed_fund_and_grant(pre_db)

    result = mig.run_migration(pre_db)

    assert result["added"] == ADDED_BOTH
    assert result["tables"] == 1
    assert seam.table_exists(RECEIPT_TABLE, pre_db)
    assert "funding_basis" in seam.column_names(GRANT_TABLE, pre_db)
    conn = get_conn(pre_db)
    try:
        grant_after = dict(conn.execute(
            "SELECT * FROM nonprofitclaw_grant WHERE id = ?",
            (grant_id,)).fetchone())
        fund_after = dict(conn.execute(
            "SELECT * FROM nonprofitclaw_fund WHERE id = ?",
            (fund_id,)).fetchone())
    finally:
        conn.close()
    assert grant_after.pop("funding_basis") == "advance"
    assert grant_after == grant_before
    assert fund_after == fund_before


def test_upgrade_order_table_first_then_column(pre_db, tmp_path):
    """The module update path re-runs the installer before module migrations.

    The installer restores the receipt table but leaves the grant table
    without its new column; the migration then adds only the column.
    """
    _company_id, fund_id, grant_id, grant_before, fund_before = \
        _seed_fund_and_grant(pre_db)

    installer.create_nonprofitclaw_tables(pre_db)

    result = mig.run_migration(pre_db)

    assert result["added"] == ["nonprofitclaw_grant.funding_basis"]
    assert result["already_present"] == ["nonprofitclaw_grant_receipt"]
    assert result["tables"] == 0
    conn = get_conn(pre_db)
    try:
        grant_after = dict(conn.execute(
            "SELECT * FROM nonprofitclaw_grant WHERE id = ?",
            (grant_id,)).fetchone())
        fund_after = dict(conn.execute(
            "SELECT * FROM nonprofitclaw_fund WHERE id = ?",
            (fund_id,)).fetchone())
    finally:
        conn.close()
    assert grant_after.pop("funding_basis") == "advance"
    assert grant_after == grant_before
    assert fund_after == fund_before
    fresh = str(tmp_path / "fresh.sqlite")
    init_all_tables(fresh)
    assert (seam.describe_table(GRANT_TABLE, pre_db)
            == seam.describe_table(GRANT_TABLE, fresh))
    assert (seam.describe_table(RECEIPT_TABLE, pre_db)
            == seam.describe_table(RECEIPT_TABLE, fresh))


def test_upgrade_order_column_first_then_table(pre_db):
    """Column present but receipt table absent: only the table is added."""
    mig.run_migration(pre_db)
    conn = get_connection(pre_db)
    conn.execute(_DROP_RECEIPT)
    conn.commit()
    conn.close()
    assert not seam.table_exists(RECEIPT_TABLE, pre_db)

    result = mig.run_migration(pre_db)

    assert result["added"] == ["nonprofitclaw_grant_receipt"]
    assert result["tables"] == 1
    assert seam.table_exists(RECEIPT_TABLE, pre_db)
    assert "funding_basis" in seam.column_names(GRANT_TABLE, pre_db)


def test_second_run_changes_nothing(pre_db):
    first = mig.run_migration(pre_db)
    assert first["added"] == ADDED_BOTH

    second = mig.run_migration(pre_db)

    assert second["added"] == []
    assert second["tables"] == 0
    assert second["indexes"] == 0
    assert sorted(second["already_present"]) == sorted(ADDED_BOTH)


def test_report_only_writes_nothing(pre_db):
    result = mig.run_migration(pre_db, report_only=True)

    assert result["report_only"] is True
    assert result["added"] == []
    assert result["tables"] == 0
    assert "funding_basis" not in seam.column_names(GRANT_TABLE, pre_db)
    assert not seam.table_exists(RECEIPT_TABLE, pre_db)


def test_no_op_without_nonprofitclaw(tmp_path):
    path = str(tmp_path / "foundation.sqlite")
    spec = importlib.util.spec_from_file_location("init_schema", INIT_SCHEMA_PATH)
    foundation = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(foundation)
    foundation.init_db(path)

    result = mig.run_migration(path)

    assert result["provisioned"] is False
    assert result["tables"] == 0
    assert [t for t in seam.table_names(path)
            if t.startswith("nonprofitclaw_")] == []


def test_fresh_install_equals_migrated(pre_db, tmp_path):
    fresh = str(tmp_path / "fresh.sqlite")
    init_all_tables(fresh)
    mig.run_migration(pre_db)

    assert (seam.describe_table(GRANT_TABLE, pre_db)
            == seam.describe_table(GRANT_TABLE, fresh))
    assert (seam.describe_table(RECEIPT_TABLE, pre_db)
            == seam.describe_table(RECEIPT_TABLE, fresh))


def _check_funding_basis(path):
    conn = get_conn(path)
    try:
        company_id = seed_company(conn)
        grant_id = seed_grant(conn, company_id, fund_id=None)
        conn.execute(
            "UPDATE nonprofitclaw_grant SET funding_basis = 'reimbursement'"
            " WHERE id = ?", (grant_id,))
        conn.commit()
        assert conn.execute(
            "SELECT funding_basis FROM nonprofitclaw_grant WHERE id = ?",
            (grant_id,)).fetchone()["funding_basis"] == "reimbursement"
        with pytest.raises(db_integrity_error(conn)):
            conn.execute(
                "UPDATE nonprofitclaw_grant SET funding_basis = 'other'"
                " WHERE id = ?", (grant_id,))
            conn.commit()
        conn.rollback()
        assert conn.execute(
            "SELECT funding_basis FROM nonprofitclaw_grant WHERE id = ?",
            (grant_id,)).fetchone()["funding_basis"] == "reimbursement"
    finally:
        conn.close()


def test_funding_basis_is_checked(tmp_path):
    fresh = str(tmp_path / "fresh.sqlite")
    init_all_tables(fresh)
    _check_funding_basis(fresh)

    pre = str(tmp_path / "pre.sqlite")
    init_all_tables(pre)
    _rewind(pre)
    mig.run_migration(pre)
    _check_funding_basis(pre)


def test_new_grant_is_advance(tmp_path):
    path = str(tmp_path / "grant.sqlite")
    init_all_tables(path)
    conn = get_conn(path)
    try:
        env = build_env(conn)
        mod = load_db_query()
        result = call_action(mod.ACTIONS["nonprofit-add-grant"], conn, ns(
            company_id=env["company_id"],
            name="Community Grant",
            grantor_name="Ford Foundation",
            grantor_type="foundation",
            grant_type="project",
            amount="50000.00",
            fund_id=None,
            start_date=None,
            end_date=None,
            reporting_freq="quarterly",
            notes=None))
        assert is_ok(result), result
        row = conn.execute(
            "SELECT funding_basis FROM nonprofitclaw_grant WHERE id = ?",
            (result["id"],)).fetchone()
    finally:
        conn.close()
    assert row["funding_basis"] == "advance"


def _check_receipt_rows(path):
    conn = get_conn(path)
    try:
        company_id = seed_company(conn)
        fund_id = seed_fund(conn, company_id)
        grant_id = seed_grant(conn, company_id, fund_id=fund_id)
        with pytest.raises(db_integrity_error(conn)):
            conn.execute(
                """INSERT INTO nonprofitclaw_grant_receipt
                   (id, grant_id, receipt_date, amount, cash_account_id,
                    credit_account_id, status, company_id)
                   VALUES (?, ?, '2026-03-01', '2500.00', 'cash-1', 'rev-1',
                    'bogus', ?)""",
                (str(uuid.uuid4()), grant_id, company_id))
            conn.commit()
        conn.rollback()
        with pytest.raises(db_integrity_error(conn)):
            conn.execute(
                """INSERT INTO nonprofitclaw_grant_receipt
                   (id, grant_id, receipt_date, amount, cash_account_id,
                    credit_account_id, status, company_id)
                   VALUES (?, ?, '2026-03-01', '2500.00', 'cash-1', 'rev-1',
                    'received', ?)""",
                (str(uuid.uuid4()), str(uuid.uuid4()), company_id))
            conn.commit()
        conn.rollback()
        receipt_id = str(uuid.uuid4())
        conn.execute(
            """INSERT INTO nonprofitclaw_grant_receipt
               (id, grant_id, fund_id, receipt_date, amount, cash_account_id,
                credit_account_id, company_id)
               VALUES (?, ?, ?, '2026-03-01', '2500.00', 'cash-1', 'rev-1', ?)""",
            (receipt_id, grant_id, fund_id, company_id))
        conn.commit()
        row = conn.execute(
            "SELECT status, amount, receipt_date"
            " FROM nonprofitclaw_grant_receipt WHERE id = ?",
            (receipt_id,)).fetchone()
    finally:
        conn.close()
    assert row["status"] == "received"
    assert row["amount"] == "2500.00"
    assert row["receipt_date"] == "2026-03-01"


def test_receipt_rows_are_constrained(tmp_path):
    fresh = str(tmp_path / "fresh.sqlite")
    init_all_tables(fresh)
    _check_receipt_rows(fresh)

    pre = str(tmp_path / "pre.sqlite")
    init_all_tables(pre)
    _rewind(pre)
    mig.run_migration(pre)
    _check_receipt_rows(pre)


def test_it_declares_that_it_changes_no_data():
    """The M102 declaration is part of the migration's contract."""
    assert mig.MIGRATION_DATA_CLASS == "none"


def test_column_and_table_are_declared_by_the_installer_too():
    """Fresh and migrated cannot diverge if both sources name the same two."""
    assert "funding_basis" in {c.name for c in installer.GRANT.columns}
    assert installer.GRANT_RECEIPT.name == RECEIPT_TABLE
