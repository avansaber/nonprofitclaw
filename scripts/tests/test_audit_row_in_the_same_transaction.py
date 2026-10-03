"""An audit row commits in the same transaction as the change it records.

Regression tests for the router-drop problem: nonprofit actions used to call
``audit()`` after ``conn.commit()``, so the command router -- which closes the
database without another commit -- rolled the audit row back and lost it while
the change itself survived. Every action must write its audit row before its
commit, in the same transaction, so either both are saved or neither is.
"""
import ast
import json
import os
import subprocess
import sys
from pathlib import Path

from erpclaw_lib.db import get_connection
from erpclaw_lib.query import P, Q, Table

SCRIPTS_DIR = Path(__file__).resolve().parent.parent
DB_QUERY = SCRIPTS_DIR / "db_query.py"

_IN_TREE_LIB = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))),
    "erpclaw", "scripts", "erpclaw-setup", "lib",
)

_audit = Table("audit_log")


def _violations_in_source(source, filename="<memory>"):
    """Return [(function, line)] for audit() calls after the last conn.commit().

    One entry per function whose latest ``audit(...)`` call sits in source
    order after that function's latest ``conn.commit()`` call.
    """
    tree = ast.parse(source, filename=filename)
    violations = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        commits = []
        audits = []
        stack = list(node.body)
        while stack:
            child = stack.pop()
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef,
                                  ast.ClassDef, ast.Lambda)):
                continue
            if isinstance(child, ast.Call):
                func = child.func
                if (isinstance(func, ast.Attribute) and func.attr == "commit"
                        and isinstance(func.value, ast.Name)
                        and func.value.id == "conn"):
                    commits.append(child.lineno)
                elif isinstance(func, ast.Name) and func.id == "audit":
                    audits.append(child.lineno)
            stack.extend(ast.iter_child_nodes(child))
        if commits and audits and max(audits) > max(commits):
            violations.append((node.name, max(audits)))
    return violations


def test_no_audit_call_sits_after_the_final_commit():
    offenders = []
    for path in sorted(SCRIPTS_DIR.glob("*.py")):
        for func_name, line in _violations_in_source(
                path.read_text(), filename=path.name):
            offenders.append(f"{path.name}:{func_name}:{line}")
    assert offenders == [], (
        "audit() must run before the final conn.commit() in the same "
        "transaction; audit-after-commit found at: " + ", ".join(offenders)
    )


def test_planted_audit_after_commit_is_flagged():
    planted = (
        "def add_thing(conn, args):\n"
        '    conn.execute("INSERT INTO t (id) VALUES (1)")\n'
        "    conn.commit()\n"
        '    audit(conn, SKILL, "nonprofit-add-thing", '
        '"nonprofitclaw_thing", thing_id)\n'
        '    return ok({"id": thing_id})\n'
    )
    flagged = _violations_in_source(planted, filename="planted.py")
    assert flagged == [("add_thing", 4)], flagged
    fixed = planted.replace(
        "    conn.commit()\n"
        '    audit(conn, SKILL, "nonprofit-add-thing", '
        '"nonprofitclaw_thing", thing_id)\n',
        '    audit(conn, SKILL, "nonprofit-add-thing", '
        '"nonprofitclaw_thing", thing_id)\n'
        "    conn.commit()\n",
    )
    assert _violations_in_source(fixed, filename="planted.py") == []


def _run_router(action, extra_args, db_path, tmp_path):
    env = dict(os.environ, ERPCLAW_HOME=str(tmp_path), PYTHONPATH=_IN_TREE_LIB)
    env["ERPCLAW_DB_PATH"] = db_path
    env.pop("ERPCLAW_DB_URL", None)
    cmd = [sys.executable, str(DB_QUERY), "--action", action] + extra_args
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120, env=env)
    assert proc.returncode == 0, (
        f"router {action} failed: {proc.stdout}\n{proc.stderr}")
    return json.loads(proc.stdout)


def _audit_rows_for(entity_id):
    """Audit rows for one record id, read on a fresh connection (PyPika)."""
    reader = get_connection()
    try:
        q = (
            Q.from_(_audit)
            .select(_audit.skill, _audit.action,
                    _audit.entity_type, _audit.entity_id)
            .where(_audit.entity_id == P())
        )
        return reader.execute(q.get_sql(), (entity_id,)).fetchall()
    finally:
        reader.close()


def test_router_add_fund_leaves_its_audit_row(db_path, env, tmp_path):
    # nonprofit-add-donor cannot run through the router here: it shells out
    # to the erpclaw skill, which is not installed in this tree, so the fund
    # path stands in for the same commit semantics.
    created = _run_router("nonprofit-add-fund", [
        "--company-id", env["company_id"],
        "--name", "Router Audit Fund",
    ], db_path, tmp_path)
    assert created.get("status") == "ok", created
    fund_id = created["id"]
    rows = _audit_rows_for(fund_id)
    matches = [
        row for row in rows
        if (row["skill"], row["action"], row["entity_type"])
        == ("nonprofitclaw", "nonprofit-add-fund", "nonprofitclaw_fund")
    ]
    assert len(matches) == 1, (
        f"expected exactly one audit row for fund {fund_id!r}, found "
        f"{len(matches)} in "
        f"{[(r['skill'], r['action'], r['entity_type'], r['entity_id']) for r in rows]}"
    )


def test_router_add_grant_leaves_its_audit_row(db_path, env, tmp_path):
    created = _run_router("nonprofit-add-grant", [
        "--company-id", env["company_id"],
        "--name", "Router Audit Grant",
        "--grantor-name", "Audit Foundation",
        "--amount", "20000",
    ], db_path, tmp_path)
    assert created.get("status") == "ok", created
    grant_id = created["id"]
    rows = _audit_rows_for(grant_id)
    matches = [
        row for row in rows
        if (row["skill"], row["action"], row["entity_type"])
        == ("nonprofitclaw", "nonprofit-add-grant", "nonprofitclaw_grant")
    ]
    assert len(matches) == 1, (
        f"expected exactly one audit row for grant {grant_id!r}, found "
        f"{len(matches)} in "
        f"{[(r['skill'], r['action'], r['entity_type'], r['entity_id']) for r in rows]}"
    )


def test_router_child_imports_in_tree_erpclaw_lib(db_path, tmp_path):
    env = dict(os.environ, ERPCLAW_HOME=str(tmp_path), PYTHONPATH=_IN_TREE_LIB)
    env["ERPCLAW_DB_PATH"] = db_path
    env.pop("ERPCLAW_DB_URL", None)
    proc = subprocess.run(
        [sys.executable, "-c", "import erpclaw_lib, sys; print(erpclaw_lib.__file__)"],
        capture_output=True, text=True, timeout=120, env=env,
    )
    assert proc.returncode == 0, (
        f"in-tree import probe failed: {proc.stdout}\n{proc.stderr}")
    lib_file = proc.stdout.strip()
    assert lib_file.startswith(_IN_TREE_LIB + os.sep), (
        f"child imported {lib_file!r}, expected it under {_IN_TREE_LIB!r}")
