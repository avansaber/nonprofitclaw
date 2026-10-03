"""Guarded money totals: lost-update protection (m635b).

Every stored money total is written only if the row still holds the value
read. A stale read raises a conflict and rolls back the whole action.
"""
from decimal import Decimal, ROUND_HALF_UP

from nonprofit_helpers import (
    call_action, get_conn, is_error, is_ok, load_db_query, ns,
    seed_campaign, seed_donation, seed_donor, seed_fund, seed_grant,
)

load_db_query()

class _FakeRow:
    def __init__(self, data, order):
        self._data = data
        self._order = order
    def __getitem__(self, key):
        if isinstance(key, int):
            return self._data[self._order[key]]
        return self._data[key]
    def keys(self):
        return list(self._order)
    def __len__(self):
        return len(self._order)
    def __iter__(self):
        for col in self._order:
            yield self._data[col]


class _PatchCursor:
    def __init__(self, real_cur, patch):
        self._cur = real_cur
        self._patch = patch
        try:
            self.rowcount = real_cur.rowcount
        except Exception:
            self.rowcount = -1
        try:
            self.lastrowid = real_cur.lastrowid
        except Exception:
            self.lastrowid = None
        try:
            self.description = real_cur.description
        except Exception:
            self.description = None
    def _wrap(self, row):
        if row is None:
            return None
        desc = self._cur.description or []
        order = [d[0] for d in desc]
        data = {}
        for col in order:
            try:
                data[col] = row[col]
            except Exception:
                data[col] = None
        for col, val in self._patch.items():
            if col in data:
                data[col] = val
        return _FakeRow(data, order)
    def fetchone(self):
        return self._wrap(self._cur.fetchone())
    def fetchall(self):
        rows = self._cur.fetchall()
        return [self._wrap(r) for r in rows]
    def fetchmany(self, size=None):
        rows = self._cur.fetchmany(size) if size else self._cur.fetchmany()
        return [self._wrap(r) for r in rows]


class _StaleProxy:
    def __init__(self, real_conn, stale):
        self._c = real_conn
        self._stale = stale
    def execute(self, sql, params=()):
        if params is None:
            params = ()
        stripped = sql.strip().upper() if isinstance(sql, str) else ""
        if stripped.startswith("SELECT"):
            patch = {}
            for table, cols in self._stale.items():
                if table in sql:
                    for col, val in cols.items():
                        if col in sql or "*" in sql:
                            patch[col] = val
            if patch:
                real_cur = self._c.execute(sql, params)
                return _PatchCursor(real_cur, patch)
        return self._c.execute(sql, params)
    def __getattr__(self, name):
        return getattr(self._c, name)


def _audit_count(conn):
    return conn.execute("SELECT COUNT(*) FROM audit_log").fetchone()[0]


def _round2(val):
    return val.quantize(Decimal("0.01"), ROUND_HALF_UP)


def _donate_args(env, fund_id, campaign_id, amount, day="2026-03-10"):
    return ns(
        company_id=env["company_id"], donor_id=env["donor_id"], amount=amount,
        payment_method="check", donation_date=day, reference=None,
        fund_id=fund_id, campaign_id=campaign_id, is_recurring=None,
        recurrence_freq=None, notes=None, cash_account_id=None,
        revenue_account_id=None, cost_center_id=None,
    )

def test_add_donation_campaign_guard(conn, db_path, env):
    import donors as dm
    campaign_id = seed_campaign(conn, env["company_id"], "Guard Camp Add", "100000.00", "active")
    fund_id = env["fund_id"]
    old = conn.execute("SELECT raised_amount FROM nonprofitclaw_campaign WHERE id=?", (campaign_id,)).fetchone()["raised_amount"]
    conn_b = get_conn(db_path)
    try:
        conn_b.execute("UPDATE nonprofitclaw_campaign SET raised_amount=?, donor_count=? WHERE id=?", ("777.77", 5, campaign_id))
        conn_b.commit()
        before_audit = _audit_count(conn)
        before_don = conn.execute("SELECT COUNT(*) FROM nonprofitclaw_donation").fetchone()[0]
        proxy = _StaleProxy(conn, {"nonprofitclaw_campaign": {"raised_amount": old}})
        res = call_action(dm.add_donation, proxy, _donate_args(env, fund_id, campaign_id, "100.00"))
        exp = "Failed to add donation: Concurrent change to nonprofitclaw_campaign %s: raised_amount is no longer %s; nothing was written" % (campaign_id, old)
        assert res.get("status") == "error", res
        assert res["message"] == exp, res
        fresh = get_conn(db_path)
        try:
            got = fresh.execute("SELECT raised_amount, donor_count FROM nonprofitclaw_campaign WHERE id=?", (campaign_id,)).fetchone()
            assert got["raised_amount"] == "777.77"
            assert got["donor_count"] == 5
            assert fresh.execute("SELECT COUNT(*) FROM nonprofitclaw_donation").fetchone()[0] == before_don
            assert fresh.execute("SELECT current_balance FROM nonprofitclaw_fund WHERE id=?", (fund_id,)).fetchone()["current_balance"] == "0"
            assert _audit_count(conn) == before_audit
        finally:
            fresh.close()
    finally:
        conn_b.close()


def test_add_donation_donor_guard(conn, db_path, env):
    import donors as dm
    campaign_id = seed_campaign(conn, env["company_id"], "Guard Donor Add", "100000.00", "active")
    fund_id = env["fund_id"]
    old = conn.execute("SELECT total_donated FROM nonprofitclaw_donor_ext WHERE id=?", (env["donor_id"],)).fetchone()["total_donated"]
    conn_b = get_conn(db_path)
    try:
        conn_b.execute("UPDATE nonprofitclaw_donor_ext SET total_donated=? WHERE id=?", ("888.88", env["donor_id"]))
        conn_b.commit()
        before_audit = _audit_count(conn)
        before_don = conn.execute("SELECT COUNT(*) FROM nonprofitclaw_donation").fetchone()[0]
        proxy = _StaleProxy(conn, {"nonprofitclaw_donor_ext": {"total_donated": old}})
        res = call_action(dm.add_donation, proxy, _donate_args(env, fund_id, campaign_id, "100.00"))
        exp = "Failed to add donation: Concurrent change to nonprofitclaw_donor_ext %s: total_donated is no longer %s; nothing was written" % (env["donor_id"], old)
        assert res.get("status") == "error", res
        assert res["message"] == exp, res
        fresh = get_conn(db_path)
        try:
            assert fresh.execute("SELECT total_donated FROM nonprofitclaw_donor_ext WHERE id=?", (env["donor_id"],)).fetchone()["total_donated"] == "888.88"
            assert fresh.execute("SELECT raised_amount FROM nonprofitclaw_campaign WHERE id=?", (campaign_id,)).fetchone()["raised_amount"] == "0"
            assert fresh.execute("SELECT COUNT(*) FROM nonprofitclaw_donation").fetchone()[0] == before_don
            assert _audit_count(conn) == before_audit
        finally:
            fresh.close()
    finally:
        conn_b.close()


def test_add_donation_fund_guard(conn, db_path, env):
    import donors as dm
    campaign_id = seed_campaign(conn, env["company_id"], "Guard Fund Add", "100000.00", "active")
    fund_id = env["fund_id"]
    old = conn.execute("SELECT current_balance FROM nonprofitclaw_fund WHERE id=?", (fund_id,)).fetchone()["current_balance"]
    conn_b = get_conn(db_path)
    try:
        conn_b.execute("UPDATE nonprofitclaw_fund SET current_balance=? WHERE id=?", ("999.99", fund_id))
        conn_b.commit()
        before_audit = _audit_count(conn)
        before_don = conn.execute("SELECT COUNT(*) FROM nonprofitclaw_donation").fetchone()[0]
        proxy = _StaleProxy(conn, {"nonprofitclaw_fund": {"current_balance": old}})
        res = call_action(dm.add_donation, proxy, _donate_args(env, fund_id, campaign_id, "100.00"))
        exp = "Failed to add donation: Concurrent change to nonprofitclaw_fund %s: current_balance is no longer %s; nothing was written" % (fund_id, old)
        assert res.get("status") == "error", res
        assert res["message"] == exp, res
        fresh = get_conn(db_path)
        try:
            assert fresh.execute("SELECT current_balance FROM nonprofitclaw_fund WHERE id=?", (fund_id,)).fetchone()["current_balance"] == "999.99"
            assert fresh.execute("SELECT COUNT(*) FROM nonprofitclaw_donation").fetchone()[0] == before_don
            assert _audit_count(conn) == before_audit
        finally:
            fresh.close()
    finally:
        conn_b.close()


def _make_received_donation(conn, env, fund_id, campaign_id, amount="100.00"):
    import donors as dm
    res = call_action(dm.add_donation, conn, _donate_args(env, fund_id, campaign_id, amount, day="2026-03-11"))
    assert is_ok(res), res
    return res["id"]


def test_refund_campaign_guard(conn, db_path, env):
    import donors as dm
    campaign_id = seed_campaign(conn, env["company_id"], "Guard Camp Ref", "100000.00", "active")
    fund_id = env["fund_id"]
    did = _make_received_donation(conn, env, fund_id, campaign_id, "100.00")
    conn.commit()
    old = conn.execute("SELECT raised_amount FROM nonprofitclaw_campaign WHERE id=?", (campaign_id,)).fetchone()["raised_amount"]
    conn_b = get_conn(db_path)
    try:
        conn_b.execute("UPDATE nonprofitclaw_campaign SET raised_amount=? WHERE id=?", ("999.99", campaign_id))
        conn_b.commit()
        before_audit = _audit_count(conn)
        proxy = _StaleProxy(conn, {"nonprofitclaw_campaign": {"raised_amount": old}})
        res = call_action(dm.refund_donation, proxy, ns(id=did, donation_id=None))
        exp = "Refund failed: Concurrent change to nonprofitclaw_campaign %s: raised_amount is no longer %s; nothing was written" % (campaign_id, old)
        assert res.get("status") == "error", res
        assert res["message"] == exp, res
        fresh = get_conn(db_path)
        try:
            assert fresh.execute("SELECT raised_amount FROM nonprofitclaw_campaign WHERE id=?", (campaign_id,)).fetchone()["raised_amount"] == "999.99"
            assert fresh.execute("SELECT status FROM nonprofitclaw_donation WHERE id=?", (did,)).fetchone()["status"] == "received"
            assert fresh.execute("SELECT total_donated FROM nonprofitclaw_donor_ext WHERE id=?", (env["donor_id"],)).fetchone()["total_donated"] == "100.00"
            assert _audit_count(conn) == before_audit
        finally:
            fresh.close()
    finally:
        conn_b.close()


def test_refund_donor_guard(conn, db_path, env):
    import donors as dm
    campaign_id = seed_campaign(conn, env["company_id"], "Guard Donor Ref", "100000.00", "active")
    fund_id = env["fund_id"]
    did = _make_received_donation(conn, env, fund_id, campaign_id, "100.00")
    conn.commit()
    old = conn.execute("SELECT total_donated FROM nonprofitclaw_donor_ext WHERE id=?", (env["donor_id"],)).fetchone()["total_donated"]
    conn_b = get_conn(db_path)
    try:
        conn_b.execute("UPDATE nonprofitclaw_donor_ext SET total_donated=? WHERE id=?", ("555.55", env["donor_id"]))
        conn_b.commit()
        before_audit = _audit_count(conn)
        proxy = _StaleProxy(conn, {"nonprofitclaw_donor_ext": {"total_donated": old}})
        res = call_action(dm.refund_donation, proxy, ns(id=did, donation_id=None))
        exp = "Refund failed: Concurrent change to nonprofitclaw_donor_ext %s: total_donated is no longer %s; nothing was written" % (env["donor_id"], old)
        assert res.get("status") == "error", res
        assert res["message"] == exp, res
        fresh = get_conn(db_path)
        try:
            assert fresh.execute("SELECT total_donated FROM nonprofitclaw_donor_ext WHERE id=?", (env["donor_id"],)).fetchone()["total_donated"] == "555.55"
            assert fresh.execute("SELECT status FROM nonprofitclaw_donation WHERE id=?", (did,)).fetchone()["status"] == "received"
            assert _audit_count(conn) == before_audit
        finally:
            fresh.close()
    finally:
        conn_b.close()


def test_refund_fund_guard(conn, db_path, env):
    import donors as dm
    campaign_id = seed_campaign(conn, env["company_id"], "Guard Fund Ref", "100000.00", "active")
    fund_id = env["fund_id"]
    did = _make_received_donation(conn, env, fund_id, campaign_id, "100.00")
    conn.commit()
    old = conn.execute("SELECT current_balance FROM nonprofitclaw_fund WHERE id=?", (fund_id,)).fetchone()["current_balance"]
    conn_b = get_conn(db_path)
    try:
        conn_b.execute("UPDATE nonprofitclaw_fund SET current_balance=? WHERE id=?", ("444.44", fund_id))
        conn_b.commit()
        before_audit = _audit_count(conn)
        proxy = _StaleProxy(conn, {"nonprofitclaw_fund": {"current_balance": old}})
        res = call_action(dm.refund_donation, proxy, ns(id=did, donation_id=None))
        exp = "Refund failed: Concurrent change to nonprofitclaw_fund %s: current_balance is no longer %s; nothing was written" % (fund_id, old)
        assert res.get("status") == "error", res
        assert res["message"] == exp, res
        fresh = get_conn(db_path)
        try:
            assert fresh.execute("SELECT current_balance FROM nonprofitclaw_fund WHERE id=?", (fund_id,)).fetchone()["current_balance"] == "444.44"
            assert fresh.execute("SELECT status FROM nonprofitclaw_donation WHERE id=?", (did,)).fetchone()["status"] == "received"
            assert _audit_count(conn) == before_audit
        finally:
            fresh.close()
    finally:
        conn_b.close()

def test_fulfill_pledge_pledge_guard(conn, db_path, env):
    import campaigns as cm
    campaign_id = seed_campaign(conn, env["company_id"], "Guard Pledge", "100000.00", "active")
    res = call_action(cm.add_pledge, conn, ns(company_id=env["company_id"], donor_id=env["donor_id"], campaign_id=campaign_id, fund_id=None, amount="1000.00", pledge_date="2026-03-01", frequency="one_time", next_due_date=None, end_date=None, notes=None))
    assert is_ok(res), res
    pid = res["id"]
    conn.commit()
    old = conn.execute("SELECT fulfilled_amount FROM nonprofitclaw_pledge WHERE id=?", (pid,)).fetchone()["fulfilled_amount"]
    conn_b = get_conn(db_path)
    try:
        conn_b.execute("UPDATE nonprofitclaw_pledge SET fulfilled_amount=? WHERE id=?", ("400.00", pid))
        conn_b.commit()
        before_audit = _audit_count(conn)
        proxy = _StaleProxy(conn, {"nonprofitclaw_pledge": {"fulfilled_amount": old}})
        r2 = call_action(cm.fulfill_pledge, proxy, ns(pledge_id=pid, id=None, amount="100.00"))
        exp = "Fulfillment failed: Concurrent change to nonprofitclaw_pledge %s: fulfilled_amount is no longer %s; nothing was written" % (pid, old)
        assert r2.get("status") == "error", r2
        assert r2["message"] == exp, r2
        fresh = get_conn(db_path)
        try:
            assert fresh.execute("SELECT fulfilled_amount FROM nonprofitclaw_pledge WHERE id=?", (pid,)).fetchone()["fulfilled_amount"] == "400.00"
            assert fresh.execute("SELECT raised_amount FROM nonprofitclaw_campaign WHERE id=?", (campaign_id,)).fetchone()["raised_amount"] == "0"
            assert _audit_count(conn) == before_audit
        finally:
            fresh.close()
    finally:
        conn_b.close()


def test_fulfill_pledge_campaign_guard(conn, db_path, env):
    import campaigns as cm
    campaign_id = seed_campaign(conn, env["company_id"], "Guard Pledge Camp", "100000.00", "active")
    res = call_action(cm.add_pledge, conn, ns(company_id=env["company_id"], donor_id=env["donor_id"], campaign_id=campaign_id, fund_id=None, amount="1000.00", pledge_date="2026-03-01", frequency="one_time", next_due_date=None, end_date=None, notes=None))
    assert is_ok(res), res
    pid = res["id"]
    conn.commit()
    old = conn.execute("SELECT raised_amount FROM nonprofitclaw_campaign WHERE id=?", (campaign_id,)).fetchone()["raised_amount"]
    conn_b = get_conn(db_path)
    try:
        conn_b.execute("UPDATE nonprofitclaw_campaign SET raised_amount=? WHERE id=?", ("555.55", campaign_id))
        conn_b.commit()
        before_audit = _audit_count(conn)
        proxy = _StaleProxy(conn, {"nonprofitclaw_campaign": {"raised_amount": old}})
        r2 = call_action(cm.fulfill_pledge, proxy, ns(pledge_id=pid, id=None, amount="100.00"))
        exp = "Fulfillment failed: Concurrent change to nonprofitclaw_campaign %s: raised_amount is no longer %s; nothing was written" % (campaign_id, old)
        assert r2.get("status") == "error", r2
        assert r2["message"] == exp, r2
        fresh = get_conn(db_path)
        try:
            assert fresh.execute("SELECT raised_amount FROM nonprofitclaw_campaign WHERE id=?", (campaign_id,)).fetchone()["raised_amount"] == "555.55"
            assert fresh.execute("SELECT fulfilled_amount FROM nonprofitclaw_pledge WHERE id=?", (pid,)).fetchone()["fulfilled_amount"] == "0"
            assert _audit_count(conn) == before_audit
        finally:
            fresh.close()
    finally:
        conn_b.close()


def test_merge_donor_guard(conn, db_path, env):
    import donors as dm
    src = seed_donor(conn, env["company_id"], "Merge Source")
    tgt_id = env["donor_id"]
    seed_donation(conn, env["company_id"], src["donor_id"], amount="100.00")
    seed_donation(conn, env["company_id"], tgt_id, amount="200.00")
    old = conn.execute("SELECT total_donated FROM nonprofitclaw_donor_ext WHERE id=?", (tgt_id,)).fetchone()["total_donated"]
    conn_b = get_conn(db_path)
    try:
        conn_b.execute("UPDATE nonprofitclaw_donor_ext SET total_donated=? WHERE id=?", ("999.99", tgt_id))
        conn_b.commit()
        before_audit = _audit_count(conn)
        proxy = _StaleProxy(conn, {"nonprofitclaw_donor_ext": {"total_donated": old}})
        res = call_action(dm.merge_donors, proxy, ns(source_donor_id=src["donor_id"], target_donor_id=tgt_id))
        exp = "Merge failed: Concurrent change to nonprofitclaw_donor_ext %s: total_donated is no longer %s; nothing was written" % (tgt_id, old)
        assert res.get("status") == "error", res
        assert res["message"] == exp, res
        fresh = get_conn(db_path)
        try:
            assert fresh.execute("SELECT total_donated FROM nonprofitclaw_donor_ext WHERE id=?", (tgt_id,)).fetchone()["total_donated"] == "999.99"
            assert fresh.execute("SELECT COUNT(*) FROM nonprofitclaw_donor_ext WHERE id=?", (src["donor_id"],)).fetchone()[0] == 1
            assert fresh.execute("SELECT COUNT(*) FROM nonprofitclaw_donation WHERE donor_id=?", (src["donor_id"],)).fetchone()[0] == 1
            assert _audit_count(conn) == before_audit
        finally:
            fresh.close()
    finally:
        conn_b.close()


def test_fund_transfer_source_guard(conn, db_path, env):
    import funds as fm
    src_id = seed_fund(conn, env["company_id"], "Guard Src", "unrestricted", "500.00")
    dst_id = seed_fund(conn, env["company_id"], "Guard Dst", "unrestricted", "0")
    res = call_action(fm.add_fund_transfer, conn, ns(company_id=env["company_id"], from_fund_id=src_id, to_fund_id=dst_id, amount="100.00", transfer_date="2026-03-01", reason="x", approved_by=None))
    assert is_ok(res), res
    tid = res["id"]
    conn.commit()
    old = conn.execute("SELECT current_balance FROM nonprofitclaw_fund WHERE id=?", (src_id,)).fetchone()["current_balance"]
    conn_b = get_conn(db_path)
    try:
        conn_b.execute("UPDATE nonprofitclaw_fund SET current_balance=? WHERE id=?", ("111.11", src_id))
        conn_b.commit()
        before_audit = _audit_count(conn)
        proxy = _StaleProxy(conn, {"nonprofitclaw_fund": {"current_balance": old}})
        r2 = call_action(fm.approve_fund_transfer, proxy, ns(id=tid, approved_by="Treasurer"))
        assert r2.get("status") == "error", r2
        assert ("Concurrent change to nonprofitclaw_fund %s: current_balance is no longer %s; nothing was written" % (src_id, old)) in r2["message"], r2
        assert r2["message"].startswith("Approval failed: "), r2
        fresh = get_conn(db_path)
        try:
            assert fresh.execute("SELECT current_balance FROM nonprofitclaw_fund WHERE id=?", (src_id,)).fetchone()["current_balance"] == "111.11"
            assert fresh.execute("SELECT current_balance FROM nonprofitclaw_fund WHERE id=?", (dst_id,)).fetchone()["current_balance"] == "0"
            assert fresh.execute("SELECT status FROM nonprofitclaw_fund_transfer WHERE id=?", (tid,)).fetchone()["status"] == "draft"
            assert _audit_count(conn) == before_audit
        finally:
            fresh.close()
    finally:
        conn_b.close()


def test_approve_grant_expense_guard(conn, db_path, env):
    import grants as gm
    gres = call_action(gm.add_grant, conn, ns(company_id=env["company_id"], name="Guard Grant", grantor_name="City Trust", grantor_type="government", grant_type="project", amount="1000.00", fund_id=None, start_date="2026-01-01", end_date="2026-12-31", reporting_freq=None, notes=None))
    assert is_ok(gres), gres
    gid = gres["id"]
    conn.execute("UPDATE nonprofitclaw_grant SET status=?, received_amount=? WHERE id=?", ("active", "1000.00", gid))
    conn.commit()
    eres = call_action(gm.add_grant_expense, conn, ns(company_id=env["company_id"], grant_id=gid, amount="100.00", category="program", description="spend", expense_date="2026-03-05", receipt_reference=None))
    assert is_ok(eres), eres
    eid = eres["id"]
    conn.commit()
    old_spent = conn.execute("SELECT spent_amount FROM nonprofitclaw_grant WHERE id=?", (gid,)).fetchone()["spent_amount"]
    old_rem = conn.execute("SELECT remaining_amount FROM nonprofitclaw_grant WHERE id=?", (gid,)).fetchone()["remaining_amount"]
    conn_b = get_conn(db_path)
    try:
        conn_b.execute("UPDATE nonprofitclaw_grant SET spent_amount=?, remaining_amount=? WHERE id=?", ("200.00", "800.00", gid))
        conn_b.commit()
        before_audit = _audit_count(conn)
        proxy = _StaleProxy(conn, {"nonprofitclaw_grant": {"spent_amount": old_spent, "remaining_amount": old_rem}})
        r2 = call_action(gm.approve_grant_expense, proxy, ns(id=eid, expense_account_id=env["expense_acct"], cash_account_id=env["cash_acct"], cost_center_id=env["cc_id"]))
        exp = "Approval failed: Concurrent change to nonprofitclaw_grant %s: spent_amount is no longer %s; nothing was written" % (gid, old_spent)
        assert r2.get("status") == "error", r2
        assert r2["message"] == exp, r2
        fresh = get_conn(db_path)
        try:
            got = fresh.execute("SELECT spent_amount, remaining_amount FROM nonprofitclaw_grant WHERE id=?", (gid,)).fetchone()
            assert got["spent_amount"] == "200.00"
            assert got["remaining_amount"] == "800.00"
            assert fresh.execute("SELECT status FROM nonprofitclaw_grant_expense WHERE id=?", (eid,)).fetchone()["status"] == "draft"
            assert fresh.execute("SELECT COUNT(*) FROM gl_entry WHERE voucher_id=?", (eid,)).fetchone()[0] == 0
            assert _audit_count(conn) == before_audit
        finally:
            fresh.close()
    finally:
        conn_b.close()

def test_donor_exact_sum_large_amounts(conn, env):
    import donors as dm
    a = "90000000000001.11"
    b = "90000000000002.22"
    exact = str(_round2(Decimal(a) + Decimal(b)))
    assert exact == "180000000000003.33"
    as_float = float(a) + float(b)
    float_text = str(_round2(Decimal(str(as_float))))
    assert float_text != exact
    r1 = call_action(dm.add_donation, conn, _donate_args(env, None, None, a, day="2026-03-01"))
    assert is_ok(r1), r1
    r2 = call_action(dm.add_donation, conn, _donate_args(env, None, None, b, day="2026-03-02"))
    assert is_ok(r2), r2
    got = conn.execute("SELECT total_donated FROM nonprofitclaw_donor_ext WHERE id=?", (env["donor_id"],)).fetchone()["total_donated"]
    assert got == exact, got


def test_grant_exact_sum_large_amounts(conn, env):
    import grants as gm
    a = "90000000000001.11"
    b = "90000000000002.22"
    exact = str(_round2(Decimal(a) + Decimal(b)))
    assert exact == "180000000000003.33"
    as_float = float(a) + float(b)
    assert str(_round2(Decimal(str(as_float)))) != exact
    gres = call_action(gm.add_grant, conn, ns(company_id=env["company_id"], name="Big Grant", grantor_name="City Trust", grantor_type="government", grant_type="project", amount="500000000000000.00", fund_id=None, start_date="2026-01-01", end_date="2026-12-31", reporting_freq=None, notes=None))
    assert is_ok(gres), gres
    gid = gres["id"]
    conn.execute("UPDATE nonprofitclaw_grant SET status=?, received_amount=? WHERE id=?", ("active", "500000000000000.00", gid))
    conn.commit()
    e1 = call_action(gm.add_grant_expense, conn, ns(company_id=env["company_id"], grant_id=gid, amount=a, category="program", description="big one", expense_date="2026-03-05", receipt_reference=None))
    assert is_ok(e1), e1
    e2 = call_action(gm.add_grant_expense, conn, ns(company_id=env["company_id"], grant_id=gid, amount=b, category="program", description="big two", expense_date="2026-03-06", receipt_reference=None))
    assert is_ok(e2), e2
    r1 = call_action(gm.approve_grant_expense, conn, ns(id=e1["id"], expense_account_id=env["expense_acct"], cash_account_id=env["cash_acct"], cost_center_id=env["cc_id"]))
    assert is_ok(r1), r1
    r2 = call_action(gm.approve_grant_expense, conn, ns(id=e2["id"], expense_account_id=env["expense_acct"], cash_account_id=env["cash_acct"], cost_center_id=env["cc_id"]))
    assert is_ok(r2), r2
    got = conn.execute("SELECT spent_amount FROM nonprofitclaw_grant WHERE id=?", (gid,)).fetchone()["spent_amount"]
    assert got == exact, got


def test_refund_donor_count_decrement(conn, env):
    import donors as dm
    campaign_id = seed_campaign(conn, env["company_id"], "Count Camp", "100000.00", "active")
    conn.execute("UPDATE nonprofitclaw_campaign SET donor_count=?, raised_amount=? WHERE id=?", (3, "100.00", campaign_id))
    conn.commit()
    did = seed_donation(conn, env["company_id"], env["donor_id"], amount="10.00", campaign_id=campaign_id, status="received")
    res = call_action(dm.refund_donation, conn, ns(id=did, donation_id=None))
    assert is_ok(res), res
    got = conn.execute("SELECT donor_count, raised_amount FROM nonprofitclaw_campaign WHERE id=?", (campaign_id,)).fetchone()
    assert got["donor_count"] == 2
    assert got["raised_amount"] == "90.00"


def test_refund_donor_count_floor_zero(conn, env):
    import donors as dm
    campaign_id = seed_campaign(conn, env["company_id"], "Count Zero", "100000.00", "active")
    conn.execute("UPDATE nonprofitclaw_campaign SET donor_count=?, raised_amount=? WHERE id=?", (0, "10.00", campaign_id))
    conn.commit()
    did = seed_donation(conn, env["company_id"], env["donor_id"], amount="10.00", campaign_id=campaign_id, status="received")
    res = call_action(dm.refund_donation, conn, ns(id=did, donation_id=None))
    assert is_ok(res), res
    got = conn.execute("SELECT donor_count, raised_amount FROM nonprofitclaw_campaign WHERE id=?", (campaign_id,)).fetchone()
    assert got["donor_count"] == 0
    assert got["raised_amount"] == "0.00"
