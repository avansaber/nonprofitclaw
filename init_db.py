#!/usr/bin/env python3
"""NonprofitClaw schema — non-profit management tables.

Prerequisite: ERPClaw init_db.py must have run first (creates foundation tables).
Run: python3 init_db.py [db_path]
"""
import os
import sqlite3
import sys


DEFAULT_DB_PATH = os.path.expanduser("~/.openclaw/erpclaw/data.sqlite")


def create_nonprofitclaw_tables(db_path):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=ON")

    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    if "company" not in tables:
        print("ERROR: Foundation tables not found. Run erpclaw-setup first.")
        sys.exit(1)

    conn.executescript("""
        -- ==========================================================
        -- NonprofitClaw Non-Profit Domain Tables
        -- ==========================================================

        -- Extension table — core donor data lives in customer(id).
        -- Fields removed (live in core customer): name, email, phone,
        -- address, city, state, zip_code, tax_id.
        CREATE TABLE IF NOT EXISTS nonprofitclaw_donor_ext (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT DEFAULT 'NDNR-',
            customer_id     TEXT NOT NULL REFERENCES customer(id),
            donor_type      TEXT DEFAULT 'individual'
                            CHECK(donor_type IN ('individual','corporate','foundation','government','anonymous')),
            donor_level     TEXT DEFAULT 'standard'
                            CHECK(donor_level IN ('standard','bronze','silver','gold','platinum','major')),
            first_donation_date TEXT,
            last_donation_date TEXT,
            total_donated   TEXT DEFAULT '0',
            donation_count  INTEGER DEFAULT 0,
            notes           TEXT,
            is_active       INTEGER DEFAULT 1,
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT DEFAULT (datetime('now')),
            updated_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_nonprofitclaw_donor_ext_company ON nonprofitclaw_donor_ext(company_id);
        CREATE INDEX IF NOT EXISTS idx_nonprofitclaw_donor_ext_customer ON nonprofitclaw_donor_ext(customer_id);
        CREATE INDEX IF NOT EXISTS idx_nonprofitclaw_donor_ext_type ON nonprofitclaw_donor_ext(donor_type);
        CREATE INDEX IF NOT EXISTS idx_nonprofitclaw_donor_ext_level ON nonprofitclaw_donor_ext(donor_level);

        CREATE TABLE IF NOT EXISTS nonprofitclaw_donation (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            donor_id        TEXT NOT NULL REFERENCES nonprofitclaw_donor_ext(id) ON DELETE RESTRICT,
            fund_id         TEXT REFERENCES nonprofitclaw_fund(id) ON DELETE RESTRICT,
            campaign_id     TEXT REFERENCES nonprofitclaw_campaign(id) ON DELETE RESTRICT,
            donation_date   TEXT NOT NULL DEFAULT (date('now')),
            amount          TEXT NOT NULL DEFAULT '0',
            payment_method  TEXT NOT NULL DEFAULT 'check'
                            CHECK(payment_method IN ('cash','check','credit_card','bank_transfer','online','in_kind','stock','crypto','other')),
            reference       TEXT,
            is_recurring    INTEGER NOT NULL DEFAULT 0 CHECK(is_recurring IN (0,1)),
            recurrence_freq TEXT CHECK(recurrence_freq IN ('monthly','quarterly','annually',NULL)),
            in_kind_description TEXT,
            in_kind_fair_value TEXT,
            tax_deductible  INTEGER NOT NULL DEFAULT 1 CHECK(tax_deductible IN (0,1)),
            receipt_sent    INTEGER NOT NULL DEFAULT 0 CHECK(receipt_sent IN (0,1)),
            gl_entry_ids    TEXT,
            notes           TEXT,
            status          TEXT NOT NULL DEFAULT 'received'
                            CHECK(status IN ('pledged','received','deposited','refunded','cancelled')),
            company_id      TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at      TEXT DEFAULT (datetime('now')),
            updated_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_nonprofitclaw_donation_donor ON nonprofitclaw_donation(donor_id);
        CREATE INDEX IF NOT EXISTS idx_nonprofitclaw_donation_fund ON nonprofitclaw_donation(fund_id);
        CREATE INDEX IF NOT EXISTS idx_nonprofitclaw_donation_date ON nonprofitclaw_donation(donation_date);
        CREATE INDEX IF NOT EXISTS idx_nonprofitclaw_donation_status ON nonprofitclaw_donation(status);

        CREATE TABLE IF NOT EXISTS nonprofitclaw_fund (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            name            TEXT NOT NULL,
            fund_type       TEXT NOT NULL DEFAULT 'unrestricted'
                            CHECK(fund_type IN ('unrestricted','temporarily_restricted','permanently_restricted')),
            description     TEXT,
            target_amount   TEXT,
            current_balance TEXT NOT NULL DEFAULT '0',
            start_date      TEXT,
            end_date        TEXT,
            is_active       INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
            company_id      TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at      TEXT DEFAULT (datetime('now')),
            updated_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_nonprofitclaw_fund_company ON nonprofitclaw_fund(company_id);
        CREATE INDEX IF NOT EXISTS idx_nonprofitclaw_fund_type ON nonprofitclaw_fund(fund_type);

        CREATE TABLE IF NOT EXISTS nonprofitclaw_fund_transfer (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            from_fund_id    TEXT NOT NULL REFERENCES nonprofitclaw_fund(id) ON DELETE RESTRICT,
            to_fund_id      TEXT NOT NULL REFERENCES nonprofitclaw_fund(id) ON DELETE RESTRICT,
            amount          TEXT NOT NULL DEFAULT '0',
            transfer_date   TEXT NOT NULL DEFAULT (date('now')),
            reason          TEXT,
            approved_by     TEXT,
            status          TEXT NOT NULL DEFAULT 'draft'
                            CHECK(status IN ('draft','approved','completed','cancelled')),
            company_id      TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_nonprofitclaw_ft_company ON nonprofitclaw_fund_transfer(company_id);

        CREATE TABLE IF NOT EXISTS nonprofitclaw_grant (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            name            TEXT NOT NULL,
            grantor_name    TEXT NOT NULL,
            grantor_type    TEXT NOT NULL DEFAULT 'foundation'
                            CHECK(grantor_type IN ('foundation','government','corporate','individual','other')),
            grant_type      TEXT NOT NULL DEFAULT 'project'
                            CHECK(grant_type IN ('project','operating','capital','capacity_building','other')),
            amount          TEXT NOT NULL DEFAULT '0',
            received_amount TEXT NOT NULL DEFAULT '0',
            spent_amount    TEXT NOT NULL DEFAULT '0',
            remaining_amount TEXT NOT NULL DEFAULT '0',
            fund_id         TEXT REFERENCES nonprofitclaw_fund(id) ON DELETE RESTRICT,
            start_date      TEXT,
            end_date        TEXT,
            reporting_freq  TEXT DEFAULT 'quarterly'
                            CHECK(reporting_freq IN ('monthly','quarterly','semi_annual','annual','final_only')),
            next_report_due TEXT,
            status          TEXT NOT NULL DEFAULT 'applied'
                            CHECK(status IN ('applied','awarded','active','completed','closed','rejected')),
            notes           TEXT,
            company_id      TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at      TEXT DEFAULT (datetime('now')),
            updated_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_nonprofitclaw_grant_company ON nonprofitclaw_grant(company_id);
        CREATE INDEX IF NOT EXISTS idx_nonprofitclaw_grant_status ON nonprofitclaw_grant(status);

        CREATE TABLE IF NOT EXISTS nonprofitclaw_grant_expense (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            grant_id        TEXT NOT NULL REFERENCES nonprofitclaw_grant(id) ON DELETE RESTRICT,
            expense_date    TEXT NOT NULL DEFAULT (date('now')),
            amount          TEXT NOT NULL DEFAULT '0',
            category        TEXT NOT NULL DEFAULT 'program'
                            CHECK(category IN ('program','personnel','overhead','travel','equipment','supplies','other')),
            description     TEXT,
            receipt_reference TEXT,
            gl_entry_ids    TEXT,
            status          TEXT NOT NULL DEFAULT 'draft'
                            CHECK(status IN ('draft','submitted','approved','rejected')),
            company_id      TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_nonprofitclaw_gexp_grant ON nonprofitclaw_grant_expense(grant_id);

        CREATE TABLE IF NOT EXISTS nonprofitclaw_program (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            name            TEXT NOT NULL,
            description     TEXT,
            fund_id         TEXT REFERENCES nonprofitclaw_fund(id) ON DELETE RESTRICT,
            budget          TEXT NOT NULL DEFAULT '0',
            spent           TEXT NOT NULL DEFAULT '0',
            beneficiary_count INTEGER NOT NULL DEFAULT 0,
            start_date      TEXT,
            end_date        TEXT,
            outcome_metrics TEXT,
            is_active       INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
            company_id      TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at      TEXT DEFAULT (datetime('now')),
            updated_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_nonprofitclaw_program_company ON nonprofitclaw_program(company_id);

        CREATE TABLE IF NOT EXISTS nonprofitclaw_volunteer (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            name            TEXT NOT NULL,
            email           TEXT,
            phone           TEXT,
            skills          TEXT,
            availability    TEXT,
            total_hours     TEXT NOT NULL DEFAULT '0',
            shift_count     INTEGER NOT NULL DEFAULT 0,
            start_date      TEXT,
            is_active       INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
            company_id      TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at      TEXT DEFAULT (datetime('now')),
            updated_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_nonprofitclaw_vol_company ON nonprofitclaw_volunteer(company_id);

        CREATE TABLE IF NOT EXISTS nonprofitclaw_volunteer_shift (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            volunteer_id    TEXT NOT NULL REFERENCES nonprofitclaw_volunteer(id) ON DELETE RESTRICT,
            program_id      TEXT REFERENCES nonprofitclaw_program(id) ON DELETE RESTRICT,
            shift_date      TEXT NOT NULL DEFAULT (date('now')),
            hours           TEXT NOT NULL DEFAULT '0',
            description     TEXT,
            status          TEXT NOT NULL DEFAULT 'scheduled'
                            CHECK(status IN ('scheduled','completed','cancelled','no_show')),
            company_id      TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_nonprofitclaw_vshift_vol ON nonprofitclaw_volunteer_shift(volunteer_id);
        CREATE INDEX IF NOT EXISTS idx_nonprofitclaw_vshift_date ON nonprofitclaw_volunteer_shift(shift_date);

        CREATE TABLE IF NOT EXISTS nonprofitclaw_pledge (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            donor_id        TEXT NOT NULL REFERENCES nonprofitclaw_donor_ext(id) ON DELETE RESTRICT,
            campaign_id     TEXT REFERENCES nonprofitclaw_campaign(id) ON DELETE RESTRICT,
            fund_id         TEXT REFERENCES nonprofitclaw_fund(id) ON DELETE RESTRICT,
            pledge_date     TEXT NOT NULL DEFAULT (date('now')),
            amount          TEXT NOT NULL DEFAULT '0',
            fulfilled_amount TEXT NOT NULL DEFAULT '0',
            frequency       TEXT NOT NULL DEFAULT 'one_time'
                            CHECK(frequency IN ('one_time','monthly','quarterly','annually')),
            next_due_date   TEXT,
            end_date        TEXT,
            notes           TEXT,
            status          TEXT NOT NULL DEFAULT 'active'
                            CHECK(status IN ('active','fulfilled','partially_fulfilled','cancelled','lapsed')),
            company_id      TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at      TEXT DEFAULT (datetime('now')),
            updated_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_nonprofitclaw_pledge_donor ON nonprofitclaw_pledge(donor_id);
        CREATE INDEX IF NOT EXISTS idx_nonprofitclaw_pledge_status ON nonprofitclaw_pledge(status);

        CREATE TABLE IF NOT EXISTS nonprofitclaw_campaign (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            name            TEXT NOT NULL,
            description     TEXT,
            fund_id         TEXT REFERENCES nonprofitclaw_fund(id) ON DELETE RESTRICT,
            goal_amount     TEXT NOT NULL DEFAULT '0',
            raised_amount   TEXT NOT NULL DEFAULT '0',
            donor_count     INTEGER NOT NULL DEFAULT 0,
            start_date      TEXT,
            end_date        TEXT,
            status          TEXT NOT NULL DEFAULT 'draft'
                            CHECK(status IN ('draft','active','completed','cancelled')),
            company_id      TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at      TEXT DEFAULT (datetime('now')),
            updated_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_nonprofitclaw_campaign_company ON nonprofitclaw_campaign(company_id);
        CREATE INDEX IF NOT EXISTS idx_nonprofitclaw_campaign_status ON nonprofitclaw_campaign(status);

        CREATE TABLE IF NOT EXISTS nonprofitclaw_tax_receipt (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            donor_id        TEXT NOT NULL REFERENCES nonprofitclaw_donor_ext(id) ON DELETE RESTRICT,
            donation_id     TEXT REFERENCES nonprofitclaw_donation(id) ON DELETE RESTRICT,
            receipt_date    TEXT NOT NULL DEFAULT (date('now')),
            amount          TEXT NOT NULL DEFAULT '0',
            tax_year        TEXT NOT NULL,
            receipt_type    TEXT NOT NULL DEFAULT 'single'
                            CHECK(receipt_type IN ('single','annual_summary')),
            sent_date       TEXT,
            sent_method     TEXT CHECK(sent_method IN ('email','mail','both',NULL)),
            company_id      TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_nonprofitclaw_trec_donor ON nonprofitclaw_tax_receipt(donor_id);
        CREATE INDEX IF NOT EXISTS idx_nonprofitclaw_trec_year ON nonprofitclaw_tax_receipt(tax_year);
    """)

    conn.commit()
    conn.close()
    print(f"NonprofitClaw tables created in {db_path}")


if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB_PATH
    parent = os.path.dirname(db_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    create_nonprofitclaw_tables(db_path)
