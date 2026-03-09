---
name: nonprofitclaw
version: 1.0.0
description: "Non-Profit Management -- donors, donations, grants, funds, volunteers, campaigns. 58 actions across 8 domains. Built on ERPClaw foundation."
author: AvanSaber
homepage: https://github.com/avansaber/nonprofitclaw
source: https://github.com/avansaber/nonprofitclaw
tier: 4
category: nonprofit
requires: [erpclaw]
database: ~/.openclaw/erpclaw/data.sqlite
user-invocable: true
tags: [nonprofitclaw, nonprofit, donors, donations, grants, funds, volunteers, campaigns, pledges, tax-receipts]
scripts:
  - scripts/db_query.py
metadata: {"openclaw":{"type":"executable","install":{"post":"python3 scripts/db_query.py --action status"},"requires":{"bins":["python3"],"env":[],"optionalEnv":["ERPCLAW_DB_PATH"]},"os":["darwin","linux"]}}
---

# NonprofitClaw — Non-Profit Management

Complete non-profit operations: donor management, fund accounting,
grant tracking, volunteer coordination, campaigns, and compliance.

## Tier 1 — Core Non-Profit

Manage donors and donations, create funds (unrestricted/restricted),
track grants, coordinate volunteers, and run fundraising campaigns.

## Tier 2 — Fund Accounting & Compliance

Fund transfers, grant expense tracking with approval workflow,
pledge management, tax receipt generation, 990 data preparation.

## Tier 3 — Analytics

Donor giving history, fund balance reports, grant utilization,
volunteer hours, campaign progress, and donor summary analytics.
