---
name: nonprofitclaw
version: "1.0.0"
description: "Non-Profit Management — donors, donations, grants, funds, volunteers, campaigns"
author: "avansaber"
scripts:
  - scripts/db_query.py
actions:
  # Donors
  - nonprofit-add-donor
  - nonprofit-update-donor
  - nonprofit-list-donors
  - nonprofit-get-donor
  - nonprofit-donor-giving-history
  - nonprofit-merge-donors
  - nonprofit-import-donors
  # Donations
  - nonprofit-add-donation
  - nonprofit-update-donation
  - nonprofit-list-donations
  - nonprofit-get-donation
  - nonprofit-refund-donation
  # Funds
  - nonprofit-add-fund
  - nonprofit-update-fund
  - nonprofit-list-funds
  - nonprofit-get-fund
  - nonprofit-add-fund-transfer
  - nonprofit-list-fund-transfers
  - nonprofit-approve-fund-transfer
  - nonprofit-fund-balance-report
  # Grants
  - nonprofit-add-grant
  - nonprofit-update-grant
  - nonprofit-list-grants
  - nonprofit-get-grant
  - nonprofit-add-grant-expense
  - nonprofit-list-grant-expenses
  - nonprofit-approve-grant-expense
  - nonprofit-grant-status-report
  - nonprofit-close-grant
  - nonprofit-activate-grant
  # Programs
  - nonprofit-add-program
  - nonprofit-update-program
  - nonprofit-list-programs
  - nonprofit-get-program
  - nonprofit-update-program-outcomes
  # Volunteers
  - nonprofit-add-volunteer
  - nonprofit-update-volunteer
  - nonprofit-list-volunteers
  - nonprofit-get-volunteer
  - nonprofit-add-volunteer-shift
  - nonprofit-list-volunteer-shifts
  - nonprofit-complete-volunteer-shift
  - nonprofit-volunteer-hours-report
  # Campaigns
  - nonprofit-add-campaign
  - nonprofit-update-campaign
  - nonprofit-list-campaigns
  - nonprofit-get-campaign
  - nonprofit-activate-campaign
  - nonprofit-close-campaign
  # Pledges
  - nonprofit-add-pledge
  - nonprofit-list-pledges
  - nonprofit-get-pledge
  - nonprofit-fulfill-pledge
  - nonprofit-cancel-pledge
  # Tax Receipts
  - nonprofit-generate-tax-receipt
  - nonprofit-list-tax-receipts
  # Reports
  - nonprofit-donor-summary
  - nonprofit-status
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
