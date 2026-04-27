---
name: nonprofitclaw
version: 1.0.0
description: Non-Profit Management -- 57 actions across 7 domains. Donor management, donations, pledges, fund accounting, grants, volunteers, campaigns, tax receipts, and compliance.
author: AvanSaber
homepage: https://github.com/avansaber/nonprofitclaw
source: https://github.com/avansaber/nonprofitclaw
tier: 4
category: nonprofit
requires: [erpclaw]
database: ~/.openclaw/erpclaw/data.sqlite
user-invocable: true
tags: [nonprofitclaw, nonprofit, donors, donations, grants, funds, volunteers, campaigns, pledges, tax-receipts, fund-accounting, compliance]
scripts:
  - scripts/db_query.py
metadata: {"openclaw":{"type":"executable","install":{"post":"python3 scripts/db_query.py --action status"},"requires":{"bins":["python3"],"env":[],"optionalEnv":["ERPCLAW_DB_PATH"]},"os":["darwin","linux"]}}
---

# nonprofitclaw

Non-Profit Operations Manager for NonprofitClaw -- AI-native nonprofit management on ERPClaw.
Manages donors, donations, pledges, fund accounting (unrestricted/restricted/endowment),
grants with expense tracking and approval workflow, volunteer coordination with shift management,
fundraising campaigns, tax receipt generation, donor analytics, and compliance reporting.
All financials post to ERPClaw GL with double-entry accounting.

### Skill Activation Triggers

Activate when user mentions: nonprofit, non-profit, donor, donation, pledge, grant, fund,
volunteer, campaign, fundraising, tax receipt, endowment, restricted fund, unrestricted,
fund transfer, grant expense, 990, charitable, giving.

### Setup
```
python3 {baseDir}/../erpclaw/scripts/erpclaw-setup/db_query.py --action initialize-database
python3 {baseDir}/init_db.py
python3 {baseDir}/scripts/db_query.py --action status
```

## Quick Start
```
--action nonprofit-add-donor --company-id {id} --first-name "Jane" --last-name "Smith" --email "jane@example.com"
--action nonprofit-add-donation --company-id {id} --donor-id {id} --amount "500.00" --donation-date "2026-01-15"
--action nonprofit-add-fund --company-id {id} --name "General Fund" --fund-type unrestricted
--action nonprofit-add-grant --company-id {id} --grantor-name "State Foundation" --amount "50000.00"
--action nonprofit-add-volunteer --company-id {id} --first-name "Bob" --last-name "Jones"
--action nonprofit-generate-tax-receipt --donation-id {id}
```

## All 57 Actions

### Donors & Donations (14 actions)
| Action | Description |
|--------|-------------|
| `nonprofit-add-donor` | Add donor |
| `nonprofit-update-donor` | Update donor info |
| `nonprofit-get-donor` | Get donor details |
| `nonprofit-list-donors` | List donors |
| `nonprofit-merge-donors` | Merge duplicate donors |
| `nonprofit-import-donors` | Import donors from CSV |
| `nonprofit-add-donation` | Record donation |
| `nonprofit-update-donation` | Update donation |
| `nonprofit-get-donation` | Get donation details |
| `nonprofit-list-donations` | List donations |
| `nonprofit-refund-donation` | Refund donation |
| `nonprofit-generate-tax-receipt` | Generate tax receipt |
| `nonprofit-list-tax-receipts` | List tax receipts |
| `nonprofit-donor-summary` | Donor analytics summary |

### Pledges (5 actions)
| Action | Description |
|--------|-------------|
| `nonprofit-add-pledge` | Create pledge commitment |
| `nonprofit-get-pledge` | Get pledge details |
| `nonprofit-list-pledges` | List pledges |
| `nonprofit-fulfill-pledge` | Record pledge fulfillment |
| `nonprofit-cancel-pledge` | Cancel pledge |

### Funds (6 actions)
| Action | Description |
|--------|-------------|
| `nonprofit-add-fund` | Create fund |
| `nonprofit-update-fund` | Update fund |
| `nonprofit-get-fund` | Get fund details |
| `nonprofit-list-funds` | List funds |
| `nonprofit-add-fund-transfer` | Transfer between funds |
| `nonprofit-approve-fund-transfer` | Approve fund transfer |

### Grants (8 actions)
| Action | Description |
|--------|-------------|
| `nonprofit-add-grant` | Create grant |
| `nonprofit-update-grant` | Update grant |
| `nonprofit-get-grant` | Get grant details |
| `nonprofit-list-grants` | List grants |
| `nonprofit-activate-grant` | Activate grant |
| `nonprofit-close-grant` | Close grant |
| `nonprofit-add-grant-expense` | Record grant expense |
| `nonprofit-approve-grant-expense` | Approve grant expense |

### Volunteers (6 actions)
| Action | Description |
|--------|-------------|
| `nonprofit-add-volunteer` | Add volunteer |
| `nonprofit-update-volunteer` | Update volunteer |
| `nonprofit-get-volunteer` | Get volunteer details |
| `nonprofit-list-volunteers` | List volunteers |
| `nonprofit-add-volunteer-shift` | Schedule volunteer shift |
| `nonprofit-complete-volunteer-shift` | Complete volunteer shift |

### Campaigns (5 actions)
| Action | Description |
|--------|-------------|
| `nonprofit-add-campaign` | Create fundraising campaign |
| `nonprofit-update-campaign` | Update campaign |
| `nonprofit-get-campaign` | Get campaign details |
| `nonprofit-list-campaigns` | List campaigns |
| `nonprofit-activate-campaign` | Activate campaign |

### Programs (4 actions)
| Action | Description |
|--------|-------------|
| `nonprofit-add-program` | Create program |
| `nonprofit-update-program` | Update program |
| `nonprofit-get-program` | Get program details |
| `nonprofit-list-programs` | List programs |

### Reports & Analytics (9 actions)
| Action | Description |
|--------|-------------|
| `nonprofit-donor-giving-history` | Donor giving history |
| `nonprofit-fund-balance-report` | Fund balance report |
| `nonprofit-grant-status-report` | Grant status report |
| `nonprofit-volunteer-hours-report` | Volunteer hours report |
| `nonprofit-close-campaign` | Close campaign |
| `nonprofit-list-fund-transfers` | List fund transfers |
| `nonprofit-list-grant-expenses` | List grant expenses |
| `nonprofit-list-volunteer-shifts` | List volunteer shifts |
| `nonprofit-update-program-outcomes` | Update program outcomes |

## Technical Details (Tier 3)
**Tables:** All use `nonprofitclaw_` prefix. **Script:** `scripts/db_query.py` routes to 7 modules. **Data:** Money=TEXT(Decimal), IDs=TEXT(UUID4). **Fund types:** unrestricted, temporarily_restricted, permanently_restricted, endowment.
