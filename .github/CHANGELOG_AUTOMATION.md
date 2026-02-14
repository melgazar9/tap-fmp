# FMP API Changelog Automation

Automated monitoring of FMP API changes with AI-powered analysis and PR creation.

## How It Works

```
Weekly (Monday 9 AM UTC)
  → GitHub Action checks FMP changelog API
  → New changes? → Creates GitHub Issue (triggers email)
  → You comment /analyze-changes on the issue
  → Claude reads changelog + all stream files
  → Claude creates branch + PR with schema updates
  → You review, test, merge
```

## Required Secrets

Add these in Repository Settings → Secrets → Actions:

| Secret | Source | Purpose |
|--------|--------|---------|
| `FMP_API_KEY` | https://site.financialmodelingprep.com/ | Fetch changelog API |
| `ANTHROPIC_API_KEY` | https://console.anthropic.com/ | Claude AI analysis |

## Email Notifications

1. Go to https://github.com/settings/notifications
2. Check "Issues" under email preferences
3. Watch this repository → Custom → Issues

## Workflows

### `monitor-fmp-changelog.yml`
- **Trigger**: Weekly cron (Monday 9 AM UTC) or manual
- **What it does**: Fetches FMP changelog API, checks for changes in the last 21 days
- **Output**: Creates a GitHub issue if new changes found (skips if open issue already exists)
- **Never pushes to main**

### `analyze-changelog-with-claude.yml`
- **Trigger**: Comment `/analyze-changes` on an issue with `fmp-api-update` label
- **What it does**:
  1. Reads all stream files in `tap_fmp/streams/`
  2. Sends changelog + full codebase context to Claude API
  3. Claude identifies affected schemas and generates updated files
  4. Creates a new branch and PR via GitHub API (never pushes to main)
- **Fallback**: If Claude's response can't be parsed as JSON, posts raw analysis as an issue comment

## Customization

### Change check frequency
Edit the cron in `monitor-fmp-changelog.yml`:
```yaml
schedule:
  - cron: '0 9 * * 1'      # Weekly Monday (default)
  - cron: '0 9 * * *'      # Daily
  - cron: '0 9 * * 1,5'    # Monday + Friday
```

### Change lookback window
Edit the `21 days ago` value in `monitor-fmp-changelog.yml`.

## Troubleshooting

- **No email**: Check GitHub notification settings + spam folder
- **Workflow not running**: Verify secrets are set and workflow is on `main` branch
- **Claude returns garbage**: Raw analysis is posted as an issue comment for manual review
- **Duplicate issues**: Workflow skips issue creation if one with `fmp-api-update` label is already open
