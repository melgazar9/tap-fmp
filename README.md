# tap-fmp

`tap-fmp` is a Singer tap for FMP.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

<!--

Developer TODO: Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPI repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

## Installation

Install from PyPI:

```bash
pipx install tap-fmp
```

Install from GitHub:

```bash
pipx install git+https://github.com/ORG_NAME/tap-fmp.git@main
```

-->

## Configuration

### Accepted Config Options

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-fmp --about
```

### General Configuration

The following configuration options are available for the `tap-fmp` extractor:

*   **`api_key`**: Your FMP API key. This is a required setting.
*   **`start_date`**: The initial date to start extracting data from. This should be in `YYYY-MM-DDTHH:mm:ssZ` format.
*   **`exchange_variants_source`**: Configuration for how to source exchange variants data. You can use a CSV file or a database.
*   **`database_config`**: Database connection settings for exchange variants.

### Symbol and List Configuration

The tap allows you to specify lists of symbols for various streams. This is useful for focusing your data extraction on a specific set of securities.

*   **`symbols`**: A list of stock symbols to extract data for.
*   **`index_symbols`**: A list of index symbols to extract data for.
*   **`etf_symbols`**: A list of ETF symbols to extract data for.
*   **`ciks`**: A list of CIKs (Central Index Key) to extract data for.
*   **`exchanges`**: A list of exchanges to extract data for. Use `"*"` to select all exchanges.
*   **`sectors`**: A list of sectors to extract data for. Use `"*"` to select all sectors.
*   **`industries`**: A list of industries to extract data for. Use `"*"` to select all industries.
*   **`commodities`**: A list of commodity symbols to extract data for.
*   **`crypto_symbols`**: A list of cryptocurrency symbols to extract data for.
*   **`forex_pairs`**: A list of foreign exchange pairs to extract data for.
*   **`cot_symbols`**: A list of Commitment of Traders symbols to extract data for.

**Note on Filtering Logic:** When you specify multiple filters (e.g., `symbols`, `countries`, `currencies`), the tap will extract data that matches **any** of the specified filters. For example, if you specify a list of symbols and a list of countries, the tap will extract data for those symbols, as well as all data for the specified countries. The filtering logic is an "OR" condition, not an "AND" condition.

### Stream-Specific Configuration

You can also configure specific parameters for each stream. This allows you to customize the data extraction for each stream.

For example, you can configure the `company_screener` stream to only return stocks with a volume greater than 1,000,000 and a limit of 1000 results:

```yaml
      company_screener:
        query_params:
          volumeMoreThan: 1000000
          limit: 1000
          exchanges: ["NASDAQ", "NYSE", "AMEX", "TSX", "LSE", "NEO"]
```

You can also configure date ranges, limits, and other parameters for most streams. Please refer to the `meltano.yml` file for a full list of available options for each stream.

## Endpoint Limits & Pagination Reference

Every FMP endpoint that accepts `limit` and/or `page` has two independent caps:

- **Max limit** — the maximum records the API will return in a single request. Sending a higher value is silently truncated.
- **Max page (inclusive)** — the highest valid 0-indexed page. Pages `0..max_page` inclusive are accessible; `max_page + 1` returns HTTP 400.

The tap's `_max_pages` attribute represents the **inclusive** maximum page index (matching FMP's "Page maxed at N" semantics). The pagination loop iterates `while page <= _max_pages`, so `_max_pages = 100` fetches pages 0 through 100 (101 total).

**Source legend**
- `docs` — FMP's public API docs state the cap explicitly ("Maximum N records per request" and/or "Page maxed at N")
- `empirical` — verified by direct API probe (limit/page pushed beyond expected cap; observed the silent truncation or HTTP 400 threshold)
- `docs + empirical` — docs value confirmed by probe

### Streams with pagination (`_max_pages = 100`, pages 0..100 inclusive)

| Stream | Endpoint | Max limit | Max page | Source |
|---|---|---:|---:|---|
| `latest_earning_transcripts` | `/stable/earning-call-transcript-latest` | 100 | 100 | docs + empirical |
| `latest_financial_statements` | `/stable/latest-financial-statements` | 250 | 100 | docs + empirical |
| `latest_insider_trading` | `/stable/insider-trading/latest` | 1000 | 100 | docs + empirical |
| `insider_trades_search` | `/stable/insider-trading/search` | 1000 | 100 | docs + empirical |
| `institutional_ownership_latest_filings` | `/stable/institutional-ownership/latest` | 1000 | 100 | empirical (docs only stated page max) |
| `latest_8k_filings` | `/stable/sec-filings-8k` | 1000 | 100 | docs + empirical |
| `latest_sec_filings` | `/stable/sec-filings-financials` | 1000 | 100 | docs + empirical |
| `sec_filings_by_cik` | `/stable/sec-filings-search/cik` | 1000 | 100 | docs + empirical |
| `sec_filings_by_form_type` | `/stable/sec-filings-search/form-type` | 1000 | 100 | docs + empirical |
| `sec_filings_by_symbol` | `/stable/sec-filings-search/symbol` | 1000 | 100 | docs + empirical |
| `latest_senate_disclosures` | `/stable/senate-latest` | 250 | 100 | docs + empirical |
| `latest_house_disclosures` | `/stable/house-latest` | 250 | 100 | docs + empirical |
| `crypto_news` | `/stable/news/crypto` | 250 | 100 | docs + empirical |
| `crypto_news_latest` | `/stable/news/crypto-latest` | 250 | 100 | docs + empirical |
| `forex_news` | `/stable/news/forex` | 250 | 100 | docs + empirical |
| `forex_news_latest` | `/stable/news/forex-latest` | 250 | 100 | docs + empirical |
| `general_news` | `/stable/news/general-latest` | 250 | 100 | docs + empirical |
| `press_releases` | `/stable/news/press-releases` | 250 | 100 | docs + empirical |
| `press_releases_latest` | `/stable/news/press-releases-latest` | 250 | 100 | docs + empirical |
| `stock_news` | `/stable/news/stock` | 250 | 100 | docs + empirical |
| `stock_news_latest` | `/stable/news/stock-latest` | 250 | 100 | docs + empirical |

### Streams that paginate but have no documented page cap

Pagination proceeds until the API returns empty or the tap's default `_max_pages` fallback triggers. Records silently cap at the listed `limit` per request.

| Stream | Endpoint | Max limit | Source |
|---|---|---:|---|
| `company_screener` | `/stable/company-screener` | 10000 | empirical (docs URL example showed 1000; API accepts up to 10000) |
| `cik_list` | `/stable/cik-list` | 10000 | docs + empirical |
| `fmp_articles` | `/stable/fmp-articles` | 200 | empirical (docs URL example showed 20; API caps silently at 200) |
| `latest_crowdfunding_campaigns` | `/stable/crowdfunding-offerings-latest` | 1000 | empirical |
| `equity_offering_updates` | `/stable/fundraising-latest` | 100 | empirical |
| `form_13f_filings_extracts_with_analytics` | `/stable/institutional-ownership/extract-analytics/holder` | 100 | empirical |
| `holder_performance_summary` | `/stable/institutional-ownership/holder-performance-summary` | N/A (no `limit` param; paginate-only) | docs |
| `delisted_companies` | `/stable/delisted-companies` | 100 | docs + empirical |
| `all_shares_float` | `/stable/shares-float-all` | 5000 | docs + empirical |
| `latest_mergers_and_acquisitions` | `/stable/mergers-acquisitions-latest` | 1000 | docs |

### Non-paginated streams with an explicit `limit` cap

These stream classes do not paginate (`_paginate = False`); the tap makes a single request and the API caps records at the values shown.

| Stream | Endpoint | Max limit | Source |
|---|---|---:|---|
| `symbol_changes` | `/stable/symbol-change` | ~5281 (full universe; `page` is silently ignored) | empirical |
| `analyst_estimates` | `/stable/analyst-estimates` | 1000 | docs |
| `historical_ratings` | `/stable/ratings-historical` | 10000 | docs |
| `historical_stock_grades` | `/stable/grades-historical` | 1000 | docs |
| `dividends_calendar` | `/stable/dividends-calendar` | 4000 | docs |
| `dividends_company` | `/stable/dividends` | 1000 | docs |
| `earnings_report` | `/stable/earnings` | 1000 | docs |
| `earnings_calendar` | `/stable/earnings-calendar` | 4000 | docs |
| `stock_split_details` | `/stable/splits` | 1000 | docs |
| `stock_splits_calendar` | `/stable/splits-calendar` | 4000 | docs |
| `historical_market_cap` | `/stable/historical-market-capitalization` | 5000 | docs + empirical |
| `company_employee_count` | `/stable/employee-count` | 10000 | docs |
| `company_historical_employee_count` | `/stable/historical-employee-count` | 10000 | docs |
| `rating_snapshot` | `/stable/ratings-snapshot` | 1 | docs |
| `income_statement` | `/stable/income-statement` | 1000 | docs |
| `balance_sheet` | `/stable/balance-sheet-statement` | 1000 | docs |
| `cash_flow` | `/stable/cash-flow-statement` | 1000 | docs |
| `income_statement_ttm` | `/stable/income-statement-ttm` | 1000 | docs |
| `balance_sheet_ttm` | `/stable/balance-sheet-statement-ttm` | 1000 | docs |
| `cash_flow_ttm` | `/stable/cash-flow-statement-ttm` | 1000 | docs |
| `income_statement_growth` | `/stable/income-statement-growth` | 1000 | docs |
| `balance_sheet_growth` | `/stable/balance-sheet-statement-growth` | 1000 | docs |
| `cash_flow_growth` | `/stable/cash-flow-statement-growth` | 1000 | docs |
| `financial_statement_growth` | `/stable/financial-growth` | 1000 | docs |
| `as_reported_income_statement` | `/stable/income-statement-as-reported` | 1000 | docs |
| `as_reported_balance_statement` | `/stable/balance-sheet-statement-as-reported` | 1000 | docs |
| `as_reported_cashflow_statement` | `/stable/cash-flow-statement-as-reported` | 1000 | docs |
| `as_reported_financial_statement` | `/stable/financial-statement-full-as-reported` | 1000 | docs |
| `financial_ratios` | `/stable/ratios` | 1000 | docs |
| `key_metrics` | `/stable/key-metrics` | 1000 | docs |
| `enterprise_values` | `/stable/enterprise-values` | 1000 | docs |
| `revenue_product_segmentation` | `/stable/revenue-product-segmentation` | 1000 | docs |
| `revenue_geographic_segmentation` | `/stable/revenue-geographic-segmentation` | 1000 | docs |

### Historical OHLC streams (`/stable/historical-price-eod/*`)

These endpoints return up to **5000 records per request** (docs) and do **not** accept a `limit` query parameter. The tap windows by `from`/`to` (via `TimeSliceStream`) to stay under the 5000-row cap.

Affected streams: `company_chart_full`, `company_chart_light`, `company_dividend_adjusted_prices`, `company_unadjusted_price`, `historical_crypto_full_chart`, `historical_crypto_light_chart`, `forex_full_chart`, `forex_light_chart`, `historical_index_full_chart`, `historical_index_light_chart`, `commodities_full_chart`, `commodities_light_chart`.

### Streams with no applicable limit

All other streams not listed above either (a) return a fixed small result per query (e.g., per-symbol profile, current quote) or (b) have no documented/exercised `limit` cap. Setting `limit:` on these has no effect. See `meltano.yml` for the canonical per-stream configuration.

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

<!--
Developer TODO: If your tap requires special access on the source system, or any special authentication requirements, provide those here.
-->

## Usage

You can easily run `tap-fmp` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-fmp --version
tap-fmp --help
tap-fmp --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

Prerequisites:

- Python 3.9+
- [uv](https://docs.astral.sh/uv/)

```bash
uv sync
```

### Create and Run Tests

Create tests within the `tests` subfolder and
then run:

```bash
uv run pytest
```

You can also test the `tap-fmp` CLI interface directly using `uv run`:

```bash
uv run tap-fmp --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-fmp
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-fmp --version

# OR run a test ELT pipeline:
meltano run tap-fmp target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
