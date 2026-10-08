# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`tap-fmp` is a Singer tap for the [Financial Modeling Prep](https://financialmodelingprep.com) API, built with the [Meltano Singer SDK](https://sdk.meltano.com). It exposes ~258 concrete streams across search, directory, analyst, calendar, company, statements, 13-F, charts, indices, insider trades, market performance, commodities, crypto, forex, news, technical indicators, senate/house trades, SEC filings, COT, economics, ETFs/mutual funds, ESG, earnings transcripts, bulk, and quote families.

## Development Commands

```bash
# Install dependencies
uv sync

# Lint (black + flake8) — must pass before any work is considered done
./lint.sh

# Unit tests (mocked, fast)
uv run pytest

# Run the tap directly
uv run tap-fmp --help
uv run tap-fmp --about
```

**Always use `uv`.** Never run `python`, `pip`, or `poetry` directly — respect the package manager.

## Critical Universal Rules

**Epistemic Honesty**: Never guess. If the information isn't in your context, say "I do not know" or use a tool to fetch it. If you still don't know after fetching, say "I do not know and could not find anything after attempting to fetch."

**DRY and SOLID**: Do not repeat yourself. This codebase leans heavily on base classes and mixins — find the existing abstraction before writing a new one.

**No Silent Failures**: If a command fails, report the failure immediately. You are forbidden from pretending it worked or suppressing the error.

**Evidence-Based Coding**: Verify a file exists before editing it. Read neighboring streams before writing a new one.

**Failure is acceptable**: It is fine to fail if the request is impossible or you lack high confidence. Do not force a success state by modifying tests, deleting checks, or writing trivially-passing tests. Tests must represent real-world cases and edge cases.

**Report cheating opportunities**: If you find a way to satisfy a request technically but deceptively (hardcoding a response, weakening an assertion), flag it as misalignment and ask for clarification.

## Architecture

### Class Hierarchy

Everything inherits from **`FmpRestStream`** (`tap_fmp/client.py:39`), an ABC extending Singer SDK's `RESTStream`. It owns API-key auth, pagination (`_handle_pagination`), throttling, backoff/retry, camelCase→snake_case key conversion, schema-drift detection, and replication-key formatting.

Key subclasses in `client.py`:

| Class | Purpose |
|---|---|
| `FmpSurrogateKeyStream` | Sets `primary_keys = ["surrogate_key"]` and `_add_surrogate_key = True`. **See the PK warning below before using.** |
| `BaseSymbolPartitionStream` | Partitions over a symbol list (`BaseSymbolPartitionMixin`) |
| `CompanySymbolPartitionStream` | Partitions over cached company symbols |
| `SymbolPeriodPartitionStream` | Symbol × period (annual/quarter) fan-out |
| `TimeSliceStream` | `INCREMENTAL` on `date`; chunks the window via `create_time_slice_chunks`, detects silent API truncation (`TruncationReason`) |
| `BaseSymbolPartitionTimeSliceStream` / `CompanySymbolPartitionTimeSliceStream` | Symbol partitioning + time slicing |
| `IncrementalDateStream` | `INCREMENTAL` on `date`, day-by-day iteration |
| `IncrementalYearStream` | `INCREMENTAL` on `year` |
| `BaseSymbolYearPartitionStream` → `SymbolYearQuarterPartitionStream`, `SymbolYearPeriodPartitionStream` | Symbol × year (× quarter/period) fan-out |

### Mixins (`tap_fmp/mixins.py`)

Composition is the norm — most concrete streams are `class X(SomeMixin, SomeBaseStream)`.

- **Selection**: `SelectableStreamMixin`, `BaseConfigMixin` and its per-asset subclasses (`CommodityConfigMixin`, `CryptoConfigMixin`, `EtfConfigMixin`, `IndexConfigMixin`, `CompanyConfigMixin`, `FinancialStatementConfigMixin`, `ForexConfigMixin`, `CikConfigMixin`, `ExchangeConfigMixin`) — these read the `select_*` config keys.
- **Partitioning**: `BaseSymbolPartitionMixin`, `CompanySymbolPartitionMixin`, `FinancialStatementSymbolPartitionMixin`, `TranscriptSymbolPartitionMixin`, `BatchSymbolPartitionMixin`.
- **Price schemas**: `BasePriceSchemaMixin`, `BaseIntervalPriceSchemaMixin`, `BaseAdjustedPriceSchemaMixin`, `ChartLightMixin`, `ChartFullMixin`, `Prices1minMixin` … `Prices4HrMixin`. These carry both the schema and the `get_url` override, so an interval stream is usually a two-line class.

### Stream Modules

Concrete streams live in `tap_fmp/streams/<family>_streams.py` (e.g. `statements_streams.py`, `company_streams.py`, `bulk_streams.py`). A concrete stream typically sets `name`, `schema`, and `get_url`, inheriting everything else.

```python
class EsgDisclosuresStream(EsgStream):
    name = "esg_disclosures"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("environmental_score", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/esg-disclosures"
```

### Registering a New Stream

Three places, all required:

1. Import it in `tap_fmp/tap.py` (grouped by family).
2. Add it to `discover_streams()` in `tap_fmp/tap.py:862`, under the matching `### Family ###` comment block. Note the `# fmt: off` — preserve the existing formatting.
3. Add `<stream_name>.*` to the `select:` list in `meltano.yml`, plus a `config:` block if it needs `path_params` / `query_params` / `other_params`.

## Endpoint Version Prefix

**Use `/stable/` only.** Every `get_url` in this codebase builds `f"{self.url_base}/stable/<endpoint>"`. Never use `/v3/`, `/v4/`, or `/api/v3/` — those are legacy endpoints that now return
`403 "Legacy Endpoint : ... only available for legacy users who have valid subscriptions prior August 31, 2025"`.

`url_base` defaults to `https://financialmodelingprep.com` and is overridable via the `base_url` config key.

## Code Conventions

- **Field names are snake_case.** `clean_json_keys` (`helpers.py:120`) converts the API's camelCase automatically in `post_process`; declare schema properties in snake_case to match.
- **Declare schemas explicitly.** Never auto-discover. Use `th.PropertiesList(...).to_dict()` with `th.StringType`, `th.IntegerType`, `th.NumberType`, `th.BooleanType`, `th.DateType`, `th.DateTimeType`, `th.ArrayType`, `th.ObjectType`.
- **All imports at the top of the module.** Never `import` inside a function or method body.
- **Line length 128** (`.flake8`). `black` reformats; `flake8` ignores `F405, F403, W503, E203, E266`.
- **Pagination**: set `_paginate = True`. FMP caps paginated endpoints at page index 100 inclusive — those streams set `_max_pages = 100`. See README § "Endpoint Limits & Pagination Reference" for the per-stream table.
- **Schema drift is monitored, not fatal.** `_check_missing_fields` logs `*** SCHEMA_DRIFT ***` at ERROR once per unique (stream, missing-field-set); a Dagster sensor watches for that token. Adding a field the API already returns is how you clear a drift alert.

## Primary Key & Correctness Rules

**Correctness is paramount. Never silently truncate, drop, or misrepresent data.** When in doubt, stop and ask rather than guess.

### Known defect: the whole-row surrogate key

`generate_surrogate_key(record)` (`helpers.py:141`) hashes **every field in the record**. `FmpRestStream.post_process` (`client.py:386`) writes that hash to `record["surrogate_key"]` when `_add_surrogate_key = True`, and most streams key on it. Because the loader upserts on `primary_keys`, **any** volatile field (price, volume, market_cap, ratios, scores) changing between syncs produces a new PK and inserts a duplicate row instead of updating it. This affects a large share of existing streams.

`docs/PRIMARY_KEY_AUDIT.md` is the authoritative per-stream remediation plan — read the relevant family table before touching a stream's PK.

### Choosing a primary key for new or edited streams

1. **Prefer a natural composite PK** from the API's real identity fields — entity + time-period + variant axes (e.g. `["symbol", "date"]`, `["symbol", "fiscal_year", "period"]`).
2. **Inspect a multi-record sample**, not the docs alone. Look for any field combination that repeats across two rows.
3. **Coerce nullable PK fields in `post_process`** rather than reaching for a surrogate.
4. **Do not add a whole-row `surrogate_key` to a new stream.** If a surrogate is genuinely unavoidable, hash only the stable identity fields — never prices, volumes, or fetch timestamps — or re-fetches of the same logical row produce different keys and dedup breaks.
5. `surrogate_key` is `required=True` in most schemas, so removing the column means deleting the `th.Property` **and** clearing `_add_surrogate_key`, or the required check fails.

### No silent failures

- Never return partial data without surfacing it. If pagination breaks, raise.
- Never drop fields the API returns just because they aren't in the schema.
- Never invent fields absent from the API response or docs. Derive explicitly in `post_process` if needed.
- If you cannot determine the correct schema or PK from the docs plus a real sample, stop and report rather than guess.

## Testing Streams

Unit tests (`tests/`) are mocked and fast — run `uv run pytest`. For anything touching a real endpoint, use Meltano instead:

```bash
mkdir -p /tmp/tap-fmp-logs
meltano el tap-fmp target-jsonl --force --select "<stream_name>.*" 2>/tmp/tap-fmp-logs/<stream_name>.log &
PID=$!; sleep 30; kill $PID 2>/dev/null
```

Then verify:

```bash
wc -l output/<stream_name>.jsonl                              # record count
head -1 output/<stream_name>.jsonl | python3 -m json.tool     # inspect first record
grep -i "SCHEMA_DRIFT\|DATA_TRUNCATED\|error\|traceback" /tmp/tap-fmp-logs/<stream_name>.log
```

Each stream needs its own `meltano` invocation with `--force` (meltano locks the pipeline). To test several streams, launch them in parallel as separate Bash calls.

## FMP Documentation Access

**The FMP docs site 403s automated requests that don't send a browser User-Agent.** When fetching docs or the changelog with `curl`, pass one:

```bash
curl -s -A "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36" \
  "https://site.financialmodelingprep.com/developer/docs/changelog"
```

The changelog page is a Next.js app; entries live in the `__NEXT_DATA__` script tag at `.props.pageProps.logs` as `{date, text, url, detail, type}` objects with ISO dates.

## Automated Changelog Monitoring

`.github/workflows/monitor-fmp-changelog.yml` runs weekly (Mondays 09:00 UTC) and on `workflow_dispatch`:

1. Scrapes the FMP changelog and selects entries newer than the lookback window.
2. If there are any, runs Claude Code with the repo checked out.
3. Claude reads `formatted-entries.md` + this file, updates schemas/streams, runs `./lint.sh`, and opens a PR.

No issue is created and no manual comment is needed — detection to PR is one job.

**Required secret**: `CLAUDE_TAP_FMP_API_KEY` (Claude Code OAuth token).
