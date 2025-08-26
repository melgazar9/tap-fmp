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

For example, you can configure the `stock_screener` stream to only return stocks with a volume greater than 1,000,000 and a limit of 1000 results:

```yaml
      stock_screener:
        query_params:
          volumeMoreThan: 1000000
          limit: 1000
          exchanges: ["NASDAQ", "NYSE", "AMEX", "TSX", "LSE", "NEO"]
```

You can also configure date ranges, limits, and other parameters for most streams. Please refer to the `meltano.yml` file for a full list of available options for each stream.

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
