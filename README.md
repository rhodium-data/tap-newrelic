# tap-newrelic-apirequest

Singer tap for New Relic `ApiRequestEvent` via the NerdGraph (NRQL) API.

## Installation

```bash
pip install git+https://github.com/rhodium-data/tap-newrelic-apirequest.git
```

## Configuration

| Setting | Env var | Required | Description |
|---|---|---|---|
| `account_id` | `NEW_RELIC_ACCOUNT_ID` | yes | NR account ID |
| `api_key` | `NEW_RELIC_API_KEY` | yes | NR User API key |
| `start_time` | — | no | ISO8601, default 1h ago |
| `end_time` | — | no | ISO8601, default now |
| `query` | — | no | Base NRQL, default `SELECT * FROM ApiRequestEvent` |
| `stream_name` | — | no | Singer stream name, default `api_request_event` |

Set credentials via environment variables:

```bash
export NEW_RELIC_ACCOUNT_ID=your_account_id
export NEW_RELIC_API_KEY=NRAK-...
```

## Usage

```bash
# Singer mode (default) — pipe to a target
tap-newrelic-apirequest | target-redshift

# With state (incremental)
tap-newrelic-apirequest --state state.json | target-redshift

# Discover mode — output Singer catalog
tap-newrelic-apirequest --discover

# Custom time window
tap-newrelic-apirequest --start-time 2025-01-01T00:00:00Z --end-time 2025-01-02T00:00:00Z

# NDJSON output (no Singer protocol)
tap-newrelic-apirequest --mode ndjson
```

## Meltano

```yaml
plugins:
  extractors:
  - name: tap-newrelic-apirequest
    namespace: tap_newrelic_apirequest
    pip_url: git+https://github.com/rhodium-data/tap-newrelic-apirequest.git
    capabilities:
      - state
      - catalog
      - discover
    config:
      account_id: ${NEW_RELIC_ACCOUNT_ID}
      api_key: ${NEW_RELIC_API_KEY}
```

## Known Limitations

- **Catch-up runs (>100k records)**: pipelinewise target-redshift triggers a mid-stream batch flush at 100k rows, before the tap's final STATE message is received. This causes `AttributeError: 'NoneType'.get()` in the target. Normal hourly runs (~2k records) are unaffected. For catch-up scenarios use a narrow `start_time`/`end_time` window to stay under 100k records per run.

## How it works

Pulls events using NRQL `SINCE`/`UNTIL` time windows. When a window returns
the NRQL `LIMIT MAX` (5000 rows), it bisects the window and retries each half.
Bisection stops at 0.5 seconds — if a sub-second window still hits the cap, a
warning is printed and available rows are emitted.

Bookmark tracks the highest `timestamp` seen, not the run's `end_time`, so
late-arriving events are caught on the next run.
