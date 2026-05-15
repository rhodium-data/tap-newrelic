# tap-newrelic

Singer tap for New Relic `ApiRequestEvent` via the NerdGraph (NRQL) API.

## Installation

```bash
pip install git+https://github.com/rhodium-data/tap-newrelic.git
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
tap-newrelic | target-redshift

# With state (incremental)
tap-newrelic --state state.json | target-redshift

# Custom time window
tap-newrelic --start-time 2025-01-01T00:00:00Z --end-time 2025-01-02T00:00:00Z

# NDJSON output (no Singer protocol)
tap-newrelic --mode ndjson
```

## Meltano

```yaml
plugins:
  extractors:
  - name: tap-newrelic
    namespace: tap_newrelic
    pip_url: git+https://github.com/rhodium-data/tap-newrelic.git
    capabilities:
      - state
    config:
      account_id: ${NEW_RELIC_ACCOUNT_ID}
      api_key: ${NEW_RELIC_API_KEY}
```

## Known Limitations

- **Catch-up runs (>100k records)**: pipelinewise target-redshift triggers a mid-stream batch flush at 100k rows, before the tap's final STATE message is received. This causes `AttributeError: 'NoneType'.get()` in the target. Normal hourly runs (~2k records) are unaffected. For catch-up scenarios use a narrow `start_time`/`end_time` window to stay under 100k records per run.
- **No `--discover` mode**: the tap does not support Singer's `--discover` flag or `catalog`/`discover` capabilities. Field selection via `meltano select`, strict schema-validating targets, and Singer ecosystem tooling that requires catalog discovery will not work. For single-stream, all-fields use cases (the intended deployment) this has no impact.

## How it works

Pulls events using NRQL `SINCE`/`UNTIL` time windows. When a window returns
the NRQL `LIMIT MAX` (5000 rows), it bisects the window and retries each half.
Bisection stops at 0.5 seconds — if a sub-second window still hits the cap, a
warning is printed and available rows are emitted.

Bookmark tracks the highest `timestamp` seen, not the run's `end_time`, so
late-arriving events are caught on the next run.
