#!/usr/bin/env python3
"""
tap-newrelic-apirequest
=======================

Pulls New Relic ApiRequestEvent rows via the NerdGraph (NRQL) API.

Two output modes:
  --mode singer  (default)  Singer-tap protocol: SCHEMA, RECORD, STATE messages
  --mode ndjson             one JSON object per line on stdout

Two ways to be configured:
  1. CLI flags / env vars (standalone use)
  2. --config FILE (Meltano / Singer convention)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


# -- Constants ---------------------------------------------------------------

NERDGRAPH_URL = "https://api.newrelic.com/graphql"
DEFAULT_QUERY = "SELECT * FROM ApiRequestEvent"
DEFAULT_STREAM_NAME = "api_request_event"
NRQL_LIMIT_MAX = 5000
MIN_BISECT_SECONDS = 0.5
HTTP_TIMEOUT_SECONDS = 60


# -- NerdGraph query template -------------------------------------------------

NERDGRAPH_QUERY_TEMPLATE = """
query($accountId: Int!, $nrql: Nrql!) {
  actor {
    account(id: $accountId) {
      nrql(query: $nrql) { results }
    }
  }
}
""".strip()


# -- Singer stream schema -----------------------------------------------------

# Typed schema for ApiRequestEvent based on Boron's
# `lambdas/v{0,1}/common/lib/boron_base_lambda.rb#send_newrelic_event`. Boron sends
# everything as a string (`.to_s`) except `timestamp` (added by NR as ms unix int).
# `additionalProperties: true` keeps NR-injected fields (entityGuid, appName, etc.)
# from failing schema validation in strict targets.
APIREQUEST_SCHEMA = {
    "type": "object",
    "additionalProperties": True,
    "properties": {
        "_row_id":            {"type": "string"},
        "timestamp":          {"type": "integer"},
        "eventType":          {"type": ["string", "null"]},
        "env":                {"type": ["string", "null"]},
        "account_id":         {"type": ["string", "null"]},
        "user_id":            {"type": ["string", "null"]},
        "company_id":         {"type": ["string", "null"]},
        "endpoint":           {"type": ["string", "null"]},
        "endpoint_version":   {"type": ["string", "null"]},
        "request_body":       {"type": ["string", "null"]},
        "query_parameters":   {"type": ["string", "null"]},
        "path_parameters":    {"type": ["string", "null"]},
        "aws_request_id":     {"type": ["string", "null"]},
        "request_id":         {"type": ["string", "null"]},
    },
}
APIREQUEST_KEY_PROPERTIES = ["_row_id"]
APIREQUEST_REPLICATION_KEY = "timestamp"


# -- Datetime utilities -------------------------------------------------------

def parse_iso8601(s: str) -> datetime:
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def nrql_ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


# -- HTTP / NerdGraph ---------------------------------------------------------

def make_session() -> requests.Session:
    """
    Session with retry/backoff for transient errors.

    - Retries: 5 total
    - Backoff: 2s, 4s, 8s, 16s, 32s (urllib3 backoff_factor=2 → 2 * 2**(n-1))
    - Status codes retried: 429, 500, 502, 503, 504. NR sets `Retry-After` on 429s
      and urllib3 honors it (overrides the backoff schedule).
    - Auth errors (401/403) and bad NRQL (400) are NOT retried — they fail fast.
    """
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("POST",),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def fetch_window(
    session: requests.Session,
    account_id: int,
    api_key: str,
    base_query: str,
    start: datetime,
    end: datetime,
) -> list[dict]:
    nrql = (
        f"{base_query} "
        f"SINCE '{nrql_ts(start)}' UNTIL '{nrql_ts(end)}' LIMIT MAX"
    )
    payload = {
        "query": NERDGRAPH_QUERY_TEMPLATE,
        "variables": {"accountId": account_id, "nrql": nrql},
    }
    r = session.post(
        NERDGRAPH_URL,
        json=payload,
        headers={"API-Key": api_key, "Content-Type": "application/json"},
        timeout=HTTP_TIMEOUT_SECONDS,
    )
    r.raise_for_status()
    body = r.json()
    if body.get("errors"):
        raise RuntimeError(f"NerdGraph errors: {body['errors']}")
    return body["data"]["actor"]["account"]["nrql"]["results"] or []


def stream_events(
    session: requests.Session,
    account_id: int,
    api_key: str,
    base_query: str,
    start: datetime,
    end: datetime,
) -> Iterator[dict]:
    """
    Yield events in [start, end).

    NRQL caps a `SELECT *` page at LIMIT MAX (5000) rows. When a window hits the
    cap, bisect it and try the halves. Bisecting stops at MIN_BISECT_SECONDS — if a
    sub-second window still hits the cap, warn and emit what we got (some events
    will be missed; raise the issue with NR or use a narrower base query).

    Stack is processed LIFO, so push the later half first to get chronological
    output.
    """
    stack: list[tuple[datetime, datetime]] = [(start, end)]
    while stack:
        s, e = stack.pop()
        if s >= e:
            continue
        rows = fetch_window(session, account_id, api_key, base_query, s, e)
        capped = len(rows) >= NRQL_LIMIT_MAX
        bisectable = (e - s).total_seconds() > MIN_BISECT_SECONDS
        if capped and bisectable:
            mid = s + (e - s) / 2
            stack.append((mid, e))
            stack.append((s, mid))
            continue
        if capped:
            print(
                f"WARNING: window {s.isoformat()}..{e.isoformat()} returned "
                f"LIMIT MAX ({NRQL_LIMIT_MAX}) and is too narrow to bisect; "
                "some events may be missing.",
                file=sys.stderr,
            )
        for row in rows:
            yield row


# -- Singer output ------------------------------------------------------------

def emit_ndjson(events: Iterator[dict]) -> tuple[int, int | None]:
    """Emit NDJSON. Returns (count, max_timestamp_ms_seen_or_None)."""
    n = 0
    max_ts: int | None = None
    for e in events:
        e["_row_id"] = e.get("aws_request_id") or e.get("request_id")
        ts = e.get("timestamp")
        if isinstance(ts, int) and (max_ts is None or ts > max_ts):
            max_ts = ts
        sys.stdout.write(json.dumps(e, separators=(",", ":")) + "\n")
        n += 1
    return n, max_ts


def emit_singer(
    events: Iterator[dict],
    stream_name: str,
    schema: dict[str, Any],
    key_properties: list[str],
    replication_key: str | None,
) -> tuple[int, int | None]:
    """Emit Singer messages. Returns (count, max_timestamp_ms_seen_or_None)."""
    schema_msg: dict[str, Any] = {
        "type": "SCHEMA",
        "stream": stream_name,
        "schema": schema,
        "key_properties": key_properties,
    }
    if replication_key:
        schema_msg["bookmark_properties"] = [replication_key]
    sys.stdout.write(json.dumps(schema_msg) + "\n")
    extracted_at = datetime.now(timezone.utc).isoformat()
    n = 0
    max_ts: int | None = None
    for e in events:
        e["_row_id"] = e.get("aws_request_id") or e.get("request_id")
        ts = e.get("timestamp")
        if isinstance(ts, int) and (max_ts is None or ts > max_ts):
            max_ts = ts
        sys.stdout.write(json.dumps({
            "type": "RECORD",
            "stream": stream_name,
            "record": e,
            "time_extracted": extracted_at,
        }) + "\n")
        n += 1
    return n, max_ts


def write_state(stream_name: str, bookmark_iso: str, path: str | None = None) -> None:
    state = {"bookmarks": {stream_name: {"replication_key_value": bookmark_iso}}}
    sys.stdout.write(json.dumps({"type": "STATE", "value": state}) + "\n")
    if path:
        with open(path, "w") as f:
            json.dump(state, f)


# -- Config & state I/O -------------------------------------------------------

def load_config_file(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    with open(path) as f:
        return json.load(f)


def load_state_file(path: str) -> dict[str, Any] | None:
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return None


# -- CLI ----------------------------------------------------------------------

_CONFIG_KEYS = {"account_id", "api_key", "start_time", "end_time", "query", "stream_name"}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Pull New Relic ApiRequestEvent rows via NerdGraph.")
    p.add_argument("--config",      help="Singer-style JSON config file")
    p.add_argument("--state",       help="Singer-style JSON state file (singer mode only)")
    p.add_argument("--account-id",  type=int, default=None, help="NR account ID (default: $NEW_RELIC_ACCOUNT_ID)")
    p.add_argument("--api-key",     default=None, help="NR User API key (default: $NEW_RELIC_API_KEY)")
    p.add_argument("--start-time",  default=None, help="ISO8601 (default: 1h ago UTC)")
    p.add_argument("--end-time",    default=None, help="ISO8601 (default: now UTC)")
    p.add_argument("--query",       default=None, help=f"Base NRQL (default: {DEFAULT_QUERY!r})")
    p.add_argument("--mode",        choices=("ndjson", "singer"), default="singer")
    p.add_argument("--stream-name", default=None, help=f"Singer stream name (default: {DEFAULT_STREAM_NAME!r})")
    return p.parse_args()


def resolve_config(args: argparse.Namespace) -> dict[str, Any]:
    """Build effective config: defaults → config file → env vars → CLI flags (highest priority)."""
    cfg: dict[str, Any] = {
        "query":       DEFAULT_QUERY,
        "stream_name": DEFAULT_STREAM_NAME,
    }
    cfg.update({k: v for k, v in load_config_file(args.config).items() if k in _CONFIG_KEYS and v is not None})
    if val := os.environ.get("NEW_RELIC_ACCOUNT_ID"):
        cfg["account_id"] = val
    if val := os.environ.get("NEW_RELIC_API_KEY"):
        cfg["api_key"] = val
    cfg.update({k: v for k, v in vars(args).items() if k in _CONFIG_KEYS and v is not None})
    return cfg


# -- Entry point --------------------------------------------------------------

def main() -> None:
    args = parse_args()
    cfg = resolve_config(args)

    if not cfg.get("account_id"):
        sys.exit("ERROR: account_id required (--account-id, $NEW_RELIC_ACCOUNT_ID, or --config)")
    if not cfg.get("api_key"):
        sys.exit("ERROR: api_key required (--api-key, $NEW_RELIC_API_KEY, or --config)")
    try:
        account_id = int(cfg["account_id"])
    except (TypeError, ValueError):
        sys.exit(f"ERROR: account_id must be an integer, got {cfg['account_id']!r}")

    end = parse_iso8601(cfg["end_time"]) if cfg.get("end_time") else datetime.now(timezone.utc)
    start = parse_iso8601(cfg["start_time"]) if cfg.get("start_time") else end - timedelta(hours=1)

    prior_bookmark_iso: str | None = None
    if args.mode == "singer" and args.state:
        state = load_state_file(args.state)
        if state:
            prior_bookmark_iso = (
                state.get("bookmarks", {})
                     .get(cfg["stream_name"], {})
                     .get("replication_key_value")
            )
            if prior_bookmark_iso:
                start = parse_iso8601(prior_bookmark_iso)
                print(f"Resumed from state bookmark {prior_bookmark_iso}", file=sys.stderr)

    if start >= end:
        sys.exit(f"ERROR: start ({start.isoformat()}) >= end ({end.isoformat()})")

    session = make_session()
    events = stream_events(session, account_id, cfg["api_key"], cfg["query"], start, end)

    # Apply the typed schema only when using the default ApiRequestEvent stream.
    # Custom queries (--query / --stream-name overrides) get an open schema.
    if args.mode == "singer" and cfg["stream_name"] == DEFAULT_STREAM_NAME:
        schema, key_properties, replication_key = (
            APIREQUEST_SCHEMA, APIREQUEST_KEY_PROPERTIES, APIREQUEST_REPLICATION_KEY
        )
    else:
        schema, key_properties, replication_key = (
            {"type": "object", "additionalProperties": True}, [], None
        )

    if args.mode == "singer":
        n, max_ts = emit_singer(events, cfg["stream_name"], schema, key_properties, replication_key)
    else:
        n, max_ts = emit_ndjson(events)

    # Bookmark = highest event timestamp seen, not the run's end_time. This way a
    # re-run from the bookmark catches late-arriving events that landed in NR after
    # this run finished. If no events were seen, leave the prior bookmark in place
    # (don't advance past data we never observed).
    if max_ts is not None:
        # Ceil to next whole second: nrql_ts() truncates sub-seconds, so truncating
        # the bookmark would re-fetch events in the last partial second on the next run.
        max_dt = datetime.fromtimestamp(max_ts / 1000.0, tz=timezone.utc)
        has_subseconds = max_dt.microsecond > 0
        ceiled = max_dt.replace(microsecond=0) + (timedelta(seconds=1) if has_subseconds else timedelta(0))
        bookmark_iso = ceiled.isoformat()
    elif prior_bookmark_iso:
        bookmark_iso = prior_bookmark_iso
    else:
        bookmark_iso = start.isoformat()

    print(
        f"Emitted {n} records from {start.isoformat()} to {end.isoformat()}; "
        f"new bookmark = {bookmark_iso}",
        file=sys.stderr,
    )

    if args.mode == "singer":
        write_state(cfg["stream_name"], bookmark_iso, path=args.state)


if __name__ == "__main__":
    main()
