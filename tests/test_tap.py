"""Basic tests for tap-newrelic-apirequest."""

from datetime import datetime, timezone

import pytest

from tap_newrelic_apirequest import parse_iso8601, nrql_ts


def test_parse_iso8601_z_suffix():
    dt = parse_iso8601("2025-05-01T12:00:00Z")
    assert dt.tzinfo is not None
    assert dt.year == 2025 and dt.hour == 12


def test_parse_iso8601_offset():
    dt = parse_iso8601("2025-05-01T12:00:00+00:00")
    assert dt == datetime(2025, 5, 1, 12, 0, 0, tzinfo=timezone.utc)


def test_parse_iso8601_naive_assumes_utc():
    dt = parse_iso8601("2025-05-01T12:00:00")
    assert dt.tzinfo is not None


def test_nrql_ts_format():
    dt = datetime(2025, 5, 1, 14, 30, 45, tzinfo=timezone.utc)
    assert nrql_ts(dt) == "2025-05-01 14:30:45"


def test_nrql_ts_drops_subseconds():
    dt = datetime(2025, 5, 1, 14, 30, 45, 123456, tzinfo=timezone.utc)
    assert nrql_ts(dt) == "2025-05-01 14:30:45"
