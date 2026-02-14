"""Tests for ClaimX handler utilities."""

from datetime import UTC, date, datetime

from pipeline.claimx.handlers.utils import (
    elapsed_ms,
    inject_metadata,
    now_datetime,
    now_iso,
    parse_timestamp,
    parse_timestamp_dt,
    safe_bool,
    safe_decimal_str,
    safe_float,
    safe_int,
    safe_str,
    safe_str_id,
    today_date,
)

# ============================================================================
# safe_int
# ============================================================================


class TestSafeInt:
    def test_safe_int_returns_int_from_int(self):
        assert safe_int(42) == 42

    def test_safe_int_returns_int_from_string(self):
        assert safe_int("42") == 42

    def test_safe_int_returns_int_from_float(self):
        assert safe_int(42.9) == 42

    def test_safe_int_returns_none_for_none(self):
        assert safe_int(None) is None

    def test_safe_int_returns_none_for_non_numeric_string(self):
        assert safe_int("abc") is None

    def test_safe_int_returns_none_for_empty_string(self):
        assert safe_int("") is None

    def test_safe_int_returns_zero(self):
        assert safe_int(0) == 0

    def test_safe_int_returns_negative(self):
        assert safe_int(-5) == -5

    def test_safe_int_returns_int_from_bool(self):
        assert safe_int(True) == 1
        assert safe_int(False) == 0


# ============================================================================
# safe_str
# ============================================================================


class TestSafeStr:
    def test_safe_str_returns_string(self):
        assert safe_str("hello") == "hello"

    def test_safe_str_strips_whitespace(self):
        assert safe_str("  hello  ") == "hello"

    def test_safe_str_returns_none_for_none(self):
        assert safe_str(None) is None

    def test_safe_str_returns_none_for_empty_string(self):
        assert safe_str("") is None

    def test_safe_str_returns_none_for_whitespace_only(self):
        assert safe_str("   ") is None

    def test_safe_str_converts_int_to_string(self):
        assert safe_str(42) == "42"

    def test_safe_str_converts_float_to_string(self):
        assert safe_str(3.14) == "3.14"

    def test_safe_str_converts_bool_to_string(self):
        assert safe_str(True) == "True"


# ============================================================================
# safe_str_id
# ============================================================================


class TestSafeStrId:
    def test_safe_str_id_returns_string_for_string(self):
        assert safe_str_id("abc") == "abc"

    def test_safe_str_id_strips_whitespace(self):
        assert safe_str_id("  abc  ") == "abc"

    def test_safe_str_id_returns_none_for_none(self):
        assert safe_str_id(None) is None

    def test_safe_str_id_returns_none_for_empty_string(self):
        assert safe_str_id("") is None

    def test_safe_str_id_converts_int_without_decimal(self):
        assert safe_str_id(42) == "42"

    def test_safe_str_id_converts_float_to_int_string(self):
        assert safe_str_id(42.7) == "42"

    def test_safe_str_id_returns_none_for_whitespace_only(self):
        assert safe_str_id("   ") is None


# ============================================================================
# safe_bool
# ============================================================================


class TestSafeBool:
    def test_safe_bool_returns_none_for_none(self):
        assert safe_bool(None) is None

    def test_safe_bool_returns_true_for_true(self):
        assert safe_bool(True) is True

    def test_safe_bool_returns_false_for_false(self):
        assert safe_bool(False) is False

    def test_safe_bool_returns_true_for_string_true(self):
        assert safe_bool("true") is True

    def test_safe_bool_returns_true_for_string_yes(self):
        assert safe_bool("yes") is True

    def test_safe_bool_returns_true_for_string_1(self):
        assert safe_bool("1") is True

    def test_safe_bool_returns_false_for_string_false(self):
        assert safe_bool("false") is False

    def test_safe_bool_returns_false_for_string_no(self):
        assert safe_bool("no") is False

    def test_safe_bool_returns_true_for_case_insensitive(self):
        assert safe_bool("TRUE") is True
        assert safe_bool("True") is True
        assert safe_bool("YES") is True

    def test_safe_bool_returns_true_for_nonzero_int(self):
        assert safe_bool(1) is True
        assert safe_bool(42) is True

    def test_safe_bool_returns_false_for_zero(self):
        assert safe_bool(0) is False


# ============================================================================
# safe_float
# ============================================================================


class TestSafeFloat:
    def test_safe_float_returns_float_from_float(self):
        assert safe_float(3.14) == 3.14

    def test_safe_float_returns_float_from_int(self):
        assert safe_float(42) == 42.0

    def test_safe_float_returns_float_from_string(self):
        assert safe_float("3.14") == 3.14

    def test_safe_float_returns_none_for_none(self):
        assert safe_float(None) is None

    def test_safe_float_returns_none_for_non_numeric_string(self):
        assert safe_float("abc") is None

    def test_safe_float_returns_none_for_empty_string(self):
        assert safe_float("") is None


# ============================================================================
# safe_decimal_str
# ============================================================================


class TestSafeDecimalStr:
    def test_safe_decimal_str_from_int(self):
        assert safe_decimal_str(42) == "42"

    def test_safe_decimal_str_from_float(self):
        result = safe_decimal_str(3.14)
        assert result is not None
        assert "3.14" in result

    def test_safe_decimal_str_from_string(self):
        assert safe_decimal_str("99.95") == "99.95"

    def test_safe_decimal_str_returns_none_for_none(self):
        assert safe_decimal_str(None) is None

    def test_safe_decimal_str_returns_none_for_invalid(self):
        assert safe_decimal_str("abc") is None

    def test_safe_decimal_str_returns_none_for_empty(self):
        assert safe_decimal_str("") is None


# ============================================================================
# parse_timestamp
# ============================================================================


class TestParseTimestamp:
    def test_parse_timestamp_returns_none_for_none(self):
        assert parse_timestamp(None) is None

    def test_parse_timestamp_from_datetime(self):
        dt = datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)
        result = parse_timestamp(dt)
        assert result == dt.isoformat()

    def test_parse_timestamp_from_string(self):
        result = parse_timestamp("2024-01-15T10:30:00")
        assert result == "2024-01-15T10:30:00"

    def test_parse_timestamp_replaces_z_suffix(self):
        result = parse_timestamp("2024-01-15T10:30:00Z")
        assert result == "2024-01-15T10:30:00+00:00"

    def test_parse_timestamp_returns_none_for_empty_string(self):
        assert parse_timestamp("") is None

    def test_parse_timestamp_returns_none_for_whitespace(self):
        assert parse_timestamp("   ") is None

    def test_parse_timestamp_returns_none_for_int(self):
        assert parse_timestamp(12345) is None

    def test_parse_timestamp_strips_whitespace(self):
        result = parse_timestamp("  2024-01-15T10:30:00  ")
        assert result == "2024-01-15T10:30:00"


# ============================================================================
# parse_timestamp_dt
# ============================================================================


class TestParseTimestampDt:
    def test_parse_timestamp_dt_returns_none_for_none(self):
        assert parse_timestamp_dt(None) is None

    def test_parse_timestamp_dt_returns_datetime_for_datetime(self):
        dt = datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)
        assert parse_timestamp_dt(dt) is dt

    def test_parse_timestamp_dt_parses_iso_string(self):
        result = parse_timestamp_dt("2024-01-15T10:30:00+00:00")
        assert isinstance(result, datetime)
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15

    def test_parse_timestamp_dt_handles_z_suffix(self):
        result = parse_timestamp_dt("2024-01-15T10:30:00Z")
        assert isinstance(result, datetime)

    def test_parse_timestamp_dt_returns_none_for_empty_string(self):
        assert parse_timestamp_dt("") is None

    def test_parse_timestamp_dt_returns_none_for_whitespace(self):
        assert parse_timestamp_dt("   ") is None

    def test_parse_timestamp_dt_returns_none_for_invalid_string(self):
        assert parse_timestamp_dt("not-a-date") is None

    def test_parse_timestamp_dt_returns_none_for_int(self):
        assert parse_timestamp_dt(12345) is None


# ============================================================================
# now_iso, now_datetime, today_date
# ============================================================================


class TestTimeUtilities:
    def test_now_iso_returns_iso_string(self):
        result = now_iso()
        assert isinstance(result, str)
        # Should parse back into a datetime
        datetime.fromisoformat(result)

    def test_now_datetime_returns_utc_datetime(self):
        result = now_datetime()
        assert isinstance(result, datetime)
        assert result.tzinfo is not None

    def test_today_date_returns_date(self):
        result = today_date()
        assert isinstance(result, date)


# ============================================================================
# elapsed_ms
# ============================================================================


class TestElapsedMs:
    def test_elapsed_ms_returns_non_negative(self):
        start = datetime.now(UTC)
        result = elapsed_ms(start)
        assert isinstance(result, int)
        assert result >= 0


# ============================================================================
# inject_metadata
# ============================================================================


class TestInjectMetadata:
    def test_inject_metadata_adds_fields(self):
        row = {"key": "value"}
        result = inject_metadata(row, "evt_001")
        assert result["trace_id"] == "evt_001"
        assert "created_at" in result
        assert "updated_at" in result
        assert "last_enriched_at" in result

    def test_inject_metadata_without_last_enriched(self):
        row = {"key": "value"}
        result = inject_metadata(row, "evt_001", include_last_enriched=False)
        assert result["trace_id"] == "evt_001"
        assert "created_at" in result
        assert "updated_at" in result
        assert "last_enriched_at" not in result

    def test_inject_metadata_modifies_row_in_place(self):
        row = {"key": "value"}
        result = inject_metadata(row, "evt_001")
        assert result is row

    def test_inject_metadata_created_and_updated_are_same(self):
        row = {}
        inject_metadata(row, "evt_001")
        assert row["created_at"] == row["updated_at"]
