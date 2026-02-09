from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path

from core.utils.json_serializers import json_serializer


class Color(Enum):
    RED = "red"
    BLUE = "blue"


class SampleObj:
    def __init__(self, x, y):
        self.x = x
        self.y = y


# =========================================================================
# json_serializer
# =========================================================================


class TestJsonSerializer:

    def test_serializes_datetime_to_isoformat(self):
        dt = datetime(2025, 6, 15, 10, 30, 0)
        assert json_serializer(dt) == "2025-06-15T10:30:00"

    def test_serializes_datetime_with_microseconds(self):
        dt = datetime(2025, 1, 1, 0, 0, 0, 123456)
        result = json_serializer(dt)
        assert result == "2025-01-01T00:00:00.123456"

    def test_serializes_date_to_isoformat(self):
        d = date(2025, 12, 25)
        assert json_serializer(d) == "2025-12-25"

    def test_serializes_decimal_to_float(self):
        result = json_serializer(Decimal("3.14"))
        assert result == 3.14
        assert isinstance(result, float)

    def test_serializes_decimal_zero(self):
        result = json_serializer(Decimal("0"))
        assert result == 0.0
        assert isinstance(result, float)

    def test_serializes_large_decimal(self):
        result = json_serializer(Decimal("123456789.123456789"))
        assert isinstance(result, float)

    def test_serializes_path_to_string(self):
        p = Path("/usr/local/bin")
        assert json_serializer(p) == "/usr/local/bin"

    def test_serializes_windows_style_path(self):
        p = Path("C:/Users/test/file.txt")
        result = json_serializer(p)
        assert isinstance(result, str)
        assert "file.txt" in result

    def test_serializes_enum_to_value(self):
        assert json_serializer(Color.RED) == "red"
        assert json_serializer(Color.BLUE) == "blue"

    def test_serializes_object_with_dict(self):
        obj = SampleObj(1, "hello")
        result = json_serializer(obj)
        assert result == {"x": 1, "y": "hello"}

    def test_fallback_to_string(self):
        result = json_serializer(42)
        assert result == "42"

    def test_fallback_for_set(self):
        result = json_serializer({1, 2, 3})
        assert isinstance(result, str)

    def test_fallback_for_bytes(self):
        result = json_serializer(b"hello")
        assert result == "b'hello'"

    def test_serializes_none_to_string(self):
        # None has no .value and no __dict__, so falls through to str()
        result = json_serializer(None)
        assert result == "None"

    def test_date_before_datetime_check(self):
        # date is a parent of datetime; both should produce isoformat
        d = date(2025, 1, 1)
        dt = datetime(2025, 1, 1, 12, 0, 0)
        assert json_serializer(d) == "2025-01-01"
        assert json_serializer(dt) == "2025-01-01T12:00:00"
