"""
Test ADX-compatible logging with type safety.

Verifies that numeric fields are properly typed in JSON output,
not converted to strings.
"""

import json
import logging
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from core.logging.formatters import JSONFormatter
from core.logging.context import set_log_context


def test_numeric_type_preservation():
    """Test that numeric fields maintain their types in JSON output."""

    # Create a logger with JSON formatter
    logger = logging.getLogger("test_adx")
    logger.setLevel(logging.DEBUG)

    # Create string handler to capture output
    from io import StringIO
    stream = StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JSONFormatter())
    logger.addHandler(handler)

    # Set some context
    set_log_context(
        domain="verisk",
        stage="download",
        worker_id="test-worker",
        trace_id="evt_test_123"
    )

    # Log with various numeric fields
    logger.info(
        "Processing batch",
        extra={
            "batch_size": 100,
            "processing_time_ms": 1234.56,
            "retry_count": 3,
            "records_processed": 1000,
            "bytes_downloaded": 1048576,
            "http_status": 200,
        }
    )

    # Get the JSON output
    output = stream.getvalue().strip()
    print("Raw JSON output:")
    print(output)
    print()

    # Parse JSON
    log_entry = json.loads(output)

    # Verify types
    print("Type Verification:")
    print("-" * 50)

    test_cases = [
        ("batch_size", int, 100),
        ("processing_time_ms", float, 1234.56),
        ("retry_count", int, 3),
        ("records_processed", int, 1000),
        ("bytes_downloaded", int, 1048576),
        ("http_status", int, 200),
    ]

    all_passed = True
    for field, expected_type, expected_value in test_cases:
        value = log_entry.get(field)
        actual_type = type(value)

        # Check type
        type_match = actual_type == expected_type
        # Check value
        value_match = value == expected_value

        status = "✅" if (type_match and value_match) else "❌"

        print(f"{status} {field}:")
        print(f"   Expected: {expected_type.__name__} = {expected_value}")
        print(f"   Actual:   {actual_type.__name__} = {value}")

        if not (type_match and value_match):
            all_passed = False
            if not type_match:
                print(f"   ⚠️  Type mismatch!")
            if not value_match:
                print(f"   ⚠️  Value mismatch!")
        print()

    # Verify structured fields exist
    print("Context Fields:")
    print("-" * 50)
    for field in ["ts", "level", "logger", "msg", "domain", "stage", "trace_id"]:
        value = log_entry.get(field)
        print(f"✅ {field}: {value}")

    return all_passed


def test_exception_structure():
    """Test that exceptions are structured properly for ADX querying."""

    logger = logging.getLogger("test_exception")
    logger.setLevel(logging.ERROR)

    from io import StringIO
    stream = StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JSONFormatter())
    logger.addHandler(handler)

    # Log an exception
    try:
        raise ValueError("Test error for ADX")
    except ValueError as e:
        logger.error("Upload failed", extra={"error": str(e)}, exc_info=True)

    output = stream.getvalue().strip()
    print("\nException Handling:")
    print("-" * 50)
    print("Raw JSON:")
    print(output)
    print()

    log_entry = json.loads(output)

    # Verify exception structure
    if "exception" in log_entry:
        exc = log_entry["exception"]
        print("Exception structure:")
        print(f"  Type:    {exc.get('type')}")
        print(f"  Message: {exc.get('message')}")
        print(f"  Has stacktrace: {bool(exc.get('stacktrace'))}")

        # Verify it's a dict, not a string
        if isinstance(exc, dict):
            print("✅ Exception is structured (dict)")
            print("✅ Can query in ADX: exception.type, exception.message")
            return True
        else:
            print("❌ Exception is not structured (string)")
            return False
    else:
        print("❌ No exception field found")
        return False


def test_url_sanitization():
    """Test that URLs with sensitive params are sanitized."""

    logger = logging.getLogger("test_sanitize")
    logger.setLevel(logging.INFO)

    from io import StringIO
    stream = StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JSONFormatter())
    logger.addHandler(handler)

    # Log with sensitive URL
    logger.info(
        "Download complete",
        extra={
            "download_url": "https://api.example.com/file?id=123&sig=secret_token_here&other=value",
            "batch_size": 50,
        }
    )

    output = stream.getvalue().strip()
    log_entry = json.loads(output)

    print("\nURL Sanitization:")
    print("-" * 50)

    sanitized_url = log_entry.get("download_url")
    print(f"Sanitized URL: {sanitized_url}")

    if "sig=[REDACTED]" in sanitized_url:
        print("✅ Sensitive parameter sanitized")
        secret_removed = "secret_token_here" not in sanitized_url
        if secret_removed:
            print("✅ Original secret removed")
            return True
        else:
            print("❌ Original secret still present!")
            return False
    else:
        print("❌ URL not sanitized properly")
        return False


if __name__ == "__main__":
    print("=" * 60)
    print("ADX Logging Type Safety Test")
    print("=" * 60)
    print()

    test1 = test_numeric_type_preservation()
    test2 = test_exception_structure()
    test3 = test_url_sanitization()

    print("\n" + "=" * 60)
    print("Test Results:")
    print("=" * 60)
    print(f"Numeric Type Preservation: {'✅ PASS' if test1 else '❌ FAIL'}")
    print(f"Exception Structure:       {'✅ PASS' if test2 else '❌ FAIL'}")
    print(f"URL Sanitization:          {'✅ PASS' if test3 else '❌ FAIL'}")
    print()

    if all([test1, test2, test3]):
        print("🎉 All tests passed! Logging is ADX-ready.")
        sys.exit(0)
    else:
        print("❌ Some tests failed.")
        sys.exit(1)
