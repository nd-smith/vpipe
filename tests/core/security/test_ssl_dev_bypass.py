import os
import ssl
from unittest.mock import patch

import pytest

import core.security.ssl_dev_bypass as ssl_bypass_module
from core.security.ssl_dev_bypass import apply_ssl_dev_bypass, get_eventhub_ssl_kwargs


@pytest.fixture(autouse=True)
def reset_patched_state():
    """Reset the module-level _patched flag before and after each test."""
    original_patched = ssl_bypass_module._patched
    ssl_bypass_module._patched = False
    yield
    ssl_bypass_module._patched = original_patched


# =========================================================================
# apply_ssl_dev_bypass
# =========================================================================


class TestApplySslDevBypass:

    @patch.dict(os.environ, {"DISABLE_SSL_VERIFY": "false"}, clear=True)
    def test_does_nothing_when_disabled(self):
        apply_ssl_dev_bypass()
        assert ssl_bypass_module._patched is False

    @patch.dict(os.environ, {}, clear=True)
    def test_does_nothing_when_env_var_not_set(self):
        apply_ssl_dev_bypass()
        assert ssl_bypass_module._patched is False

    @patch.dict(os.environ, {"DISABLE_SSL_VERIFY": "no"}, clear=True)
    def test_does_nothing_when_env_var_is_no(self):
        apply_ssl_dev_bypass()
        assert ssl_bypass_module._patched is False

    @patch.dict(os.environ, {"DISABLE_SSL_VERIFY": "random"}, clear=True)
    def test_does_nothing_for_unrecognized_value(self):
        apply_ssl_dev_bypass()
        assert ssl_bypass_module._patched is False

    @patch.dict(
        os.environ,
        {"DISABLE_SSL_VERIFY": "true", "ENVIRONMENT": "production"},
        clear=True,
    )
    def test_refuses_in_production_environment(self):
        apply_ssl_dev_bypass()
        assert ssl_bypass_module._patched is False

    @patch.dict(
        os.environ,
        {"DISABLE_SSL_VERIFY": "true", "APP_ENV": "production"},
        clear=True,
    )
    def test_refuses_in_production_app_env(self):
        apply_ssl_dev_bypass()
        assert ssl_bypass_module._patched is False

    @patch.dict(
        os.environ,
        {"DISABLE_SSL_VERIFY": "true", "ENVIRONMENT": "development"},
        clear=True,
    )
    def test_applies_patch_in_development(self):
        apply_ssl_dev_bypass()
        assert ssl_bypass_module._patched is True

    @patch.dict(
        os.environ, {"DISABLE_SSL_VERIFY": "1", "ENVIRONMENT": "dev"}, clear=True
    )
    def test_accepts_1_as_true(self):
        apply_ssl_dev_bypass()
        assert ssl_bypass_module._patched is True

    @patch.dict(
        os.environ, {"DISABLE_SSL_VERIFY": "yes", "ENVIRONMENT": "dev"}, clear=True
    )
    def test_accepts_yes_as_true(self):
        apply_ssl_dev_bypass()
        assert ssl_bypass_module._patched is True

    @patch.dict(
        os.environ, {"DISABLE_SSL_VERIFY": "TRUE", "ENVIRONMENT": "dev"}, clear=True
    )
    def test_case_insensitive_true(self):
        apply_ssl_dev_bypass()
        assert ssl_bypass_module._patched is True

    @patch.dict(
        os.environ, {"DISABLE_SSL_VERIFY": "true", "ENVIRONMENT": "dev"}, clear=True
    )
    def test_only_patches_once(self):
        apply_ssl_dev_bypass()
        assert ssl_bypass_module._patched is True

        # Capture current ssl.create_default_context reference
        first_create_default = ssl.create_default_context

        # Call again - should be a no-op
        apply_ssl_dev_bypass()
        assert ssl_bypass_module._patched is True

        # The function should not have been re-wrapped
        assert ssl.create_default_context is first_create_default

    @patch.dict(
        os.environ, {"DISABLE_SSL_VERIFY": "true", "ENVIRONMENT": "dev"}, clear=True
    )
    def test_patches_ssl_create_default_context(self):
        original_create = ssl.create_default_context
        apply_ssl_dev_bypass()
        # After patching, ssl.create_default_context should be different
        assert ssl.create_default_context is not original_create

    @patch.dict(
        os.environ, {"DISABLE_SSL_VERIFY": "true", "ENVIRONMENT": "dev"}, clear=True
    )
    def test_patched_create_default_context_disables_verification(self):
        apply_ssl_dev_bypass()
        ctx = ssl.create_default_context()
        assert ctx.check_hostname is False
        assert ctx.verify_mode == ssl.CERT_NONE


# =========================================================================
# get_eventhub_ssl_kwargs
# =========================================================================


class TestGetEventhubSslKwargs:

    def test_returns_empty_dict_when_not_patched(self):
        ssl_bypass_module._patched = False
        result = get_eventhub_ssl_kwargs()
        assert result == {}

    def test_returns_connection_verify_false_when_patched(self):
        ssl_bypass_module._patched = True
        result = get_eventhub_ssl_kwargs()
        assert result == {"connection_verify": False}

    def test_can_unpack_into_kwargs(self):
        ssl_bypass_module._patched = True
        kwargs = get_eventhub_ssl_kwargs()
        # Verify it works when unpacked
        assert "connection_verify" in kwargs
        assert kwargs["connection_verify"] is False
