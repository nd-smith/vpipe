"""Tests for configuration security validation.

Tests the ConfigValidator class to ensure secrets are properly blocked
from YAML files and required environment variables are validated.
"""

import os
import pytest
from unittest.mock import patch

import sys
sys.path.insert(0, '/home/nick/projects/vpipe/src')

from config.config_validator import (
    ConfigValidator,
    REQUIRED_ENV_SECRETS,
    OPTIONAL_ENV_SECRETS,
)


class TestConfigValidator:
    """Test suite for ConfigValidator security validation."""

    def setup_method(self):
        """Set up test fixtures."""
        self.validator = ConfigValidator()

    def test_detect_secret_in_yaml_token(self):
        """Test that API tokens in YAML are detected."""
        config = {
            "api": {
                "token": "abc123secret",
            }
        }

        errors = self.validator.validate_no_secrets_in_yaml(config)
        assert len(errors) > 0
        assert "api.token" in errors[0]
        assert "Secret found in YAML" in errors[0]

    def test_allow_env_var_reference(self):
        """Test that ${VAR_NAME} references are allowed."""
        config = {
            "api": {
                "token": "${CLAIMX_API_TOKEN}",
            }
        }

        errors = self.validator.validate_no_secrets_in_yaml(config)
        assert len(errors) == 0

    @patch.dict(os.environ, {"CLAIMX_API_TOKEN": "test-token", "ONELAKE_BASE_PATH": "test-path"})
    def test_load_secrets_success(self):
        """Test loading secrets from environment when all required are set."""
        secrets = self.validator.load_secrets_from_env()

        assert secrets["CLAIMX_API_TOKEN"] == "test-token"
        assert secrets["ONELAKE_BASE_PATH"] == "test-path"

    @patch.dict(os.environ, {"CLAIMX_API_TOKEN": "test-token"}, clear=True)
    def test_load_secrets_missing_required(self):
        """Test that missing required secrets raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            self.validator.load_secrets_from_env()

        error_msg = str(exc_info.value)
        assert "Required environment variables not set" in error_msg
        assert "ONELAKE_BASE_PATH" in error_msg


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
