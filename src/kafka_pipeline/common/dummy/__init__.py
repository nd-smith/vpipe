"""
Dummy data source for testing the Kafka pipeline without external dependencies.

Provides realistic-looking insurance claim data for both XACT and ClaimX domains,
allowing pipeline testing without access to production data sources.

Plugin Profiles:
    Use plugin profiles to generate data tailored for specific plugin workflows:
    - itel_cabinet_api: Generate itel Cabinet Repair Form events (task_id=32513)
    - standard_claimx: Generate standard ClaimX events
    - standard_xact: Generate standard XACT events

Example:
    # config/dummy/itel_test.yaml
    generator:
      plugin_profile: "itel_cabinet_api"
    domains:
      - claimx
    events_per_minute: 10.0

See kafka_pipeline/common/dummy/PLUGIN_TESTING.md for complete documentation.
"""

from kafka_pipeline.common.dummy.file_server import DummyFileServer
from kafka_pipeline.common.dummy.plugin_profiles import (
    ItelCabinetDataGenerator,
    ItelCabinetFormData,
    PluginProfile,
)
from kafka_pipeline.common.dummy.source import DummyDataSource, DummySourceConfig

__all__ = [
    "DummyDataSource",
    "DummySourceConfig",
    "DummyFileServer",
    "PluginProfile",
    "ItelCabinetDataGenerator",
    "ItelCabinetFormData",
]
