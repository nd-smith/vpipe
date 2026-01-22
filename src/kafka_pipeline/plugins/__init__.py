# Copyright (c) 2024-2026 nickdsmith. All Rights Reserved.
# SPDX-License-Identifier: PROPRIETARY
# 
# This file is proprietary and confidential. Unauthorized copying of this file,
# via any medium is strictly prohibited.

"""
Plugin framework for injecting business logic into the pipeline.

Provides a lightweight mechanism for reacting to pipeline events
based on configurable conditions and executing actions.

Import plugin classes and utilities from:
- kafka_pipeline.plugins.shared.base - Core plugin interfaces
- kafka_pipeline.plugins.shared.registry - Plugin registration
- kafka_pipeline.plugins.shared.loader - Plugin loading utilities
- kafka_pipeline.plugins.itel_cabinet_api - iTel Cabinet API plugin
"""
