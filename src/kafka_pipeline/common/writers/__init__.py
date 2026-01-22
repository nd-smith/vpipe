# Copyright (c) 2024-2026 nickdsmith. All Rights Reserved.
# SPDX-License-Identifier: PROPRIETARY
# 
# This file is proprietary and confidential. Unauthorized copying of this file,
# via any medium is strictly prohibited.

"""Delta table writers and base classes."""

from kafka_pipeline.common.writers.base import BaseDeltaWriter
from kafka_pipeline.common.writers.delta_writer import DeltaWriter

__all__ = ["BaseDeltaWriter", "DeltaWriter"]
