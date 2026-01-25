# Copyright (c) 2024-2026 nickdsmith. All Rights Reserved.
# SPDX-License-Identifier: PROPRIETARY
#
# This file is proprietary and confidential. Unauthorized copying of this file,
# via any medium is strictly prohibited.

"""iTel Cabinet API Plugin.

Simplified plugin architecture after refactoring.
- models.py: Typed data structures
- parsers.py: Form parsing functions
- pipeline.py: Main processing logic
- delta.py: Delta writer wrapper
- itel_cabinet_tracking_worker.py: Tracking worker
- itel_cabinet_api_worker.py: API worker
"""
