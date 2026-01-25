# Copyright (c) 2024-2026 nickdsmith. All Rights Reserved.
# SPDX-License-Identifier: PROPRIETARY
#
# This file is proprietary and confidential. Unauthorized copying of this file,
# via any medium is strictly prohibited.

"""Allow running simulation tools as modules.

Usage:
    python -m kafka_pipeline.simulation.dummy_producer --domains claimx
"""

import sys


def main():
    """Entry point for simulation module execution."""
    # Check if dummy_producer is being run directly
    if "dummy_producer" in sys.argv[0]:
        # Already running dummy_producer.py directly
        from kafka_pipeline.simulation.dummy_producer import main as dummy_main
        import asyncio

        asyncio.run(dummy_main())
    else:
        print("Usage: python -m kafka_pipeline.simulation.dummy_producer [args]")
        print("See: python -m kafka_pipeline.simulation.dummy_producer --help")
        sys.exit(1)


if __name__ == "__main__":
    main()
