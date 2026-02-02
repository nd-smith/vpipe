"""Allow running simulation tools as modules.

Usage:
    python -m pipeline.simulation.dummy_producer --domains claimx
"""

import sys


def main():
    """Entry point for simulation module execution."""
    # Check if dummy_producer is being run directly
    if "dummy_producer" in sys.argv[0]:
        # Already running dummy_producer.py directly
        import asyncio

        from pipeline.simulation.dummy_producer import main as dummy_main

        asyncio.run(dummy_main())
    else:
        print("Usage: python -m pipeline.simulation.dummy_producer [args]")
        print("See: python -m pipeline.simulation.dummy_producer --help")
        sys.exit(1)


if __name__ == "__main__":
    main()
