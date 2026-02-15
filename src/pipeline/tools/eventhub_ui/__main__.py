"""Entry point: python -m pipeline.tools.eventhub_ui"""

import argparse
import sys


def main():
    parser = argparse.ArgumentParser(description="EventHub Utility Dashboard")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8550, help="Bind port (default: 8550)")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload for development")
    args = parser.parse_args()

    import uvicorn

    uvicorn.run(
        "pipeline.tools.eventhub_ui.app:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
    )


if __name__ == "__main__":
    main()
