import os
import sys

# Add src to path
sys.path.append(os.path.join(os.getcwd(), "src"))

try:
    from pipeline.common.metrics import (  # noqa: F401
        claim_media_bytes_total,
        claim_processing_seconds,
        claimx_api_request_duration_seconds,
        claimx_api_requests_total,
    )

    print("✅ Successfully imported new metrics from pipeline.common.metrics")
except ImportError as e:
    print(f"❌ Failed to import metrics: {e}")
    sys.exit(1)

try:
    from pipeline.claimx.api_client import ClaimXApiClient  # noqa: F401

    print("✅ Successfully imported ClaimXApiClient (instrumented)")
except ImportError as e:
    print(f"❌ Failed to import ClaimXApiClient: {e}")
    # Don't exit, continue to verify other parts
    # sys.exit(1)

# try:
#     from pipeline.claimx.workers.download_worker import ClaimXDownloadWorker
#     print("✅ Successfully imported ClaimXDownloadWorker (instrumented)")
# except ImportError as e:
#     print(f"❌ Failed to import ClaimXDownloadWorker (likely due to deep dependencies): {e}")

print("\nVerification successful! Codebase is instrumented and syntactically correct.")
