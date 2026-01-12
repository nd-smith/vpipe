
import sys
import os

sys.path.append(os.path.join(os.getcwd(), 'src'))

try:
    from kafka_pipeline.common.metrics import uploads_concurrent, update_uploads_concurrent
    print("✅ Successfully imported uploads_concurrent from kafka_pipeline.common.metrics")
except ImportError as e:
    print(f"❌ Failed to import uploads_concurrent: {e}")
    sys.exit(1)

print("\nMetrics verification successful.")
