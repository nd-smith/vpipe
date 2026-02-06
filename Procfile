# XACT Pipeline Workers
# Each process type maps to a Cloud Foundry app deployed via Jenkinsfile
# Process type names must match the WORKER_NAME in conveyorDeploy()

# Polls Eventhouse for XACT events and produces to events.raw topic
xact-poller: python -m pipeline --worker xact-poller --log-to-stdout

# Consumes from events.raw, processes events, and produces download tasks
xact-event-ingester: python -m pipeline --worker xact-event-ingester --log-to-stdout

# Consumes from events.raw and writes to Delta Lake xact_events table
xact-delta-writer: python -m pipeline --worker xact-delta-writer --log-to-stdout

# Enriches events with additional data
xact-enricher: python -m pipeline --worker xact-enricher --log-to-stdout

# Downloads files from external sources
xact-download: python -m pipeline --worker xact-download --log-to-stdout

# Uploads cached files to storage
xact-upload: python -m pipeline --worker xact-upload --log-to-stdout

# Processes download results and writes to Delta Lake inventory/failed tables
xact-result-processor: python -m pipeline --worker xact-result-processor --log-to-stdout

# Handles retry logic for failed events
xact-retry-scheduler: python -m pipeline --worker xact-retry-scheduler --log-to-stdout
