# XACT Pipeline Workers
# Each worker runs as a separate process/app on the cube

# Polls Eventhouse for XACT events and produces to events.raw topic
poller: python -m pipeline --worker xact-poller --log-to-stdout

# Consumes from events.raw, processes events, and produces download tasks
ingester: python -m pipeline --worker xact-event-ingester --log-to-stdout

# Consumes from events.raw and writes to Delta Lake xact_events table
writer: python -m pipeline --worker xact-delta-writer --log-to-stdout

# Enriches events with additional data
enricher: python -m pipeline --worker xact-enricher --log-to-stdout
