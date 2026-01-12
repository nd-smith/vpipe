"""
Integration tests for Kafka pipeline.

Tests using real Kafka container via Testcontainers to verify:
- Producer/consumer interaction
- Message serialization/deserialization
- Retry flow with exponential backoff
- DLQ routing
- End-to-end workflows
"""
