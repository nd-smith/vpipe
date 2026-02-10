"""
Unit tests for the monitoring dashboard module.

Test Coverage:
    - MetricsParser: parsing Prometheus text format
    - MonitoringServer: session management, metrics fetching, dashboard data
    - TopicStats extraction from parsed metrics
    - WorkerStats extraction from parsed metrics
    - Dashboard HTML rendering (topic rows, worker rows, alerts)
    - HTTP handler responses (index, api/stats)
    - Dataclass defaults
"""

import math
from unittest.mock import AsyncMock, MagicMock

import pytest

from pipeline.common.monitoring import (
    DashboardData,
    MetricsParser,
    MonitoringServer,
    TopicStats,
    WorkerStats,
)

# =============================================================================
# Dataclass defaults
# =============================================================================


class TestTopicStats:
    def test_defaults(self):
        ts = TopicStats(name="my-topic")
        assert ts.name == "my-topic"
        assert ts.partitions == 0
        assert ts.total_lag == 0
        assert ts.messages_consumed == 0
        assert ts.messages_produced == 0


class TestWorkerStats:
    def test_defaults(self):
        ws = WorkerStats(name="my-worker")
        assert ws.name == "my-worker"
        assert ws.connected is False
        assert ws.in_flight == 0
        assert ws.errors == 0
        assert ws.circuit_breaker == "closed"


class TestDashboardData:
    def test_defaults(self):
        dd = DashboardData()
        assert dd.topics == {}
        assert dd.workers == {}
        assert dd.timestamp == ""
        assert dd.metrics_error is None
        assert dd.kafka_error is None


# =============================================================================
# MetricsParser
# =============================================================================


class TestMetricsParserParseEmptyAndComments:
    def test_returns_empty_dict_for_empty_string(self):
        parser = MetricsParser()
        assert parser.parse("") == {}

    def test_skips_comment_lines(self):
        text = "# HELP my_metric A help line\n# TYPE my_metric counter\n"
        parser = MetricsParser()
        assert parser.parse(text) == {}

    def test_skips_blank_lines(self):
        text = "\n\n\n"
        parser = MetricsParser()
        assert parser.parse(text) == {}


class TestMetricsParserSimpleMetrics:
    def test_parses_metric_without_labels(self):
        text = "http_requests_total 42"
        parser = MetricsParser()
        result = parser.parse(text)
        assert result == {"http_requests_total": [{"labels": {}, "value": 42.0}]}

    def test_parses_metric_with_labels(self):
        text = 'http_requests_total{method="GET",path="/api"} 100'
        parser = MetricsParser()
        result = parser.parse(text)
        assert result == {
            "http_requests_total": [{"labels": {"method": "GET", "path": "/api"}, "value": 100.0}]
        }

    def test_parses_multiple_entries_for_same_metric(self):
        text = 'http_requests_total{method="GET"} 100\nhttp_requests_total{method="POST"} 50\n'
        parser = MetricsParser()
        result = parser.parse(text)
        assert len(result["http_requests_total"]) == 2
        assert result["http_requests_total"][0]["labels"]["method"] == "GET"
        assert result["http_requests_total"][0]["value"] == 100.0
        assert result["http_requests_total"][1]["labels"]["method"] == "POST"
        assert result["http_requests_total"][1]["value"] == 50.0

    def test_parses_float_value(self):
        text = "request_duration_seconds 0.123"
        parser = MetricsParser()
        result = parser.parse(text)
        assert result["request_duration_seconds"][0]["value"] == 0.123

    def test_parses_scientific_notation(self):
        text = "tiny_value 1.5e-3"
        parser = MetricsParser()
        result = parser.parse(text)
        assert result["tiny_value"][0]["value"] == pytest.approx(0.0015)


class TestMetricsParserSpecialValues:
    def test_parses_nan(self):
        text = "broken_metric NaN"
        parser = MetricsParser()
        result = parser.parse(text)
        assert math.isnan(result["broken_metric"][0]["value"])

    def test_parses_inf(self):
        text = "big_metric Inf"
        parser = MetricsParser()
        result = parser.parse(text)
        assert result["big_metric"][0]["value"] == float("inf")

    def test_parses_negative_inf(self):
        text = "neg_metric -Inf"
        parser = MetricsParser()
        result = parser.parse(text)
        assert result["neg_metric"][0]["value"] == float("-inf")


class TestMetricsParserMalformedInput:
    def test_skips_line_that_does_not_match_pattern(self):
        text = "this is not a metric line\nvalid_metric 1"
        parser = MetricsParser()
        result = parser.parse(text)
        assert "valid_metric" in result
        assert len(result) == 1

    def test_skips_lines_with_whitespace_only(self):
        text = "   \n  valid_metric 5\n   "
        parser = MetricsParser()
        result = parser.parse(text)
        assert result["valid_metric"][0]["value"] == 5.0


class TestMetricsParserFullDocument:
    def test_parses_realistic_prometheus_output(self):
        text = (
            "# HELP pipeline_consumer_lag Consumer lag per partition\n"
            "# TYPE pipeline_consumer_lag gauge\n"
            'pipeline_consumer_lag{topic="verisk-downloads-pending",partition="0"} 5\n'
            'pipeline_consumer_lag{topic="verisk-downloads-pending",partition="1"} 3\n'
            "# HELP pipeline_messages_consumed_total Total consumed\n"
            "# TYPE pipeline_messages_consumed_total counter\n"
            'pipeline_messages_consumed_total{topic="verisk-downloads-pending"} 1000\n'
        )
        parser = MetricsParser()
        result = parser.parse(text)
        assert len(result["pipeline_consumer_lag"]) == 2
        assert len(result["pipeline_messages_consumed_total"]) == 1


# =============================================================================
# MonitoringServer — session management
# =============================================================================


class TestMonitoringServerInit:
    def test_default_values(self):
        server = MonitoringServer()
        assert server.port == 8080
        assert server.metrics_url == "http://localhost:8000/metrics"

    def test_custom_values(self):
        server = MonitoringServer(
            port=9999,
            metrics_url="http://prom:9090/metrics",
        )
        assert server.port == 9999
        assert server.metrics_url == "http://prom:9090/metrics"


class TestMonitoringServerEnsureSession:
    async def test_creates_session_when_none(self):
        server = MonitoringServer()
        assert server._session is None

        session = server._ensure_session()
        assert session is not None
        assert server._session is session
        await session.close()

    async def test_reuses_open_session(self):
        server = MonitoringServer()
        session1 = server._ensure_session()
        session2 = server._ensure_session()
        assert session1 is session2
        await session1.close()

    async def test_creates_new_session_when_closed(self):
        server = MonitoringServer()
        session1 = server._ensure_session()
        await session1.close()

        session2 = server._ensure_session()
        assert session2 is not session1
        assert not session2.closed
        await session2.close()


# =============================================================================
# MonitoringServer — fetch_metrics
# =============================================================================


class TestMonitoringServerFetchMetrics:
    async def test_returns_text_on_200(self):
        server = MonitoringServer()
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.text = AsyncMock(return_value="metric_a 1\n")
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_response)
        server._session = mock_session

        # Override _ensure_session to return our mock
        server._ensure_session = MagicMock(return_value=mock_session)

        result = await server.fetch_metrics()
        assert result == "metric_a 1\n"

    async def test_returns_none_on_non_200(self):
        server = MonitoringServer()
        mock_response = AsyncMock()
        mock_response.status = 500
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_response)
        server._ensure_session = MagicMock(return_value=mock_session)

        result = await server.fetch_metrics()
        assert result is None

    async def test_returns_none_on_exception(self):
        server = MonitoringServer()

        mock_session = MagicMock()
        mock_session.get = MagicMock(side_effect=Exception("connection refused"))
        server._ensure_session = MagicMock(return_value=mock_session)

        result = await server.fetch_metrics()
        assert result is None


# =============================================================================
# MonitoringServer — get_dashboard_data
# =============================================================================


class TestMonitoringServerGetDashboardData:
    async def test_sets_metrics_error_when_fetch_returns_none(self):
        server = MonitoringServer(metrics_url="http://bad:9090/metrics")
        server.fetch_metrics = AsyncMock(return_value=None)

        data = await server.get_dashboard_data()

        assert data.metrics_error == "Could not connect to http://bad:9090/metrics"
        assert data.topics == {}
        assert data.workers == {}
        assert data.timestamp != ""

    async def test_populates_topics_and_workers_from_metrics(self):
        metrics_text = (
            'pipeline_consumer_lag{topic="verisk-downloads-pending",partition="0"} 10\n'
            'pipeline_messages_consumed_total{topic="verisk-downloads-pending"} 500\n'
            'pipeline_connection_status{component="consumer"} 1\n'
        )
        server = MonitoringServer()
        server.fetch_metrics = AsyncMock(return_value=metrics_text)

        data = await server.get_dashboard_data()

        assert data.metrics_error is None
        assert "verisk-downloads-pending" in data.topics
        assert data.topics["verisk-downloads-pending"].total_lag == 10
        assert data.topics["verisk-downloads-pending"].messages_consumed == 500
        assert "download_worker" in data.workers


# =============================================================================
# MonitoringServer — _extract_topic_stats
# =============================================================================


class TestExtractTopicStats:
    def _make_server_and_data(self):
        server = MonitoringServer()
        data = DashboardData()
        return server, data

    def test_initializes_known_topics(self):
        server, data = self._make_server_and_data()
        server._extract_topic_stats({}, data)

        expected_topics = [
            "verisk-downloads-pending",
            "verisk-downloads-cached",
            "verisk-downloads-results",
            "verisk-dlq",
            "verisk_events",
            "claimx-downloads-pending",
            "claimx-downloads-cached",
            "claimx-downloads-results",
            "claimx-dlq",
            "claimx_events",
        ]
        for name in expected_topics:
            assert name in data.topics
            assert data.topics[name].name == name

    def test_accumulates_lag_across_partitions(self):
        server, data = self._make_server_and_data()
        metrics = {
            "pipeline_consumer_lag": [
                {"labels": {"topic": "verisk-downloads-pending", "partition": "0"}, "value": 5.0},
                {"labels": {"topic": "verisk-downloads-pending", "partition": "1"}, "value": 3.0},
            ]
        }
        server._extract_topic_stats(metrics, data)

        assert data.topics["verisk-downloads-pending"].total_lag == 8

    def test_counts_unique_partitions(self):
        server, data = self._make_server_and_data()
        metrics = {
            "pipeline_consumer_lag": [
                {"labels": {"topic": "verisk-downloads-pending", "partition": "0"}, "value": 1.0},
                {"labels": {"topic": "verisk-downloads-pending", "partition": "1"}, "value": 2.0},
                {"labels": {"topic": "verisk-downloads-pending", "partition": "1"}, "value": 3.0},
            ]
        }
        server._extract_topic_stats(metrics, data)

        assert data.topics["verisk-downloads-pending"].partitions == 2

    def test_accumulates_messages_consumed(self):
        server, data = self._make_server_and_data()
        metrics = {
            "pipeline_messages_consumed_total": [
                {"labels": {"topic": "verisk-dlq"}, "value": 42.0},
            ]
        }
        server._extract_topic_stats(metrics, data)

        assert data.topics["verisk-dlq"].messages_consumed == 42

    def test_accumulates_messages_produced(self):
        server, data = self._make_server_and_data()
        metrics = {
            "pipeline_messages_produced_total": [
                {"labels": {"topic": "verisk-downloads-results"}, "value": 99.0},
            ]
        }
        server._extract_topic_stats(metrics, data)

        assert data.topics["verisk-downloads-results"].messages_produced == 99

    def test_ignores_unknown_topics(self):
        server, data = self._make_server_and_data()
        metrics = {
            "pipeline_consumer_lag": [
                {"labels": {"topic": "unknown-topic", "partition": "0"}, "value": 100.0},
            ],
            "pipeline_messages_consumed_total": [
                {"labels": {"topic": "another-unknown"}, "value": 50.0},
            ],
        }
        server._extract_topic_stats(metrics, data)

        for topic_stats in data.topics.values():
            assert topic_stats.total_lag == 0
            assert topic_stats.messages_consumed == 0


# =============================================================================
# MonitoringServer — _extract_worker_stats
# =============================================================================


class TestExtractWorkerStats:
    def _make_server_and_data(self):
        server = MonitoringServer()
        data = DashboardData()
        return server, data

    def test_initializes_known_workers(self):
        server, data = self._make_server_and_data()
        server._extract_worker_stats({}, data)

        assert "event_ingester" in data.workers
        assert "enrichment_worker" in data.workers
        assert "download_worker" in data.workers
        assert "upload_worker" in data.workers
        assert "result_processor" in data.workers
        assert "delta_events_writer" in data.workers
        assert "entity_delta_writer" in data.workers

    def test_worker_display_names_are_title_case(self):
        server, data = self._make_server_and_data()
        server._extract_worker_stats({}, data)

        assert data.workers["event_ingester"].name == "Event Ingester"
        assert data.workers["enrichment_worker"].name == "Enrichment Worker"
        assert data.workers["download_worker"].name == "Download Worker"
        assert data.workers["upload_worker"].name == "Upload Worker"
        assert data.workers["result_processor"].name == "Result Processor"
        assert data.workers["delta_events_writer"].name == "Delta Events Writer"
        assert data.workers["entity_delta_writer"].name == "Entity Delta Writer"

    def test_consumer_connection_sets_all_workers_connected(self):
        server, data = self._make_server_and_data()
        metrics = {
            "pipeline_connection_status": [
                {"labels": {"component": "consumer"}, "value": 1.0},
            ]
        }
        server._extract_worker_stats(metrics, data)

        for worker in data.workers.values():
            assert worker.connected is True

    def test_consumer_connection_disconnected(self):
        server, data = self._make_server_and_data()
        metrics = {
            "pipeline_connection_status": [
                {"labels": {"component": "consumer"}, "value": 0.0},
            ]
        }
        server._extract_worker_stats(metrics, data)

        for worker in data.workers.values():
            assert worker.connected is False

    def test_producer_connection_ors_with_existing_status(self):
        server, data = self._make_server_and_data()
        metrics = {
            "pipeline_connection_status": [
                {"labels": {"component": "consumer"}, "value": 0.0},
                {"labels": {"component": "producer"}, "value": 1.0},
            ]
        }
        server._extract_worker_stats(metrics, data)

        # consumer set False, producer ORed True -> True
        for worker in data.workers.values():
            assert worker.connected is True

    def test_concurrent_downloads_metric_removed(self):
        """The kafka_downloads_concurrent metric was removed; in_flight stays at default 0."""
        server, data = self._make_server_and_data()
        server._extract_worker_stats({}, data)

        assert data.workers["download_worker"].in_flight == 0

    def test_processing_errors_pending_topic_goes_to_download_worker(self):
        server, data = self._make_server_and_data()
        metrics = {
            "pipeline_processing_errors_total": [
                {"labels": {"topic": "verisk-downloads-pending"}, "value": 3.0},
            ]
        }
        server._extract_worker_stats(metrics, data)

        assert data.workers["download_worker"].errors == 3

    def test_processing_errors_retry_topic_goes_to_download_worker(self):
        server, data = self._make_server_and_data()
        metrics = {
            "pipeline_processing_errors_total": [
                {"labels": {"topic": "verisk-downloads-pending-retry-5m"}, "value": 2.0},
            ]
        }
        server._extract_worker_stats(metrics, data)

        assert data.workers["download_worker"].errors == 2

    def test_processing_errors_cached_topic_goes_to_upload_worker(self):
        server, data = self._make_server_and_data()
        metrics = {
            "pipeline_processing_errors_total": [
                {"labels": {"topic": "verisk-downloads-cached"}, "value": 5.0},
            ]
        }
        server._extract_worker_stats(metrics, data)

        assert data.workers["upload_worker"].errors == 5

    def test_processing_errors_results_topic_goes_to_result_processor(self):
        server, data = self._make_server_and_data()
        metrics = {
            "pipeline_processing_errors_total": [
                {"labels": {"topic": "verisk-downloads-results"}, "value": 1.0},
            ]
        }
        server._extract_worker_stats(metrics, data)

        assert data.workers["result_processor"].errors == 1

    def test_processing_errors_unmatched_topic_ignored(self):
        server, data = self._make_server_and_data()
        metrics = {
            "pipeline_processing_errors_total": [
                {"labels": {"topic": "something-else"}, "value": 10.0},
            ]
        }
        server._extract_worker_stats(metrics, data)

        for worker in data.workers.values():
            assert worker.errors == 0

    def test_circuit_breaker_state_open(self):
        server, data = self._make_server_and_data()
        metrics = {
            "circuit_breaker_state": [
                {"labels": {"name": "download_worker"}, "value": 1.0},
            ]
        }
        server._extract_worker_stats(metrics, data)

        assert data.workers["download_worker"].circuit_breaker == "open"

    def test_circuit_breaker_state_half_open(self):
        server, data = self._make_server_and_data()
        metrics = {
            "circuit_breaker_state": [
                {"labels": {"name": "upload_worker"}, "value": 2.0},
            ]
        }
        server._extract_worker_stats(metrics, data)

        assert data.workers["upload_worker"].circuit_breaker == "half-open"

    def test_circuit_breaker_state_closed(self):
        server, data = self._make_server_and_data()
        metrics = {
            "circuit_breaker_state": [
                {"labels": {"name": "result_processor"}, "value": 0.0},
            ]
        }
        server._extract_worker_stats(metrics, data)

        assert data.workers["result_processor"].circuit_breaker == "closed"

    def test_circuit_breaker_unknown_state_value(self):
        server, data = self._make_server_and_data()
        metrics = {
            "circuit_breaker_state": [
                {"labels": {"name": "event_ingester"}, "value": 99.0},
            ]
        }
        server._extract_worker_stats(metrics, data)

        assert data.workers["event_ingester"].circuit_breaker == "unknown"

    def test_circuit_breaker_ignores_unknown_name(self):
        server, data = self._make_server_and_data()
        metrics = {
            "circuit_breaker_state": [
                {"labels": {"name": "nonexistent"}, "value": 1.0},
            ]
        }
        server._extract_worker_stats(metrics, data)

        # All workers keep their default "closed"
        for worker in data.workers.values():
            assert worker.circuit_breaker == "closed"


# =============================================================================
# MonitoringServer — render_dashboard
# =============================================================================


class TestRenderDashboard:
    def test_renders_html_with_topic_rows(self):
        server = MonitoringServer()
        data = DashboardData(
            topics={"t1": TopicStats(name="t1", messages_consumed=50, total_lag=0)},
            workers={},
            timestamp="2025-01-01 00:00:00",
        )
        html = server.render_dashboard(data)

        assert "<code>t1</code>" in html
        assert "50" in html

    def test_lag_zero_gets_text_success_class(self):
        server = MonitoringServer()
        data = DashboardData(
            topics={"t": TopicStats(name="t", total_lag=0)},
            workers={},
            timestamp="",
        )
        html = server.render_dashboard(data)
        assert "text-success" in html

    def test_lag_under_100_gets_text_warning_class(self):
        server = MonitoringServer()
        data = DashboardData(
            topics={"t": TopicStats(name="t", total_lag=50)},
            workers={},
            timestamp="",
        )
        html = server.render_dashboard(data)
        assert "text-warning" in html

    def test_lag_100_or_more_gets_text_danger_class(self):
        server = MonitoringServer()
        data = DashboardData(
            topics={"t": TopicStats(name="t", total_lag=100)},
            workers={},
            timestamp="",
        )
        html = server.render_dashboard(data)
        assert "text-danger" in html

    def test_worker_connected_shows_connected_badge(self):
        server = MonitoringServer()
        data = DashboardData(
            topics={},
            workers={"w": WorkerStats(name="Worker", connected=True)},
            timestamp="",
        )
        html = server.render_dashboard(data)
        assert "Connected" in html

    def test_worker_disconnected_shows_disconnected_badge(self):
        server = MonitoringServer()
        data = DashboardData(
            topics={},
            workers={"w": WorkerStats(name="Worker", connected=False)},
            timestamp="",
        )
        html = server.render_dashboard(data)
        assert "Disconnected" in html

    def test_circuit_breaker_open_shows_open_badge(self):
        server = MonitoringServer()
        data = DashboardData(
            topics={},
            workers={"w": WorkerStats(name="W", circuit_breaker="open")},
            timestamp="",
        )
        html = server.render_dashboard(data)
        assert "Open" in html

    def test_circuit_breaker_half_open_shows_half_open_badge(self):
        server = MonitoringServer()
        data = DashboardData(
            topics={},
            workers={"w": WorkerStats(name="W", circuit_breaker="half-open")},
            timestamp="",
        )
        html = server.render_dashboard(data)
        assert "Half-Open" in html

    def test_circuit_breaker_unknown_shows_unknown_badge(self):
        server = MonitoringServer()
        data = DashboardData(
            topics={},
            workers={"w": WorkerStats(name="W", circuit_breaker="bogus")},
            timestamp="",
        )
        html = server.render_dashboard(data)
        assert "Unknown" in html

    def test_worker_errors_nonzero_gets_text_danger_class(self):
        server = MonitoringServer()
        data = DashboardData(
            topics={},
            workers={"w": WorkerStats(name="W", errors=5)},
            timestamp="",
        )
        html = server.render_dashboard(data)
        assert "text-danger" in html

    def test_metrics_error_shows_alert(self):
        server = MonitoringServer()
        data = DashboardData(
            metrics_error="Could not connect to http://localhost:8000/metrics",
            timestamp="",
        )
        html = server.render_dashboard(data)
        assert "Metrics unavailable" in html
        assert "Could not connect" in html

    def test_kafka_error_shows_alert(self):
        server = MonitoringServer()
        data = DashboardData(
            kafka_error="Broker not reachable",
            timestamp="",
        )
        html = server.render_dashboard(data)
        assert "Kafka unavailable" in html
        assert "Broker not reachable" in html

    def test_no_errors_means_no_alerts(self):
        server = MonitoringServer()
        data = DashboardData(timestamp="")
        html = server.render_dashboard(data)
        assert "alert-warning" not in html

    def test_renders_configuration_section(self):
        server = MonitoringServer(
            metrics_url="http://prom:9090/metrics",
        )
        data = DashboardData(timestamp="")
        html = server.render_dashboard(data)
        assert "http://prom:9090/metrics" in html

    def test_renders_timestamp(self):
        server = MonitoringServer()
        data = DashboardData(timestamp="2025-06-15 12:30:00")
        html = server.render_dashboard(data)
        assert "2025-06-15 12:30:00" in html


# =============================================================================
# MonitoringServer — HTTP handlers
# =============================================================================


class TestHandleIndex:
    async def test_returns_html_response(self):
        server = MonitoringServer()
        server.get_dashboard_data = AsyncMock(
            return_value=DashboardData(timestamp="2025-01-01 00:00:00")
        )

        request = MagicMock()
        response = await server.handle_index(request)

        assert response.content_type == "text/html"
        assert "Pipeline Monitor" in response.text


class TestHandleApiStats:
    async def test_returns_json_with_expected_keys(self):
        server = MonitoringServer()
        dashboard = DashboardData(
            timestamp="2025-01-01 00:00:00",
            topics={"t": TopicStats(name="t", total_lag=5)},
            workers={"w": WorkerStats(name="W", connected=True)},
            metrics_error=None,
            kafka_error="oops",
        )
        server.get_dashboard_data = AsyncMock(return_value=dashboard)

        request = MagicMock()
        response = await server.handle_api_stats(request)

        import json

        body = json.loads(response.body)

        assert body["timestamp"] == "2025-01-01 00:00:00"
        assert "t" in body["topics"]
        assert body["topics"]["t"]["total_lag"] == 5
        assert "w" in body["workers"]
        assert body["workers"]["w"]["connected"] is True
        assert body["errors"]["metrics"] is None
        assert body["errors"]["kafka"] == "oops"


# =============================================================================
# MonitoringServer — create_app
# =============================================================================


class TestCreateApp:
    def test_creates_app_with_routes(self):
        server = MonitoringServer()
        app = server.create_app()

        route_paths = [r.resource.canonical for r in app.router.routes()]
        assert "/" in route_paths
        assert "/api/stats" in route_paths


# =============================================================================
# MonitoringServer — stop
# =============================================================================


class TestMonitoringServerStop:
    async def test_stop_closes_open_session(self):
        server = MonitoringServer()
        mock_session = AsyncMock()
        mock_session.closed = False
        server._session = mock_session

        await server.stop()

        mock_session.close.assert_awaited_once()

    async def test_stop_skips_closed_session(self):
        server = MonitoringServer()
        mock_session = AsyncMock()
        mock_session.closed = True
        server._session = mock_session

        await server.stop()

        mock_session.close.assert_not_awaited()

    async def test_stop_with_no_session(self):
        server = MonitoringServer()
        server._session = None
        server._runner = None

        # Should not raise
        await server.stop()

    async def test_stop_cleans_up_runner(self):
        server = MonitoringServer()
        server._session = None
        mock_runner = AsyncMock()
        server._runner = mock_runner

        await server.stop()

        mock_runner.cleanup.assert_awaited_once()
