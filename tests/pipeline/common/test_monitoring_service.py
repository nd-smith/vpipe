"""
Unit tests for the monitoring aggregator service.

Test Coverage:
    - PrometheusClient: query, get_worker_status, get_consumer_lag,
      check_prometheus_health, session management, close
    - MonitoringService: HTTP handlers for /health, /health/workers,
      /health/prometheus, /status
    - Edge cases: empty workers, down workers, exception handling,
      lag aggregation, worker-to-consumer-group matching
"""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from pipeline.common.monitoring_service import (
    MonitoringService,
    PrometheusClient,
)


# =============================================================================
# PrometheusClient — init and session management
# =============================================================================


class TestPrometheusClientInit:

    def test_strips_trailing_slash_from_url(self):
        client = PrometheusClient("http://prom:9090/")
        assert client.prometheus_url == "http://prom:9090"
        assert client.query_url == "http://prom:9090/api/v1/query"

    def test_preserves_url_without_trailing_slash(self):
        client = PrometheusClient("http://prom:9090")
        assert client.prometheus_url == "http://prom:9090"

    def test_session_starts_as_none(self):
        client = PrometheusClient("http://prom:9090")
        assert client._session is None


class TestPrometheusClientEnsureSession:

    async def test_creates_session_when_none(self):
        client = PrometheusClient("http://prom:9090")
        session = client._ensure_session()
        assert session is not None
        assert client._session is session
        await session.close()

    async def test_reuses_open_session(self):
        client = PrometheusClient("http://prom:9090")
        session1 = client._ensure_session()
        session2 = client._ensure_session()
        assert session1 is session2
        await session1.close()

    async def test_creates_new_session_when_previous_closed(self):
        client = PrometheusClient("http://prom:9090")
        session1 = client._ensure_session()
        await session1.close()

        session2 = client._ensure_session()
        assert session2 is not session1
        assert not session2.closed
        await session2.close()


class TestPrometheusClientClose:

    async def test_closes_open_session(self):
        client = PrometheusClient("http://prom:9090")
        mock_session = AsyncMock()
        mock_session.closed = False
        client._session = mock_session

        await client.close()

        mock_session.close.assert_awaited_once()

    async def test_skips_when_session_already_closed(self):
        client = PrometheusClient("http://prom:9090")
        mock_session = AsyncMock()
        mock_session.closed = True
        client._session = mock_session

        await client.close()

        mock_session.close.assert_not_awaited()

    async def test_skips_when_no_session(self):
        client = PrometheusClient("http://prom:9090")
        client._session = None

        # Should not raise
        await client.close()


# =============================================================================
# PrometheusClient — query
# =============================================================================


class TestPrometheusClientQuery:

    async def test_returns_data_on_success(self):
        client = PrometheusClient("http://prom:9090")

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.raise_for_status = MagicMock()
        mock_response.json = AsyncMock(return_value={
            "status": "success",
            "data": {"resultType": "vector", "result": []},
        })
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_response)
        client._ensure_session = MagicMock(return_value=mock_session)

        result = await client.query("up")

        assert result == {"resultType": "vector", "result": []}
        mock_session.get.assert_called_once_with(
            "http://prom:9090/api/v1/query", params={"query": "up"}
        )

    async def test_raises_on_non_success_status(self):
        client = PrometheusClient("http://prom:9090")

        mock_response = AsyncMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json = AsyncMock(return_value={
            "status": "error",
            "error": "bad query",
        })
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_response)
        client._ensure_session = MagicMock(return_value=mock_session)

        with pytest.raises(Exception, match="Prometheus query failed"):
            await client.query("bad_query")


# =============================================================================
# PrometheusClient — get_worker_status
# =============================================================================


class TestPrometheusClientGetWorkerStatus:

    async def test_returns_workers_from_up_metric(self):
        client = PrometheusClient("http://prom:9090")
        client.query = AsyncMock(return_value={
            "result": [
                {
                    "metric": {
                        "job": "download_worker",
                        "instance": "worker-1:8000",
                        "domain": "verisk",
                        "worker_type": "download",
                    },
                    "value": [1234567890, "1"],
                },
                {
                    "metric": {
                        "job": "upload_worker",
                        "instance": "worker-2:8000",
                        "domain": "verisk",
                        "worker_type": "upload",
                    },
                    "value": [1234567890, "0"],
                },
            ]
        })

        workers = await client.get_worker_status()

        assert len(workers) == 2
        assert workers[0]["job"] == "download_worker"
        assert workers[0]["status"] == "up"
        assert workers[0]["domain"] == "verisk"
        assert workers[1]["job"] == "upload_worker"
        assert workers[1]["status"] == "down"

    async def test_returns_empty_list_when_no_results(self):
        client = PrometheusClient("http://prom:9090")
        client.query = AsyncMock(return_value={"result": []})

        workers = await client.get_worker_status()

        assert workers == []

    async def test_defaults_to_unknown_for_missing_labels(self):
        client = PrometheusClient("http://prom:9090")
        client.query = AsyncMock(return_value={
            "result": [
                {
                    "metric": {},
                    "value": [1234567890, "1"],
                }
            ]
        })

        workers = await client.get_worker_status()

        assert workers[0]["job"] == "unknown"
        assert workers[0]["instance"] == "unknown"
        assert workers[0]["domain"] == "unknown"
        assert workers[0]["worker_type"] == "unknown"


# =============================================================================
# PrometheusClient — get_consumer_lag
# =============================================================================


class TestPrometheusClientGetConsumerLag:

    async def test_returns_lag_entries(self):
        client = PrometheusClient("http://prom:9090")
        client.query = AsyncMock(return_value={
            "result": [
                {
                    "metric": {
                        "consumer_group": "verisk-download",
                        "topic": "verisk-downloads-pending",
                        "partition": "0",
                    },
                    "value": [1234567890, "42"],
                },
            ]
        })

        lags = await client.get_consumer_lag()

        assert len(lags) == 1
        assert lags[0]["consumer_group"] == "verisk-download"
        assert lags[0]["topic"] == "verisk-downloads-pending"
        assert lags[0]["partition"] == 0
        assert lags[0]["lag"] == 42

    async def test_returns_empty_list_when_no_results(self):
        client = PrometheusClient("http://prom:9090")
        client.query = AsyncMock(return_value={"result": []})

        lags = await client.get_consumer_lag()

        assert lags == []

    async def test_defaults_for_missing_labels(self):
        client = PrometheusClient("http://prom:9090")
        client.query = AsyncMock(return_value={
            "result": [
                {
                    "metric": {},
                    "value": [1234567890, "10"],
                }
            ]
        })

        lags = await client.get_consumer_lag()

        assert lags[0]["consumer_group"] == "unknown"
        assert lags[0]["topic"] == "unknown"
        assert lags[0]["partition"] == 0
        assert lags[0]["lag"] == 10


# =============================================================================
# PrometheusClient — check_prometheus_health
# =============================================================================


class TestPrometheusClientCheckHealth:

    async def test_returns_true_when_healthy(self):
        client = PrometheusClient("http://prom:9090")

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_response)
        client._ensure_session = MagicMock(return_value=mock_session)

        result = await client.check_prometheus_health()

        assert result is True
        mock_session.get.assert_called_once_with("http://prom:9090/-/healthy")

    async def test_returns_false_when_not_200(self):
        client = PrometheusClient("http://prom:9090")

        mock_response = AsyncMock()
        mock_response.status = 503
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_response)
        client._ensure_session = MagicMock(return_value=mock_session)

        result = await client.check_prometheus_health()

        assert result is False

    async def test_returns_false_on_exception(self):
        client = PrometheusClient("http://prom:9090")

        mock_session = MagicMock()
        mock_session.get = MagicMock(side_effect=Exception("connection refused"))
        client._ensure_session = MagicMock(return_value=mock_session)

        result = await client.check_prometheus_health()

        assert result is False


# =============================================================================
# MonitoringService — HTTP endpoint tests using aiohttp test client
# =============================================================================


def _make_mock_prometheus():
    """Create a mock PrometheusClient with all methods as AsyncMocks."""
    mock = MagicMock(spec=PrometheusClient)
    mock.get_worker_status = AsyncMock()
    mock.get_consumer_lag = AsyncMock()
    mock.check_prometheus_health = AsyncMock()
    mock.prometheus_url = "http://prom:9090"
    mock.close = AsyncMock()
    return mock


@pytest.fixture
def mock_prometheus():
    return _make_mock_prometheus()


@pytest.fixture
async def client(mock_prometheus):
    service = MonitoringService(mock_prometheus, port=0)
    async with TestClient(TestServer(service.app)) as tc:
        yield tc, mock_prometheus


# =============================================================================
# /health endpoint
# =============================================================================


class TestHandleHealth:

    async def test_returns_200_when_all_workers_up(self, client):
        tc, prom = client
        prom.get_worker_status.return_value = [
            {"job": "worker1", "instance": "w1:8000", "status": "up"},
            {"job": "worker2", "instance": "w2:8000", "status": "up"},
        ]

        resp = await tc.get("/health")
        assert resp.status == 200

        body = await resp.json()
        assert body["status"] == "healthy"
        assert body["workers_total"] == 2
        assert body["workers_up"] == 2
        assert body["workers_down"] == 0
        assert "down_workers" not in body

    async def test_returns_503_when_some_workers_down(self, client):
        tc, prom = client
        prom.get_worker_status.return_value = [
            {"job": "worker1", "instance": "w1:8000", "status": "up"},
            {"job": "worker2", "instance": "w2:8000", "status": "down"},
        ]

        resp = await tc.get("/health")
        assert resp.status == 503

        body = await resp.json()
        assert body["status"] == "unhealthy"
        assert body["workers_down"] == 1
        assert len(body["down_workers"]) == 1
        assert body["down_workers"][0]["job"] == "worker2"

    async def test_returns_503_when_no_workers_found(self, client):
        tc, prom = client
        prom.get_worker_status.return_value = []

        resp = await tc.get("/health")
        assert resp.status == 503

        body = await resp.json()
        assert body["status"] == "unknown"
        assert "No workers found" in body["message"]

    async def test_returns_503_on_prometheus_error(self, client):
        tc, prom = client
        prom.get_worker_status.side_effect = Exception("connection refused")

        resp = await tc.get("/health")
        assert resp.status == 503

        body = await resp.json()
        assert body["status"] == "error"
        assert "connection refused" in body["message"]


# =============================================================================
# /health/workers endpoint
# =============================================================================


class TestHandleWorkersHealth:

    async def test_returns_200_with_worker_details(self, client):
        tc, prom = client
        prom.get_worker_status.return_value = [
            {"job": "w1", "instance": "i1", "domain": "verisk", "worker_type": "download", "status": "up"},
            {"job": "w2", "instance": "i2", "domain": "verisk", "worker_type": "upload", "status": "down"},
        ]

        resp = await tc.get("/health/workers")
        assert resp.status == 200

        body = await resp.json()
        assert body["total"] == 2
        assert body["up"] == 1
        assert body["down"] == 1
        assert len(body["workers"]) == 2

    async def test_returns_503_on_exception(self, client):
        tc, prom = client
        prom.get_worker_status.side_effect = Exception("query timeout")

        resp = await tc.get("/health/workers")
        assert resp.status == 503

        body = await resp.json()
        assert "query timeout" in body["error"]


# =============================================================================
# /health/prometheus endpoint
# =============================================================================


class TestHandlePrometheusHealth:

    async def test_returns_200_when_prometheus_healthy(self, client):
        tc, prom = client
        prom.check_prometheus_health.return_value = True

        resp = await tc.get("/health/prometheus")
        assert resp.status == 200

        body = await resp.json()
        assert body["status"] == "healthy"
        assert body["prometheus_url"] == "http://prom:9090"

    async def test_returns_503_when_prometheus_unhealthy(self, client):
        tc, prom = client
        prom.check_prometheus_health.return_value = False

        resp = await tc.get("/health/prometheus")
        assert resp.status == 503

        body = await resp.json()
        assert body["status"] == "unhealthy"

    async def test_returns_503_on_exception(self, client):
        tc, prom = client
        prom.check_prometheus_health.side_effect = Exception("network error")

        resp = await tc.get("/health/prometheus")
        assert resp.status == 503

        body = await resp.json()
        assert body["status"] == "error"
        assert "network error" in body["message"]


# =============================================================================
# /status endpoint
# =============================================================================


class TestHandleStatus:

    async def test_returns_workers_and_lag(self, client):
        tc, prom = client
        prom.get_worker_status.return_value = [
            {"job": "w1", "instance": "i1", "domain": "verisk", "worker_type": "download", "status": "up"},
        ]
        prom.get_consumer_lag.return_value = [
            {"consumer_group": "verisk-download", "topic": "pending", "partition": 0, "lag": 10},
            {"consumer_group": "verisk-download", "topic": "pending", "partition": 1, "lag": 5},
        ]

        resp = await tc.get("/status")
        assert resp.status == 200

        body = await resp.json()
        assert body["total_workers"] == 1
        assert body["workers_up"] == 1
        assert body["workers_down"] == 0

        # Check consumer group aggregation
        assert len(body["consumer_groups"]) == 1
        cg = body["consumer_groups"][0]
        assert cg["consumer_group"] == "verisk-download"
        assert cg["total_lag"] == 15

    async def test_matches_worker_to_consumer_group(self, client):
        tc, prom = client
        prom.get_worker_status.return_value = [
            {"job": "w1", "instance": "i1", "domain": "verisk", "worker_type": "download", "status": "up"},
        ]
        prom.get_consumer_lag.return_value = [
            {"consumer_group": "verisk-download", "topic": "pending", "partition": 0, "lag": 7},
        ]

        resp = await tc.get("/status")
        body = await resp.json()

        worker = body["workers"][0]
        assert worker["consumer_lag"] is not None
        assert worker["consumer_lag"]["total_lag"] == 7

    async def test_worker_without_matching_consumer_group_has_null_lag(self, client):
        tc, prom = client
        prom.get_worker_status.return_value = [
            {"job": "w1", "instance": "i1", "domain": "verisk", "worker_type": "download", "status": "up"},
        ]
        prom.get_consumer_lag.return_value = [
            {"consumer_group": "unrelated-group", "topic": "t", "partition": 0, "lag": 99},
        ]

        resp = await tc.get("/status")
        body = await resp.json()

        worker = body["workers"][0]
        assert worker["consumer_lag"] is None

    async def test_aggregates_lag_per_topic_within_consumer_group(self, client):
        tc, prom = client
        prom.get_worker_status.return_value = []
        prom.get_consumer_lag.return_value = [
            {"consumer_group": "cg1", "topic": "topicA", "partition": 0, "lag": 10},
            {"consumer_group": "cg1", "topic": "topicA", "partition": 1, "lag": 20},
            {"consumer_group": "cg1", "topic": "topicB", "partition": 0, "lag": 5},
        ]

        resp = await tc.get("/status")
        body = await resp.json()

        cg = body["consumer_groups"][0]
        assert cg["total_lag"] == 35

        topics = {t["topic"]: t for t in cg["topics"]}
        assert topics["topicA"]["total_lag"] == 30
        assert len(topics["topicA"]["partitions"]) == 2
        assert topics["topicB"]["total_lag"] == 5
        assert len(topics["topicB"]["partitions"]) == 1

    async def test_multiple_consumer_groups(self, client):
        tc, prom = client
        prom.get_worker_status.return_value = []
        prom.get_consumer_lag.return_value = [
            {"consumer_group": "cg1", "topic": "t1", "partition": 0, "lag": 10},
            {"consumer_group": "cg2", "topic": "t2", "partition": 0, "lag": 20},
        ]

        resp = await tc.get("/status")
        body = await resp.json()

        assert len(body["consumer_groups"]) == 2
        groups_by_name = {cg["consumer_group"]: cg for cg in body["consumer_groups"]}
        assert groups_by_name["cg1"]["total_lag"] == 10
        assert groups_by_name["cg2"]["total_lag"] == 20

    async def test_returns_503_on_exception(self, client):
        tc, prom = client
        prom.get_worker_status.side_effect = Exception("boom")

        resp = await tc.get("/status")
        assert resp.status == 503

        body = await resp.json()
        assert "boom" in body["error"]

    async def test_empty_workers_and_empty_lag(self, client):
        tc, prom = client
        prom.get_worker_status.return_value = []
        prom.get_consumer_lag.return_value = []

        resp = await tc.get("/status")
        assert resp.status == 200

        body = await resp.json()
        assert body["workers"] == []
        assert body["total_workers"] == 0
        assert body["consumer_groups"] == []


# =============================================================================
# MonitoringService — app creation
# =============================================================================


class TestMonitoringServiceInit:

    def test_creates_app_with_all_routes(self):
        prom = _make_mock_prometheus()
        service = MonitoringService(prom, port=9091)

        route_paths = [r.resource.canonical for r in service.app.router.routes()]
        assert "/health" in route_paths
        assert "/health/workers" in route_paths
        assert "/health/prometheus" in route_paths
        assert "/status" in route_paths

    def test_stores_port(self):
        prom = _make_mock_prometheus()
        service = MonitoringService(prom, port=4444)
        assert service.port == 4444

    def test_stores_prometheus_client(self):
        prom = _make_mock_prometheus()
        service = MonitoringService(prom)
        assert service.prometheus is prom
