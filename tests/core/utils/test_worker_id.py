"""Tests for core.utils.worker_id module."""

from core.utils.worker_id import generate_worker_id


class TestGenerateWorkerId:
    def test_no_prefix(self):
        worker_id = generate_worker_id()
        assert isinstance(worker_id, str)
        assert len(worker_id) > 0
        assert "-" in worker_id

    def test_with_prefix(self):
        worker_id = generate_worker_id("claimx-ingester")
        assert worker_id.startswith("claimx-ingester-")
        suffix = worker_id.removeprefix("claimx-ingester-")
        assert len(suffix) > 0

    def test_unique(self):
        ids = {generate_worker_id() for _ in range(10)}
        assert len(ids) == 10

    def test_empty_prefix_treated_as_no_prefix(self):
        worker_id = generate_worker_id("")
        # Empty prefix is falsy, so no prefix prepended
        assert not worker_id.startswith("-")
