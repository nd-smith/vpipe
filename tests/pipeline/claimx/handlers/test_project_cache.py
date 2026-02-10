"""Tests for ClaimX project cache."""

from pipeline.claimx.handlers.project_cache import ProjectCache


class TestProjectCache:
    def test_cache_starts_empty(self):
        cache = ProjectCache()
        assert cache.size() == 0

    def test_has_returns_false_for_missing(self):
        cache = ProjectCache()
        assert cache.has("123") is False

    def test_has_returns_true_after_add(self):
        cache = ProjectCache()
        cache.add("123")
        assert cache.has("123") is True

    def test_add_increases_size(self):
        cache = ProjectCache()
        cache.add("123")
        assert cache.size() == 1
        cache.add("456")
        assert cache.size() == 2

    def test_add_same_id_twice_does_not_increase_size(self):
        cache = ProjectCache()
        cache.add("123")
        cache.add("123")
        assert cache.size() == 1

    def test_clear_removes_all_entries(self):
        cache = ProjectCache()
        cache.add("123")
        cache.add("456")
        cache.clear()
        assert cache.size() == 0
        assert cache.has("123") is False

    def test_load_from_ids_adds_entries(self):
        cache = ProjectCache()
        loaded = cache.load_from_ids(["100", "200", "300"])
        assert loaded == 3
        assert cache.size() == 3
        assert cache.has("100") is True
        assert cache.has("200") is True
        assert cache.has("300") is True

    def test_load_from_ids_returns_new_count_only(self):
        cache = ProjectCache()
        cache.add("100")
        loaded = cache.load_from_ids(["100", "200", "300"])
        assert loaded == 2
        assert cache.size() == 3

    def test_load_from_ids_with_empty_list(self):
        cache = ProjectCache()
        loaded = cache.load_from_ids([])
        assert loaded == 0
        assert cache.size() == 0

    def test_load_from_ids_converts_to_string(self):
        cache = ProjectCache()
        loaded = cache.load_from_ids([100, 200])
        assert loaded == 2
        assert cache.has("100") is True
        assert cache.has("200") is True

    def test_load_from_ids_handles_duplicates(self):
        cache = ProjectCache()
        loaded = cache.load_from_ids(["100", "100", "200"])
        assert loaded == 2
        assert cache.size() == 2
