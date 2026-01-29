# High Impact Changes - Execution Plan

**Priority:** Execute these changes first for maximum impact with minimal risk

## 1. Merge Duplicate JSON Serializer ⚡
**Impact:** HIGH | **Risk:** LOW | **LOC Reduction:** 35 lines

**Files:**
- Source: `src/core/logging/formatters.py:20-54`
- Duplicate: `src/kafka_pipeline/common/producer.py:16-19`
- New: `src/core/utils/json_serializer.py` (create)

**Tasks:**
1. Create `core/utils/` directory
2. Extract JSON serializer to `core/utils/json_serializer.py`
3. Update imports in formatters.py
4. Update imports in producer.py
5. Run tests to verify no regressions

**Agent Assignment:** agent-1-json-serializer

---

## 2. Remove Unnecessary Test Docstrings ⚡
**Impact:** HIGH | **Risk:** NONE | **LOC Reduction:** ~1,000 lines

**Strategy:**
Remove "Should..." docstrings from tests where the function name is self-documenting.

**Keep docstrings for:**
- Complex test scenarios
- Non-obvious test setup
- Security edge cases
- Integration test workflows

**Remove docstrings from:**
- Simple validation tests (e.g., `test_valid_url()`)
- Obvious edge cases (e.g., `test_empty_string()`)
- Type conversion tests
- Standard CRUD operations

**Target files (sample):**
- `tests/core/security/test_url_validation.py`
- `tests/core/resilience/test_retry.py`
- `tests/kafka_pipeline/claimx/workers/test_upload_worker.py`

**Agent Assignment:** agent-2-test-docstrings

---

## 3. Consolidate Validation Patterns ⚡
**Impact:** HIGH | **Risk:** MEDIUM | **LOC Reduction:** 50-70 lines

**File:** `src/config/config.py:260-293`

**Current pattern (repeated ~4 times):**
```python
def _validate_consumer_settings(...)
def _validate_producer_settings(...)
def _validate_admin_settings(...)
```

**Target pattern:**
```python
def _validate_kafka_settings(settings, context, setting_type)
```

**Tasks:**
1. Create generic `_validate_kafka_settings()` method
2. Extract common validation logic
3. Update all callers to use generic method
4. Remove duplicate validation methods
5. Ensure all tests pass

**Agent Assignment:** agent-3-validation-consolidation

---

## 4. Replace Assertions with Proper Error Handling ⚡
**Impact:** MEDIUM | **Risk:** LOW | **LOC Changed:** ~15 assertions

**Files:**
- `src/kafka_pipeline/claimx/api_client.py:238`
- `src/kafka_pipeline/claimx/workers/download_worker.py:800, 904`
- `src/kafka_pipeline/claimx/workers/upload_worker.py:477, 551-552, 612, 642`

**Pattern:**
```python
# BEFORE
assert self._session is not None
assert self.retry_handler is not None, "RetryHandler not initialized"

# AFTER
if self._session is None:
    raise RuntimeError("Session not initialized - call _ensure_session() first")
if self.retry_handler is None:
    raise RuntimeError("RetryHandler not initialized - call setup() first")
```

**Agent Assignment:** agent-4-assertions

---

## Execution Order

### Parallel Track (No Dependencies)
Launch all 4 agents simultaneously:

1. **Agent 1:** Merge JSON serializer (15 min)
2. **Agent 2:** Remove test docstrings (30 min)
3. **Agent 3:** Consolidate validation patterns (45 min)
4. **Agent 4:** Replace assertions (20 min)

**Total Time:** ~45 minutes (parallel execution)
**Expected LOC Reduction:** ~1,100+ lines

### Testing Strategy
After all agents complete:
1. Run full test suite: `pytest tests/`
2. Check for any import errors
3. Verify no functionality regressions
4. Review git diff for unintended changes

---

## Success Criteria

- [ ] All tests pass (3,562 assertions verified)
- [ ] No import errors
- [ ] Git diff shows only intended changes
- [ ] Code complexity reduced
- [ ] Zero functionality regressions

---

## Rollback Plan

Each agent creates separate commits:
- `git revert <commit-hash>` to undo individual changes
- Independent changes allow selective rollback
