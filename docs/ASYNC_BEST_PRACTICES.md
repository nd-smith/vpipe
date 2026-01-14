# Async/Await Best Practices

## Critical Issue: Missing await on Async Calls

### The Problem

Calling an async function without `await` creates a coroutine object but doesn't execute it. This causes:

1. **Silent failures** - The function never runs, but no error is raised
2. **Resource leaks** - Cleanup code doesn't execute
3. **Data loss** - Metrics, logs, or database operations are skipped
4. **Debugging nightmares** - The code "looks" fine but doesn't work

### Example Bug

```python
# WRONG - Missing await
async def process_event(event):
    result = await enrich_event(event)
    self.metrics.record_enrichment(result)  # BUG! Never executes
    return result

# CORRECT - With await
async def process_event(event):
    result = await enrich_event(event)
    await self.metrics.record_enrichment(result)  # Executes properly
    return result
```

### Why Python Doesn't Catch This

Python only warns about unawaited coroutines at **module exit**, not at call time.

## Detection & Prevention

### 1. Static Type Checking (Recommended)

Enable mypy with `warn_unawaited = true`:

```toml
# pyproject.toml
[tool.mypy]
strict = true
warn_unawaited = true
```

Run mypy in CI:

```bash
mypy src
```

### 2. Runtime Detection

Our custom scanner detects suspicious patterns:

```bash
python scripts/check_awaits.py src
```

### 3. Code Review Checklist

- [ ] Every `async def` function is called with `await`
- [ ] No standalone function calls in async contexts
- [ ] Fire-and-forget patterns use `asyncio.create_task()` explicitly
- [ ] Cleanup methods (`close()`, `stop()`) are awaited

## Common Patterns

### Background Tasks

If you intentionally don't want to wait, use `create_task()`:

```python
# Explicit fire-and-forget (OK if intentional)
async def start_background_work():
    task = asyncio.create_task(long_running_operation())
    # Track task for cleanup
    self._background_tasks.add(task)
    task.add_done_callback(self._background_tasks.discard)
```

### Context Managers

Always await async context managers:

```python
# WRONG
async with session.get(url):  # Missing await
    ...

# CORRECT
async with await session.get(url) as response:
    ...
```

## References

- [PEP 492 - Coroutines with async and await](https://peps.python.org/pep-0492/)
- [Python asyncio documentation](https://docs.python.org/3/library/asyncio.html)
- [mypy async checking](https://mypy.readthedocs.io/en/stable/cheat_sheet_py3.html#coroutines-and-asyncio)
