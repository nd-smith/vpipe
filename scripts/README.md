# Pipeline Scripts

Utility scripts for code quality, analysis, and maintenance.

## check_awaits.py

Scans Python code for potential missing `await` keywords on async function calls.

### Usage

```bash
# Scan entire src directory
python scripts/check_awaits.py src

# Scan specific directory
python scripts/check_awaits.py src/kafka_pipeline/claimx

# Scan single file
python scripts/check_awaits.py src/kafka_pipeline/claimx/workers/enrichment_worker.py
```

### What it detects

The script uses AST analysis to find:
- Method calls in async functions that aren't wrapped in `await`
- Common async patterns like `send()`, `process()`, `execute()`, `record_*()`
- Expression statements that might be unawaited coroutines

### Note on false positives

The script may report false positives for:
- Synchronous methods with names that sound async (e.g., `record_success()`)
- Methods that return non-awaitable objects
- Library code with intentional fire-and-forget patterns

Always verify findings manually or use mypy for definitive checking.

### Integration with CI/CD

Add to your CI pipeline:

```yaml
- name: Check for missing awaits
  run: python scripts/check_awaits.py src
```

### Best practice

Use mypy with `warn_unawaited = true` for authoritative detection:

```bash
mypy src
```

See `pyproject.toml` for mypy configuration.
