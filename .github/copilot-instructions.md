# Copilot instructions for wpipe

## Big picture architecture
- This is an async, event-driven worker pipeline with two core domains: `verisk` (XACT) and `claimx`.
- Main entrypoint is `src/pipeline/__main__.py`; worker names map to runner functions via `src/pipeline/runners/registry.py`.
- Runners in `src/pipeline/runners/{verisk_runners,claimx_runners,plugin_runners}.py` construct workers and apply shared lifecycle behavior.
- Shared lifecycle logic lives in `src/pipeline/runners/common.py` (`execute_worker_with_shutdown`, startup retry, error-mode health handling).
- Typical flow is ingest -> enrich -> download -> upload -> result/delta writers, with retry scheduling (`pipeline/common/retry/unified_scheduler.py`).

## Transport and config rules (important)
- Internal transport is selected by `PIPELINE_TRANSPORT`; default is Event Hub AMQP (`eventhub`), Kafka is fallback (`src/pipeline/common/transport.py`).
- Source Event Hub can be separate from internal pipeline Event Hubs (`SOURCE_EVENTHUB_NAMESPACE_CONNECTION_STRING`).
- Config comes from `src/config/config.yaml` with env expansion support in code (`config.config._expand_env_vars`).
- `PipelineConfig` (`src/config/pipeline_config.py`) layers env vars over YAML for table paths and toggles.
- When touching topic/entity routing, update config + transport resolution paths together (domain/topic key/consumer group resolution).

## Worker implementation conventions
- Workers are long-running async services with `start()`/`stop()` and usually an attached `HealthCheckServer`.
- Health endpoints are standardized: `/health/live` and `/health/ready` (`src/pipeline/common/health.py`).
- Fatal startup/runtime errors should keep health alive in error mode instead of hard exit (see runner common helpers).
- Multi-instance scaling uses `--count` and `instance_id`; preserve per-instance log/worker identity propagation.
- For new workers: implement worker class, add runner function, register in `WORKER_REGISTRY`, then wire config/topic keys.

## Logging and observability conventions
- Use standard `logging` with structured `extra={...}` fields; avoid custom logging wrappers (`docs/LOGGING.md`).
- Keep correlation context (`trace_id`, domain/stage context) for cross-worker tracing.
- `__main__.py` starts health server early, initializes telemetry, and starts Prometheus metrics server (`--metrics-port`).
- Respect log output mode flags (`--log-to-stdout`, `LOG_TO_STDOUT`) and existing logging setup functions in `core/logging/setup.py`.

## Developer workflows
- Run workers from `src/`: `python -m pipeline --worker <name>` or `python -m pipeline` for all.
- Useful local flags: `--dev`, `--no-delta`, `--count N`, `--log-level DEBUG`.
- Plugin workers are separate module entrypoints (examples in `docs/QUICKSTART.md` and `docs/COMPLETE_WORKER_LIST.md`).
- Tests run with pytest from repo root; config is in both `pytest.ini` and `[tool.pytest.ini_options]` in `pyproject.toml`.
- Common markers: `integration`, `performance`, `slow`, `extended`; default asyncio mode is `auto`.

## Code style expectations from this repo
- Follow `CLAUDE.md`: prioritize simple/explicit code; avoid broad refactors unless requested.
- Prefer small, readable changes over abstractions; keep behavior-focused edits localized.
- Do not introduce unnecessary helper layers around logging, worker lifecycle, or config access.
