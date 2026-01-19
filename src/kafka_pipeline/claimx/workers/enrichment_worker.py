"""
ClaimX Enrichment Worker - Enriches events with API data and produces download tasks.

This worker is the core of the ClaimX enrichment pipeline:
1. Consumes ClaimXEnrichmentTask from enrichment pending topic
2. Routes events to appropriate handlers based on event_type
3. Handlers call ClaimX API to fetch entity data
4. Writes entity rows to Delta Lake tables
5. Produces ClaimXDownloadTask for media files with download URLs

Consumer group: {prefix}-claimx-enrichment-worker
Input topic: claimx.enrichment.pending
Output topic: claimx.entities.rows (entity data), claimx.downloads.pending (downloads)
Delta tables: claimx_projects, claimx_contacts, claimx_attachment_metadata, claimx_tasks,
              claimx_task_templates, claimx_external_links, claimx_video_collab
"""

import asyncio
import json
import uuid
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord, TopicPartition
from pydantic import ValidationError

from core.auth.kafka_oauth import create_kafka_oauth_callback
from core.logging.setup import get_logger
from core.logging.utilities import format_cycle_output, log_worker_error
from core.types import ErrorCategory
from config.config import KafkaConfig
from kafka_pipeline.common.metrics import (
    record_delta_write,
    record_message_consumed,
    update_connection_status,
    update_assigned_partitions,
)
from kafka_pipeline.common.producer import BaseKafkaProducer
from kafka_pipeline.claimx.api_client import ClaimXApiClient, ClaimXApiError
from kafka_pipeline.claimx.handlers import get_handler_registry, HandlerRegistry
from kafka_pipeline.claimx.handlers.project_cache import ProjectCache
from kafka_pipeline.common.storage.delta import DeltaTableReader
from kafka_pipeline.claimx.monitoring import HealthCheckServer
from kafka_pipeline.claimx.retry import EnrichmentRetryHandler
from kafka_pipeline.claimx.schemas.entities import EntityRowsMessage
from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage
from kafka_pipeline.claimx.schemas.tasks import (
    ClaimXEnrichmentTask,
    ClaimXDownloadTask,
)
from kafka_pipeline.plugins.shared.base import (
    Domain,
    PipelineStage,
    PluginContext,
)
from kafka_pipeline.plugins.shared.loader import load_plugins_from_directory
from kafka_pipeline.plugins.shared.registry import (
    ActionExecutor,
    PluginOrchestrator,
    get_plugin_registry,
)

logger = get_logger(__name__)


class ClaimXEnrichmentWorker:
    """
    Worker to consume ClaimX enrichment tasks and enrich them with API data.

    Architecture:
    - Event routing via handler registry
    - Single-task processing (no batching - delta writer handles batching)
    - Entity data writes to 7 Delta tables
    - Download task generation for media files
    - Background task tracking for graceful shutdown
    """

    def __init__(
        self,
        config: KafkaConfig,
        entity_writer: Any = None,
        domain: str = "claimx",
        enable_delta_writes: bool = True,
        enrichment_topic: str = "",
        download_topic: str = "",
        producer_config: Optional[KafkaConfig] = None,
        projects_table_path: str = "",
    ):
        self.consumer_config = config
        self.producer_config = producer_config if producer_config else config
        self.domain = domain
        self.enrichment_topic = enrichment_topic or config.get_topic(domain, "enrichment_pending")
        self.download_topic = download_topic or config.get_topic(domain, "downloads_pending")
        self.entity_rows_topic = config.get_topic(domain, "entities_rows")
        self.enable_delta_writes = enable_delta_writes
        
        self.consumer_group = config.get_consumer_group(domain, "enrichment_worker")
        self.processing_config = config.get_worker_config(domain, "enrichment_worker", "processing")
        self.max_poll_records = self.processing_config.get("max_poll_records", 100)

        self._retry_delays = config.get_retry_delays(domain)
        self._max_retries = config.get_max_retries(domain)

        retry_topics = [
            self._get_retry_topic(i) for i in range(len(self._retry_delays))
        ]
        self.topics = [self.enrichment_topic] + retry_topics

        self.producer: Optional[BaseKafkaProducer] = None
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.api_client: Optional[ClaimXApiClient] = None
        self.retry_handler: Optional[EnrichmentRetryHandler] = None

        self.handler_registry: HandlerRegistry = get_handler_registry()

        # Project cache prevents redundant API calls for in-flight verification
        cache_config = self.processing_config.get("project_cache", {})
        cache_ttl = cache_config.get("ttl_seconds", 1800)
        self._preload_cache_from_delta = cache_config.get("preload_from_delta", False)
        self.project_cache = ProjectCache(ttl_seconds=cache_ttl)

        self.plugin_registry = get_plugin_registry()
        self.plugin_orchestrator: Optional[PluginOrchestrator] = None
        self.action_executor: Optional[ActionExecutor] = None

        # port=0 for dynamic port assignment (avoids conflicts with multiple workers)
        health_port = self.processing_config.get("health_port", 0)
        health_enabled = self.processing_config.get("health_enabled", True)
        self.health_server = HealthCheckServer(
            port=health_port,
            worker_name="claimx-enricher",
            enabled=health_enabled,
        )

        self._pending_tasks: Set[asyncio.Task] = set()
        self._task_counter = 0
        self._consume_task: Optional[asyncio.Task] = None
        self._running = False

        self._records_processed = 0
        self._records_succeeded = 0
        self._records_failed = 0
        self._records_skipped = 0
        self._last_cycle_log = time.monotonic()
        self._cycle_count = 0
        self._cycle_task: Optional[asyncio.Task] = None

        self._projects_table_path = projects_table_path

        logger.info(
            "Initialized ClaimXEnrichmentWorker",
            extra={
                "domain": domain,
                "worker_name": "enrichment_worker",
                "consumer_group": config.get_consumer_group(domain, "enrichment_worker"),
                "topics": self.topics,
                "enrichment_topic": self.enrichment_topic,
                "download_topic": self.download_topic,
                "delta_writes_enabled": self.enable_delta_writes,
                "max_poll_records": self.max_poll_records,
                "retry_delays": self._retry_delays,
                "max_retries": self._max_retries,
                "project_cache_ttl": cache_ttl,
                "project_cache_preload": self._preload_cache_from_delta,
            },
        )

    async def _preload_project_cache(self) -> None:
        """Preload project cache with existing project IDs from Delta table to reduce API calls."""
        if not self._projects_table_path:
            logger.warning(
                "Cannot preload project cache - projects_table_path not configured"
            )
            return

        try:
            logger.info(
                "Preloading project cache from Delta",
                extra={"table_path": self._projects_table_path},
            )

            reader = DeltaTableReader(self._projects_table_path)
            if not reader.exists():
                logger.info("Projects table does not exist yet, skipping preload")
                return

            df = reader.read(columns=["project_id"])
            if df.is_empty():
                logger.info("Projects table is empty, skipping preload")
                return

            project_ids = df["project_id"].drop_nulls().unique().to_list()
            loaded_count = self.project_cache.load_from_ids(project_ids)

            logger.info(
                "Project cache preloaded from Delta",
                extra={
                    "table_path": self._projects_table_path,
                    "unique_project_ids": len(project_ids),
                    "loaded_to_cache": loaded_count,
                    "cache_size": self.project_cache.size(),
                },
            )

        except Exception as e:
            logger.warning(
                "Failed to preload project cache from Delta, continuing without preload",
                extra={
                    "table_path": self._projects_table_path,
                    "error": str(e)[:200],
                },
                exc_info=True,
            )

    async def start(self) -> None:
        if self._running:
            logger.warning("Worker already running")
            return

        logger.info("Starting ClaimXEnrichmentWorker")
        self._running = True

        from kafka_pipeline.common.telemetry import initialize_telemetry
        import os

        initialize_telemetry(
            service_name=f"{self.domain}-enrichment-worker",
            environment=os.getenv("ENVIRONMENT", "development"),
        )

        self._cycle_task = asyncio.create_task(self._periodic_cycle_output())

        await self.health_server.start()

        self.api_client = ClaimXApiClient(
            base_url=self.consumer_config.claimx_api_url,
            token=self.consumer_config.claimx_api_token,
            timeout_seconds=self.consumer_config.claimx_api_timeout_seconds,
            max_concurrent=self.consumer_config.claimx_api_concurrency,
        )
        await self.api_client._ensure_session()

        api_reachable = not self.api_client.is_circuit_open
        self.health_server.set_ready(
            kafka_connected=False,
            api_reachable=api_reachable,
        )

        self.producer = BaseKafkaProducer(
            config=self.producer_config,
            domain=self.domain,
            worker_name="enrichment_worker",
        )
        await self.producer.start()

        plugins_dir = self.processing_config.get(
            "plugins_dir",
            "config/plugins"
        )
        try:
            import os
            if os.path.exists(plugins_dir):
                loaded_plugins = load_plugins_from_directory(plugins_dir)
                logger.info(
                    "Loaded plugins from directory",
                    extra={
                        "plugins_dir": plugins_dir,
                        "plugins_loaded": len(loaded_plugins),
                        "plugin_names": [p.name for p in loaded_plugins],
                    },
                )
            else:
                logger.info(
                    "Plugin directory not found, continuing without plugins",
                    extra={"plugins_dir": plugins_dir},
                )
        except Exception as e:
            logger.warning(
                "Failed to load plugins, continuing without plugins",
                extra={
                    "plugins_dir": plugins_dir,
                    "error": str(e),
                },
                exc_info=True,
            )

        self.action_executor = ActionExecutor(
            producer=self.producer,
            http_client=None,
        )
        self.plugin_orchestrator = PluginOrchestrator(
            registry=self.plugin_registry,
            action_executor=self.action_executor,
        )
        logger.info(
            "Plugin orchestrator initialized",
            extra={
                "registered_plugins": len(self.plugin_registry.list_plugins()),
            },
        )

        for plugin in self.plugin_registry.list_plugins():
            try:
                await plugin.on_load()
                logger.debug(
                    "Plugin loaded",
                    extra={"plugin_name": plugin.name, "plugin_version": plugin.version},
                )
            except Exception as e:
                logger.error(
                    "Plugin on_load failed",
                    extra={"plugin_name": plugin.name, "error": str(e)},
                    exc_info=True,
                )

        self.retry_handler = EnrichmentRetryHandler(
            config=self.consumer_config,
            producer=self.producer,
        )
        logger.info(
            "Retry handler initialized",
            extra={
                "retry_topics": [t for t in self.topics if "retry" in t],
                "dlq_topic": self.retry_handler.dlq_topic,
            },
        )

        if self._preload_cache_from_delta:
            await self._preload_project_cache()

        try:
            self.consumer = self._create_consumer()
            await self.consumer.start()
            update_connection_status(True, self.consumer_config.get_consumer_group(self.domain, "enrichment_worker"))

            self.health_server.set_ready(
                kafka_connected=True,
                api_reachable=api_reachable,
                circuit_open=self.api_client.is_circuit_open,
            )

            self._consume_task = asyncio.create_task(self._consume_loop())
            await self._consume_task
            
        except asyncio.CancelledError:
            logger.info("Worker cancelled during startup/run")
            raise
        except Exception:
            self._running = False
            raise

    async def stop(self) -> None:
        logger.info("Stopping ClaimXEnrichmentWorker")
        self._running = False

        if self._cycle_task and not self._cycle_task.done():
            self._cycle_task.cancel()
            try:
                await self._cycle_task
            except asyncio.CancelledError:
                pass

        await self._wait_for_pending_tasks(timeout_seconds=30)

        for plugin in self.plugin_registry.list_plugins():
            try:
                await plugin.on_unload()
                logger.debug(
                    "Plugin unloaded",
                    extra={"plugin_name": plugin.name},
                )
            except Exception as e:
                logger.error(
                    "Plugin on_unload failed",
                    extra={"plugin_name": plugin.name, "error": str(e)},
                    exc_info=True,
                )

        if self.consumer:
            await self.consumer.stop()
            update_connection_status(False, self.consumer_config.get_consumer_group(self.domain, "enrichment_worker"))

        if self.producer:
            await self.producer.stop()

        if self.api_client:
            await self.api_client.close()

        await self.health_server.stop()

        logger.info("ClaimXEnrichmentWorker stopped successfully")

    async def request_shutdown(self) -> None:
        logger.info("Graceful shutdown requested")
        self._running = False

    def _create_consumer(self) -> AIOKafkaConsumer:
        group_id = self.consumer_config.get_consumer_group(self.domain, "enrichment_worker")

        consumer_config_dict = self.consumer_config.get_worker_config(
            self.domain, "enrichment_worker", "consumer"
        )

        common_args = {
            "bootstrap_servers": self.consumer_config.bootstrap_servers,
            "group_id": group_id,
            "enable_auto_commit": False,
            "auto_offset_reset": consumer_config_dict.get("auto_offset_reset", "earliest"),
            "metadata_max_age_ms": 30000,
            "session_timeout_ms": consumer_config_dict.get("session_timeout_ms", 45000),
            "max_poll_interval_ms": consumer_config_dict.get("max_poll_interval_ms", 600000),
            "request_timeout_ms": self.consumer_config.request_timeout_ms,
            "connections_max_idle_ms": self.consumer_config.connections_max_idle_ms,
        }

        if "heartbeat_interval_ms" in consumer_config_dict:
            common_args["heartbeat_interval_ms"] = consumer_config_dict["heartbeat_interval_ms"]

        if self.consumer_config.security_protocol != "PLAINTEXT":
            common_args["security_protocol"] = self.consumer_config.security_protocol
            common_args["sasl_mechanism"] = self.consumer_config.sasl_mechanism

            if self.consumer_config.sasl_mechanism == "OAUTHBEARER":
                common_args["sasl_oauth_token_provider"] = create_kafka_oauth_callback()

        return AIOKafkaConsumer(
            *self.topics,
            **common_args
        )

    async def _consume_loop(self) -> None:
        """
        Main consumption loop.

        CRITICAL (Issue #38): Commits offsets only after verifying all background tasks
        (entity row production) complete successfully. This prevents data loss from
        race conditions where offsets are committed before writes finish.
        """
        logger.info("Started consumption loop")

        try:
            while self._running:
                msg_dict = await self.consumer.getmany(
                    timeout_ms=1000,
                    max_records=self.max_poll_records
                )

                if not msg_dict:
                    continue

                for partition, messages in msg_dict.items():
                    for msg in messages:
                        await self._handle_enrichment_task(msg)

                # CRITICAL (Issue #38): Wait for all background tasks to complete before committing offsets.
                # This ensures entity rows are produced to Kafka before we advance the consumer offset.
                if self._pending_tasks:
                    await self._wait_for_pending_tasks(timeout_seconds=30)

                await self.consumer.commit()
                update_assigned_partitions(self.consumer_group, len(self.consumer.assignment()))

        except asyncio.CancelledError:
            logger.debug("Consumption loop cancelled")
            raise
        except Exception as e:
            logger.error(
                "Error in consumption loop",
                extra={"error": str(e)},
                exc_info=True
            )
            raise
        finally:
            logger.info("Consumption loop ended")


    def _create_tracked_task(
        self,
        coro,
        task_name: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> asyncio.Task:
        self._task_counter += 1
        full_name = f"{task_name}-{self._task_counter}"
        context = context or {}

        task = asyncio.create_task(coro, name=full_name)
        self._pending_tasks.add(task)

        logger.debug(
            "Background task created",
            extra={
                "task_name": full_name,
                "pending_tasks": len(self._pending_tasks),
                **context,
            },
        )

        def _on_task_done(t: asyncio.Task) -> None:
            self._pending_tasks.discard(t)

            if t.cancelled():
                logger.debug(
                    "Background task cancelled",
                    extra={
                        "task_name": t.get_name(),
                        "pending_tasks": len(self._pending_tasks),
                    },
                )
            elif t.exception() is not None:
                exc = t.exception()
                logger.error(
                    "Background task failed",
                    extra={
                        "task_name": t.get_name(),
                        "error": str(exc)[:200],
                        "pending_tasks": len(self._pending_tasks),
                        **context,
                    },
                )
            else:
                logger.debug(
                    "Background task completed",
                    extra={
                        "task_name": t.get_name(),
                        "pending_tasks": len(self._pending_tasks),
                    },
                )

        task.add_done_callback(_on_task_done)
        return task

    async def _wait_for_pending_tasks(self, timeout_seconds: float = 30) -> None:
        if not self._pending_tasks:
            return

        pending_count = len(self._pending_tasks)
        task_names = [t.get_name() for t in self._pending_tasks]

        logger.info(
            "Waiting for pending background tasks to complete",
            extra={
                "pending_count": pending_count,
                "task_names": task_names,
                "timeout_seconds": timeout_seconds,
            },
        )

        tasks_to_wait = list(self._pending_tasks)

        try:
            done, pending = await asyncio.wait(
                tasks_to_wait,
                timeout=timeout_seconds,
                return_when=asyncio.ALL_COMPLETED,
            )

            if pending:
                pending_names = [t.get_name() for t in pending]
                logger.warning(
                    "Cancelling background tasks that did not complete in time",
                    extra={
                        "pending_count": len(pending),
                        "pending_task_names": pending_names,
                        "timeout_seconds": timeout_seconds,
                    },
                )

                for task in pending:
                    task.cancel()

                await asyncio.gather(*pending, return_exceptions=True)

            completed_count = len(done)
            failed_count = sum(1 for t in done if t.exception() is not None)

            logger.info(
                "Background task cleanup complete",
                extra={
                    "completed": completed_count,
                    "failed": failed_count,
                    "cancelled": len(pending),
                },
            )

        except Exception as e:
            logger.error(
                "Error waiting for pending tasks",
                extra={"error": str(e)[:200]},
                exc_info=True,
            )

    async def _handle_enrichment_task(self, record: ConsumerRecord) -> None:
        """Process enrichment task. No batching at enricher level - delta writer handles batching."""
        try:
            message_data = json.loads(record.value.decode("utf-8"))
            task = ClaimXEnrichmentTask.model_validate(message_data)
        except (json.JSONDecodeError, ValidationError) as e:
            logger.error(
                "Failed to parse ClaimXEnrichmentTask",
                extra={
                    "topic": record.topic,
                    "partition": record.partition,
                    "offset": record.offset,
                    "error": str(e),
                },
                exc_info=True,
            )
            raise

        self._records_processed += 1

        logger.debug(
            "Processing enrichment task",
            extra={
                "event_id": task.event_id,
                "event_type": task.event_type,
                "project_id": task.project_id,
                "retry_count": task.retry_count,
            },
        )

        await self._process_single_task(task)

    async def _process_single_task(self, task: ClaimXEnrichmentTask) -> None:
        start_time = datetime.now(timezone.utc)

        if task.project_id:
            await self._ensure_projects_exist([task.project_id])

        event = ClaimXEventMessage(
            event_id=task.event_id,
            event_type=task.event_type,
            project_id=task.project_id,
            media_id=task.media_id,
            task_assignment_id=task.task_assignment_id,
            video_collaboration_id=task.video_collaboration_id,
            master_file_name=task.master_file_name,
            ingested_at=task.created_at,
        )

        handler_class = self.handler_registry.get_handler_class(event.event_type)
        if not handler_class:
            logger.warning(
                "No handler found for event",
                extra={
                    "event_id": event.event_id,
                    "event_type": event.event_type,
                },
            )
            self._records_skipped += 1
            return

        handler = handler_class(self.api_client, project_cache=self.project_cache)

        try:
            from opentelemetry import trace
            from opentelemetry.trace import SpanKind

            tracer = trace.get_tracer(__name__)
            with tracer.start_as_current_span("claimx.api.enrich", kind=SpanKind.CLIENT) as span:
                span.set_attribute("event.id", task.event_id)
                span.set_attribute("event.type", task.event_type)
                span.set_attribute("project.id", task.project_id)
                handler_result = await handler.process([event])

            entity_rows = handler_result.rows
            self._records_succeeded += 1

            if self.plugin_orchestrator:
                plugin_context = PluginContext(
                    domain=Domain.CLAIMX,
                    stage=PipelineStage.ENRICHMENT_COMPLETE,
                    message=task,
                    event_id=task.event_id,
                    event_type=task.event_type,
                    project_id=task.project_id,
                    data={
                        "entities": entity_rows,
                        "handler_result": handler_result,
                    },
                    headers={},
                )

                try:
                    orchestrator_result = await self.plugin_orchestrator.execute(plugin_context)

                    if orchestrator_result.terminated:
                        logger.info(
                            "Pipeline terminated by plugin",
                            extra={
                                "event_id": task.event_id,
                                "event_type": task.event_type,
                                "project_id": task.project_id,
                                "reason": orchestrator_result.termination_reason,
                                "plugin_results": orchestrator_result.success_count,
                            },
                        )
                        return

                    if orchestrator_result.actions_executed > 0:
                        logger.debug(
                            "Plugin actions executed",
                            extra={
                                "event_id": task.event_id,
                                "actions": orchestrator_result.actions_executed,
                                "plugins": orchestrator_result.success_count,
                            },
                        )

                except Exception as e:
                    logger.error(
                        "Plugin execution failed",
                        extra={
                            "event_id": task.event_id,
                            "error": str(e),
                        },
                        exc_info=True,
                    )

            if self.enable_delta_writes and not entity_rows.is_empty():
                self._create_tracked_task(
                    self._produce_entity_rows(entity_rows, [task]),
                    task_name="produce_entity_rows",
                    context={
                        "event_id": task.event_id,
                        "row_count": entity_rows.row_count(),
                    },
                )

            if entity_rows.media:
                download_tasks = self._create_download_tasks_from_media(entity_rows.media)
                if download_tasks:
                    await self._produce_download_tasks(download_tasks)

            elapsed_ms = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            logger.debug(
                "Enrichment task complete",
                extra={
                    "event_id": task.event_id,
                    "event_type": task.event_type,
                    "handler": handler_class.__name__,
                    "entity_rows": entity_rows.row_count(),
                    "api_calls": handler_result.api_calls,
                    "duration_ms": round(elapsed_ms, 2),
                },
            )

        except ClaimXApiError as e:
            log_worker_error(
                logger,
                "Handler failed with API error",
                event_id=task.event_id,
                error_category=e.category.value,
                exc=e,
                handler=handler_class.__name__,
            )
            record_processing_error(
                topic=self.enrichment_topic,
                consumer_group=self.consumer_group,
                error_category=e.category.value,
            )
            await self._handle_enrichment_failure(task, e, e.category)

        except Exception as e:
            error_category = ErrorCategory.UNKNOWN
            log_worker_error(
                logger,
                "Handler failed with unexpected error",
                event_id=task.event_id,
                error_category=error_category.value,
                exc=e,
                handler=handler_class.__name__,
                error_type=type(e).__name__,
            )
            record_processing_error(
                topic=self.enrichment_topic,
                consumer_group=self.consumer_group,
                error_category=error_category.value,
            )
            await self._handle_enrichment_failure(task, e, error_category)

    async def _periodic_cycle_output(self) -> None:
        logger.info(format_cycle_output(0, 0, 0, 0))
        self._last_cycle_log = time.monotonic()
        self._cycle_count = 0

        try:
            while True:
                await asyncio.sleep(1)

                cycle_elapsed = time.monotonic() - self._last_cycle_log
                if cycle_elapsed >= 30:
                    self._cycle_count += 1
                    self._last_cycle_log = time.monotonic()

                    cycle_msg = format_cycle_output(
                        cycle_count=self._cycle_count,
                        succeeded=self._records_succeeded,
                        failed=self._records_failed,
                        skipped=self._records_skipped,
                    )
                    logger.info(
                        cycle_msg,
                        extra={
                            "cycle": self._cycle_count,
                            "records_processed": self._records_processed,
                            "records_succeeded": self._records_succeeded,
                            "records_failed": self._records_failed,
                            "records_skipped": self._records_skipped,
                            "project_cache_size": self.project_cache.size(),
                            "cycle_interval_seconds": 30,
                        },
                    )

        except asyncio.CancelledError:
            logger.debug("Periodic cycle output task cancelled")
            raise

    async def _ensure_projects_exist(
        self,
        project_ids: List[str],
    ) -> None:
        """
        Pre-flight check: Ensure projects exist to prevent referential integrity issues
        when writing child entities. Failures are non-fatal.
        """
        if not project_ids or not self.enable_delta_writes:
            return

        unique_project_ids = list(set(project_ids))

        logger.debug(
            "Pre-flight: Checking project existence",
            extra={
                "project_count": len(unique_project_ids),
                "project_ids": unique_project_ids[:10],
            },
        )

        try:
            import polars as pl
            from deltalake import DeltaTable

            # Pre-flight check disabled in decoupled writer mode - project existence handled by downstream delta writer
            return

        except Exception as e:
            logger.error(
                "Pre-flight check failed",
                extra={
                    "error": str(e)[:200],
                    "project_count": len(project_ids),
                },
                exc_info=True,
            )

    def _create_download_tasks_from_media(
        self,
        media_rows: List[Dict[str, Any]],
    ) -> List[ClaimXDownloadTask]:
        download_tasks = []

        for media_row in media_rows:
            download_url = media_row.get("full_download_link")
            if not download_url:
                logger.debug(
                    "Skipping media row without download URL",
                    extra={
                        "media_id": media_row.get("media_id"),
                        "project_id": media_row.get("project_id"),
                    },
                )
                continue

            task = ClaimXDownloadTask(
                media_id=str(media_row.get("media_id", "")),
                project_id=str(media_row.get("project_id", "")),
                download_url=download_url,
                blob_path=self._generate_blob_path(media_row),
                file_type=media_row.get("file_type", ""),
                file_name=media_row.get("file_name", ""),
                source_event_id=media_row.get("event_id", ""),
                retry_count=0,
                expires_at=media_row.get("expires_at"),
                refresh_count=0,
            )
            download_tasks.append(task)

        logger.debug(
            "Created download tasks from media rows",
            extra={
                "media_rows": len(media_rows),
                "download_tasks": len(download_tasks),
            },
        )

        return download_tasks

    def _generate_blob_path(self, media_row: Dict[str, Any]) -> str:
        """
        Generate blob storage path for media file.
        Path is relative to OneLake domain base path (which includes 'claimx' prefix).
        """
        project_id = media_row.get("project_id", "unknown")
        media_id = media_row.get("media_id", "unknown")
        file_name = media_row.get("file_name", f"media_{media_id}")
        return f"{project_id}/media/{file_name}"

    async def _produce_entity_rows(
        self,
        entity_rows: EntityRowsMessage,
        tasks: List[ClaimXEnrichmentTask],
    ) -> None:
        """Write entity rows to Kafka. On failure, routes all tasks to retry/DLQ."""
        batch_id = uuid.uuid4().hex[:8]
        event_ids = [task.event_id for task in tasks[:5]]


        try:
            event_id = tasks[0].event_id if tasks else batch_id
            await self.producer.send(
                topic=self.entity_rows_topic,
                value=entity_rows,
                key=event_id,
            )

            logger.info(
                "Produced entity rows batch",
                extra={
                    "batch_id": batch_id,
                    "event_ids": event_ids,
                    "row_count": entity_rows.row_count(),
                },
            )

        except Exception as e:
            logger.error(
                "Error writing entities to Delta - routing batch to retry",
                extra={
                    "batch_id": batch_id,
                    "event_ids": event_ids,
                    "row_count": entity_rows.row_count(),
                    "task_count": len(tasks),
                    "error_category": ErrorCategory.TRANSIENT.value,
                    "error": str(e)[:200],
                },
                exc_info=True,
            )

            record_delta_write(
                table="claimx_entities_produce",
                event_count=entity_rows.row_count(),
                success=False,
            )

            error_category = ErrorCategory.TRANSIENT
            for task in tasks:
                await self._handle_enrichment_failure(task, e, error_category)

    async def _produce_download_tasks(
        self,
        download_tasks: List[ClaimXDownloadTask],
    ) -> None:
        logger.info(
            "Producing download tasks",
            extra={"task_count": len(download_tasks)},
        )

        for task in download_tasks:
            try:
                metadata = await self.producer.send(
                    topic=self.download_topic,
                    key=task.source_event_id,
                    value=task,
                    headers={"event_id": task.source_event_id},
                )

                logger.debug(
                    "Produced download task",
                    extra={
                        "media_id": task.media_id,
                        "project_id": task.project_id,
                        "partition": metadata.partition,
                        "offset": metadata.offset,
                    },
                )

            except Exception as e:
                logger.error(
                    "Failed to produce download task",
                    extra={
                        "media_id": task.media_id,
                        "project_id": task.project_id,
                        "error": str(e),
                    },
                    exc_info=True,
                )

    def _get_retry_topic(self, retry_level: int) -> str:
        if retry_level >= len(self._retry_delays):
            raise ValueError(
                f"Retry level {retry_level} exceeds max retries "
                f"({len(self._retry_delays)})"
            )

        delay_seconds = self._retry_delays[retry_level]
        delay_minutes = delay_seconds // 60
        return f"{self.enrichment_topic}.retry.{delay_minutes}m"

    async def _handle_enrichment_failure(
        self,
        task: ClaimXEnrichmentTask,
        error: Exception,
        error_category: "ErrorCategory",
    ) -> None:
        """
        Route failed task to retry topic or DLQ based on error category and retry count.
        TRANSIENT/AUTH/CIRCUIT_OPEN/UNKNOWN -> retry with backoff, PERMANENT -> DLQ immediately.
        """
        assert self.retry_handler is not None, "Retry handler not initialized"

        log_worker_error(
            logger,
            "Enrichment task failed",
            event_id=task.event_id,
            error_category=error_category.value,
            exc=error,
            event_type=task.event_type,
            project_id=task.project_id,
            retry_count=task.retry_count,
        )

        self._records_failed += 1

        await self.retry_handler.handle_failure(
            task=task,
            error=error,
            error_category=error_category,
        )


__all__ = ["ClaimXEnrichmentWorker"]
