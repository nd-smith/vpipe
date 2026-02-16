"""FastAPI app for EventHub utility dashboard."""

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

try:
    from core.security.ssl_dev_bypass import apply_ssl_dev_bypass

    apply_ssl_dev_bypass()
except ImportError:
    pass

import asyncio
import logging
import traceback
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Form, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from pipeline.tools.eventhub_ui.config import (
    EventHubInfo,
    extract_fqdn,
    get_blob_connection_string,
    get_checkpoint_container_name,
    get_namespace_connection_string,
    get_source_connection_string,
    get_ssl_kwargs,
    list_eventhubs,
)

logger = logging.getLogger(__name__)

app = FastAPI(title="EventHub Utility", docs_url=None, redoc_url=None)

templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


def _conn_str_for_hub(hub: EventHubInfo) -> str:
    """Get the right connection string based on whether the hub is source or internal."""
    if hub.is_source:
        return get_source_connection_string()
    return get_namespace_connection_string()


def _find_hub(eventhub_name: str) -> EventHubInfo | None:
    for hub in list_eventhubs():
        if hub.eventhub_name == eventhub_name:
            return hub
    return None


# -------------------------------------------------------------------------
# Routes
# -------------------------------------------------------------------------


@app.get("/", response_class=HTMLResponse)
async def overview(request: Request):
    """List all configured EventHubs with basic info."""
    hubs = list_eventhubs()

    # Group by domain
    domains = {}
    for hub in hubs:
        domains.setdefault(hub.domain, []).append(hub)

    return templates.TemplateResponse("overview.html", {
        "request": request,
        "domains": domains,
        "hub_count": len(hubs),
    })


@app.get("/partitions/{eventhub_name}", response_class=HTMLResponse)
async def partitions(request: Request, eventhub_name: str):
    """Show partition details for an EventHub."""
    from pipeline.tools.eventhub_ui.partitions import (
        get_eventhub_properties,
        get_partition_properties,
    )

    hub = _find_hub(eventhub_name)
    if not hub:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "message": f"EventHub '{eventhub_name}' not found in config",
        }, status_code=404)

    try:
        conn_str = _conn_str_for_hub(hub)
        ssl_kwargs = get_ssl_kwargs()
        eh_props = await get_eventhub_properties(conn_str, eventhub_name, ssl_kwargs)
        partition_list = await get_partition_properties(conn_str, eventhub_name, ssl_kwargs)
    except Exception as e:
        logger.exception(f"Failed to fetch partitions for {eventhub_name}")
        return templates.TemplateResponse("error.html", {
            "request": request,
            "message": f"Error connecting to '{eventhub_name}': {e}",
        }, status_code=500)

    return templates.TemplateResponse("partitions.html", {
        "request": request,
        "hub": hub,
        "eh_props": eh_props,
        "partitions": partition_list,
    })


async def _fetch_all_lag() -> tuple[list[dict], list[str]]:
    """Fetch lag for every consumer group. Returns (results, errors)."""
    from pipeline.tools.eventhub_ui.lag import calculate_lag

    config_errors = []
    try:
        internal_conn = get_namespace_connection_string()
        internal_fqdn = extract_fqdn(internal_conn)
    except Exception as e:
        internal_conn = None
        internal_fqdn = None
        config_errors.append(str(e))

    try:
        source_conn = get_source_connection_string()
        source_fqdn = extract_fqdn(source_conn)
    except Exception as e:
        source_conn = None
        source_fqdn = None
        config_errors.append(str(e))

    try:
        blob_conn = get_blob_connection_string()
    except Exception as e:
        blob_conn = None
        config_errors.append(str(e))

    if config_errors:
        return [], config_errors

    hubs = list_eventhubs()
    ssl_kwargs = get_ssl_kwargs()
    container_name = get_checkpoint_container_name()

    tasks = []
    for hub in hubs:
        if not hub.consumer_groups:
            continue

        conn_str = source_conn if hub.is_source else internal_conn
        fqdn = source_fqdn if hub.is_source else internal_fqdn

        for worker_name, cg_name in hub.consumer_groups.items():
            tasks.append((hub, worker_name, cg_name, calculate_lag(
                conn_str=conn_str,
                eventhub_name=hub.eventhub_name,
                consumer_group=cg_name,
                fqdn=fqdn,
                blob_conn_str=blob_conn,
                container_name=container_name,
                ssl_kwargs=ssl_kwargs,
            )))

    gathered = await asyncio.gather(
        *(coro for _, _, _, coro in tasks),
        return_exceptions=True,
    )

    results = []
    errors = []
    for (hub, worker_name, cg_name, _), outcome in zip(tasks, gathered):
        if isinstance(outcome, Exception):
            errors.append(f"{hub.eventhub_name}/{cg_name}: {outcome}")
        else:
            results.append({
                "hub": hub,
                "worker_name": worker_name,
                "lag": outcome,
            })

    return results, errors


@app.get("/lag", response_class=HTMLResponse)
async def lag_overview(request: Request):
    """Show lag for all consumer groups across all EventHubs."""
    results, errors = await _fetch_all_lag()

    return templates.TemplateResponse("lag_overview.html", {
        "request": request,
        "results": results,
        "errors": errors,
    })


@app.get("/lag/data", response_class=HTMLResponse)
async def lag_overview_data(request: Request):
    """HTMX partial: return just the lag table for polling updates."""
    from pipeline.tools.eventhub_ui.lag_history import get_trends, record_snapshot

    results, errors = await _fetch_all_lag()

    # Record snapshot to CSV for trend analysis
    if results:
        try:
            await asyncio.to_thread(record_snapshot, results)
        except Exception:
            logger.exception("Failed to record lag snapshot to CSV")

    # Load trend data for sparklines
    try:
        trends = await asyncio.to_thread(get_trends)
    except Exception:
        logger.exception("Failed to load lag trends")
        trends = {}

    now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")

    return templates.TemplateResponse("lag_overview_partial.html", {
        "request": request,
        "results": results,
        "errors": errors,
        "updated_at": now,
        "trends": trends,
    })


@app.get("/lag/history.csv")
async def lag_history_csv():
    """Download the lag history CSV."""
    from fastapi.responses import PlainTextResponse

    from pipeline.tools.eventhub_ui.lag_history import read_csv

    content = await asyncio.to_thread(read_csv)
    return PlainTextResponse(
        content,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=consumer_lag_history.csv"},
    )


@app.get("/lag/{eventhub_name}/{consumer_group}", response_class=HTMLResponse)
async def lag(request: Request, eventhub_name: str, consumer_group: str):
    """Show consumer lag for a specific consumer group."""
    from pipeline.tools.eventhub_ui.lag import calculate_lag

    hub = _find_hub(eventhub_name)
    if not hub:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "message": f"EventHub '{eventhub_name}' not found in config",
        }, status_code=404)

    try:
        conn_str = _conn_str_for_hub(hub)
        fqdn = extract_fqdn(conn_str)
        lag_result = await calculate_lag(
            conn_str=conn_str,
            eventhub_name=eventhub_name,
            consumer_group=consumer_group,
            fqdn=fqdn,
            blob_conn_str=get_blob_connection_string(),
            container_name=get_checkpoint_container_name(),
            ssl_kwargs=get_ssl_kwargs(),
        )
    except Exception as e:
        logger.exception(f"Failed to calculate lag for {eventhub_name}/{consumer_group}")
        return templates.TemplateResponse("error.html", {
            "request": request,
            "message": f"Error calculating lag: {e}",
        }, status_code=500)

    return templates.TemplateResponse("lag.html", {
        "request": request,
        "hub": hub,
        "consumer_group": consumer_group,
        "lag": lag_result,
    })


@app.get("/sampler/{eventhub_name}/{partition_id}", response_class=HTMLResponse)
async def sampler(
    request: Request,
    eventhub_name: str,
    partition_id: str,
    count: int = Query(default=5, ge=1, le=50),
):
    """Sample recent messages from a partition."""
    from pipeline.tools.eventhub_ui.sampler import sample_messages

    hub = _find_hub(eventhub_name)
    if not hub:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "message": f"EventHub '{eventhub_name}' not found in config",
        }, status_code=404)

    try:
        conn_str = _conn_str_for_hub(hub)
        messages = await sample_messages(
            conn_str=conn_str,
            eventhub_name=eventhub_name,
            partition_id=partition_id,
            count=count,
            ssl_kwargs=get_ssl_kwargs(),
        )
    except Exception as e:
        logger.exception(f"Failed to sample from {eventhub_name}/{partition_id}")
        return templates.TemplateResponse("error.html", {
            "request": request,
            "message": f"Error sampling messages: {e}",
        }, status_code=500)

    return templates.TemplateResponse("sampler.html", {
        "request": request,
        "hub": hub,
        "partition_id": partition_id,
        "messages": messages,
        "count": count,
    })


@app.get("/checkpoints/{eventhub_name}/{consumer_group}", response_class=HTMLResponse)
async def checkpoints_view(request: Request, eventhub_name: str, consumer_group: str):
    """View checkpoints for a consumer group."""
    from pipeline.tools.eventhub_ui.checkpoints import list_checkpoints

    hub = _find_hub(eventhub_name)
    if not hub:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "message": f"EventHub '{eventhub_name}' not found in config",
        }, status_code=404)

    try:
        conn_str = _conn_str_for_hub(hub)
        fqdn = extract_fqdn(conn_str)
        entries = list_checkpoints(
            blob_conn_str=get_blob_connection_string(),
            container_name=get_checkpoint_container_name(),
            fqdn=fqdn,
            eventhub_name=eventhub_name,
            consumer_group=consumer_group,
        )
    except Exception as e:
        logger.exception(f"Failed to list checkpoints for {eventhub_name}/{consumer_group}")
        return templates.TemplateResponse("error.html", {
            "request": request,
            "message": f"Error listing checkpoints: {e}",
        }, status_code=500)

    return templates.TemplateResponse("checkpoints.html", {
        "request": request,
        "hub": hub,
        "consumer_group": consumer_group,
        "entries": entries,
    })


@app.post("/checkpoints/{eventhub_name}/{consumer_group}/advance", response_class=HTMLResponse)
async def checkpoints_advance(request: Request, eventhub_name: str, consumer_group: str):
    """Advance all checkpoints to latest."""
    from pipeline.tools.eventhub_ui.checkpoints import advance_checkpoints_to_latest

    hub = _find_hub(eventhub_name)
    if not hub:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "message": f"EventHub '{eventhub_name}' not found in config",
        }, status_code=404)

    try:
        conn_str = _conn_str_for_hub(hub)
        fqdn = extract_fqdn(conn_str)
        changes = advance_checkpoints_to_latest(
            conn_str=conn_str,
            eventhub_name=eventhub_name,
            consumer_group=consumer_group,
            fqdn=fqdn,
            blob_conn_str=get_blob_connection_string(),
            container_name=get_checkpoint_container_name(),
            ssl_kwargs=get_ssl_kwargs(),
        )
    except Exception as e:
        logger.exception(f"Failed to advance checkpoints for {eventhub_name}/{consumer_group}")
        return templates.TemplateResponse("error.html", {
            "request": request,
            "message": f"Error advancing checkpoints: {e}",
        }, status_code=500)

    return templates.TemplateResponse("checkpoint_result.html", {
        "request": request,
        "hub": hub,
        "consumer_group": consumer_group,
        "action": "advanced to latest",
        "changes": changes,
    })


@app.post("/checkpoints/{eventhub_name}/{consumer_group}/reset-to-time", response_class=HTMLResponse)
async def checkpoints_reset_to_time(
    request: Request,
    eventhub_name: str,
    consumer_group: str,
    target_datetime: str = Form(...),
):
    """Reset checkpoints to a specific UTC datetime."""
    from pipeline.tools.eventhub_ui.checkpoints import reset_checkpoints_to_time

    hub = _find_hub(eventhub_name)
    if not hub:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "message": f"EventHub '{eventhub_name}' not found in config",
        }, status_code=404)

    try:
        target_time = datetime.fromisoformat(target_datetime).replace(tzinfo=timezone.utc)
    except ValueError:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "message": f"Invalid datetime format: {target_datetime}",
        }, status_code=400)

    try:
        conn_str = _conn_str_for_hub(hub)
        fqdn = extract_fqdn(conn_str)
        changes = await reset_checkpoints_to_time(
            conn_str=conn_str,
            eventhub_name=eventhub_name,
            consumer_group=consumer_group,
            fqdn=fqdn,
            blob_conn_str=get_blob_connection_string(),
            container_name=get_checkpoint_container_name(),
            target_time=target_time,
            ssl_kwargs=get_ssl_kwargs(),
        )
    except Exception as e:
        logger.exception(f"Failed to reset checkpoints for {eventhub_name}/{consumer_group}")
        return templates.TemplateResponse("error.html", {
            "request": request,
            "message": f"Error resetting checkpoints: {e}",
        }, status_code=500)

    return templates.TemplateResponse("checkpoint_result.html", {
        "request": request,
        "hub": hub,
        "consumer_group": consumer_group,
        "action": f"reset to {target_time.isoformat()}",
        "changes": changes,
    })


# -------------------------------------------------------------------------
# Log Viewer Routes
# -------------------------------------------------------------------------


@app.get("/logs", response_class=HTMLResponse)
async def logs_browse(request: Request):
    """Browse log domains and dates."""
    from pipeline.tools.eventhub_ui.logs import list_log_domains

    try:
        domains = await asyncio.to_thread(list_log_domains)
    except Exception as e:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "message": f"Error connecting to OneLake: {e}",
        }, status_code=500)

    return templates.TemplateResponse("logs_browse.html", {
        "request": request,
        "domains": domains,
    })


@app.get("/logs/view", response_class=HTMLResponse)
async def logs_view(
    request: Request,
    path: str = Query(...),
    level: str = Query(default=""),
    search: str = Query(default=""),
    trace_id: str = Query(default=""),
    tail: int = Query(default=500, ge=1, le=5000),
):
    """View and filter a log file."""
    from pipeline.tools.eventhub_ui.logs import read_log_file

    try:
        entries = await asyncio.to_thread(
            read_log_file,
            path=path,
            level=level or None,
            search=search or None,
            trace_id=trace_id or None,
            tail=tail,
        )
    except Exception as e:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "message": f"Error reading log file: {e}",
        }, status_code=500)

    filename = path.split("/")[-1]

    return templates.TemplateResponse("logs_view.html", {
        "request": request,
        "path": path,
        "filename": filename,
        "entries": entries,
        "level": level,
        "search": search,
        "trace_id": trace_id,
        "tail": tail,
    })


@app.get("/logs/{domain}", response_class=HTMLResponse)
async def logs_dates(request: Request, domain: str):
    """List available dates for a domain."""
    from pipeline.tools.eventhub_ui.logs import list_log_dates

    try:
        dates = await asyncio.to_thread(list_log_dates, domain)
    except Exception as e:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "message": f"Error listing dates for '{domain}': {e}",
        }, status_code=500)

    return templates.TemplateResponse("logs_dates.html", {
        "request": request,
        "domain": domain,
        "dates": dates,
    })


@app.get("/logs/{domain}/{date}", response_class=HTMLResponse)
async def logs_files(request: Request, domain: str, date: str):
    """List log files for a domain/date."""
    from pipeline.tools.eventhub_ui.logs import list_log_files

    try:
        files = await asyncio.to_thread(list_log_files, domain, date)
    except Exception as e:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "message": f"Error listing files for '{domain}/{date}': {e}",
        }, status_code=500)

    return templates.TemplateResponse("logs_files.html", {
        "request": request,
        "domain": domain,
        "date": date,
        "files": files,
    })
