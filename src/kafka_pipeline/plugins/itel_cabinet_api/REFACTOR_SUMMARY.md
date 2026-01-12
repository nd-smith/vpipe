# iTel Cabinet Plugin Refactor - Complete

## What Was Done

Completely refactored the iTel Cabinet plugin from an over-abstracted handler framework to explicit, traceable code.

## Before vs After

### Before (Handler Framework):
- ❌ **400+ line YAML config** defining handler pipelines
- ❌ **String-based handler instantiation** via `create_handler_from_config()`
- ❌ **Generic handlers** (TransformHandler, ValidationHandler, LookupHandler, etc.)
- ❌ **Wrapper handlers** (ConditionalCompletedHandler wrapping other handlers)
- ❌ **Opaque context passing** through `EnrichmentContext.data`
- ❌ **Stack traces** showed generic framework code, not business logic
- ❌ **Hard to debug** - had to trace through YAML → dynamic imports → handler base classes
- ❌ **Mixed logging** - some handlers used core logging, some didn't

### After (Explicit Code):
- ✅ **80 line YAML config** - just Kafka topics and table names
- ✅ **Explicit imports** - `from .pipeline import ItelCabinetPipeline`
- ✅ **Specific classes** - `ItelCabinetPipeline`, `TaskEvent`, `CabinetSubmission`
- ✅ **Simple conditionals** - `if event.task_status == 'COMPLETED'`
- ✅ **Clear data flow** - typed dataclasses with explicit field access
- ✅ **Stack traces** show actual method names: `_enrich_completed_task()`
- ✅ **Easy to debug** - set breakpoint in `pipeline.process()` and step through
- ✅ **Consistent logging** - all code uses core logging infrastructure

## New File Structure

```
kafka_pipeline/plugins/itel_cabinet_api/
├── models.py                           # NEW - Typed data structures
├── parsers.py                          # NEW - Form parsing functions
├── pipeline.py                         # NEW - Main processing logic
├── delta.py                            # NEW - Delta writer wrapper
├── itel_cabinet_tracking_worker.py     # REWRITTEN - Explicit worker
├── itel_cabinet_api_worker.py          # REWRITTEN - Simplified API worker
└── handlers/                           # DELETED - Old framework files
    ├── form_parser.py                  # ❌ Removed
    ├── form_transformer.py             # ❌ Removed
    ├── conditional_completed.py        # ❌ Removed
    ├── completed_publisher.py          # ❌ Removed
    ├── dual_table_writer.py            # ❌ Removed
    ├── itel_api_sender.py              # ❌ Removed
    └── media_downloader.py             # ❌ Removed
```

## New Architecture

### Tracking Worker Flow:
```python
# Read pipeline.py:process() to understand the ENTIRE flow
async def process(self, raw_message: dict) -> ProcessedTask:
    # 1. Parse and validate
    event = TaskEvent.from_kafka_message(raw_message)
    self._validate_event(event)

    # 2. Conditionally enrich (COMPLETED only)
    if event.task_status == 'COMPLETED':
        submission, attachments = await self._enrich_completed_task(event)
    else:
        submission, attachments = None, []

    # 3. Write to Delta (always)
    await self._write_to_delta(event, submission, attachments)

    # 4. Publish to API worker (COMPLETED only)
    if event.task_status == 'COMPLETED' and submission:
        await self._publish_for_api(event, submission, attachments)

    return ProcessedTask(event, submission, attachments)
```

### API Worker Flow:
```python
# Read itel_cabinet_api_worker.py:run() to understand the flow
async def run(self):
    async for message in self.consumer:
        # 1. Transform to iTel API format
        api_payload = self._transform_to_api_format(message.value)

        # 2. Send to API (or write to file in test mode)
        if self.api_config.get('test_mode'):
            await self._write_test_payload(api_payload)
        else:
            await self._send_to_api(api_payload)

        # 3. Commit
        await self.consumer.commit()
```

## Key Benefits

### 1. **Traceability**
- **Before**: Error in pipeline → stack trace shows `EnrichmentHandler.enrich()` → no idea which handler
- **After**: Error in pipeline → stack trace shows `ItelCabinetPipeline._enrich_completed_task()` → exact line

### 2. **Debuggability**
- **Before**: Set breakpoint → step through framework code → get lost in abstraction
- **After**: Set breakpoint in `pipeline.process()` → read code top-to-bottom → understand flow

### 3. **Maintainability**
- **Before**: Change flow → update YAML → hope dynamic imports work → debug at runtime
- **After**: Change flow → edit `pipeline.py` → IDE shows errors → fix before running

### 4. **Type Safety**
- **Before**: `context.data` dict → no autocomplete → runtime KeyError
- **After**: `TaskEvent` dataclass → IDE autocomplete → catch errors at write-time

### 5. **Onboarding**
- **Before**: New dev needs to understand: YAML syntax, handler framework, EnrichmentPipeline, context passing, dynamic imports
- **After**: New dev reads `pipeline.py` - it's just Python functions and classes

## Configuration Simplified

### Before (workers.yaml):
```yaml
enrichment_handlers:
  - type: transform
    config:
      mappings:
        event_id: event_id
        event_type: event_type
        # ... 30+ field mappings
  - type: validation
    config:
      required_fields: [...]
      field_rules: {...}
  - type: kafka_pipeline.plugins.itel_cabinet_api.handlers.conditional_completed:ConditionalCompletedHandler
    config:
      wrapped_handler:
        type: lookup
        config: {...}
  # ... 7 more handler configurations
```

### After (workers.yaml):
```yaml
itel_cabinet_tracking:
  kafka:
    input_topic: itel.cabinet.task.tracking
    consumer_group: itel_cabinet_tracking_group
  delta_tables:
    submissions: claimx_itel_forms
    attachments: claimx_itel_attachments
  pipeline:
    claimx_connection: claimx_api
    output_topic: itel.cabinet.completed
    download_media: false
```

**400+ lines → 80 lines**

## Running the Workers

### Tracking Worker:
```bash
python -m kafka_pipeline.plugins.itel_cabinet_api.itel_cabinet_tracking_worker
```

### API Worker (Production):
```bash
python -m kafka_pipeline.plugins.itel_cabinet_api.itel_cabinet_api_worker
```

### API Worker (Dev Mode - writes to files):
```bash
python -m kafka_pipeline.plugins.itel_cabinet_api.itel_cabinet_api_worker --dev
```

## What Happens When Things Break

### Before:
```
ERROR: EnrichmentHandler failed
Stack trace:
  enrichment.py:123 in execute
  base.py:45 in enrich

Where did it fail? What handler? What data?
Good luck finding out! 🤷
```

### After:
```
ERROR: Failed to enrich completed task
Stack trace:
  pipeline.py:145 in _enrich_completed_task
  parsers.py:67 in parse_cabinet_form

Assignment ID: 12345
Task ID: 32513
Missing field: customer_first_name

Jump to definition → see exact code → fix it ✅
```

## Lessons Learned

1. **Explicit > Generic**: `ItelCabinetPipeline` is better than `GenericEnrichmentPipeline`
2. **Functions > Frameworks**: `parse_cabinet_form()` is better than `TransformHandler(config)`
3. **Code > YAML**: Python business logic is better than YAML configuration
4. **Types > Dicts**: `TaskEvent` dataclass is better than `dict[str, Any]`
5. **Simple > Flexible**: Hardcoded flow is better than dynamic handler chains

## Future Plugins

When you need to add a new plugin (e.g., photo tasks, inspection reports):

### Don't:
- ❌ Try to reuse `EnrichmentPipeline` framework
- ❌ Create generic `TransformHandler` configurations
- ❌ Define behavior in YAML

### Do:
- ✅ Create `PhotoTaskPipeline` class with explicit `process()` method
- ✅ Write focused functions: `parse_photo_metadata()`, `validate_photo_task()`
- ✅ Define business logic in Python code
- ✅ Copy the iTel Cabinet plugin structure as a template

## Migration Notes

**The old enrichment handler framework still exists** in `kafka_pipeline/plugins/shared/enrichment.py`.

It's used by other parts of the system (ClaimX workers, Xact workers). We only removed it from the iTel Cabinet plugin.

If you want to refactor those too, follow the same pattern:
1. Create explicit pipeline classes
2. Move logic from handlers to focused functions
3. Simplify configuration
4. Remove handler dependencies

---

**Bottom line**: You can now understand, debug, and maintain the iTel Cabinet plugin without a PhD in abstraction theory. 🎉
