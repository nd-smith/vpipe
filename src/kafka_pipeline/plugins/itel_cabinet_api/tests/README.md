# iTel Cabinet API Worker - Test Output Directory

This directory contains API payload files generated when the worker runs in dev mode.

## Usage

Start the API worker in dev mode:
```bash
python -m kafka_pipeline.plugins.itel_cabinet_api.itel_cabinet_api_worker --dev
```

## Output Files

When running in dev mode, the worker will write API payloads to this directory instead of sending them to the iTel Cabinet API.

File naming pattern: `payload_{assignment_id}_{timestamp}.json`

Example:
```
tests/
├── payload_5423723_20260111_153045.json
├── payload_5423724_20260111_153046.json
└── payload_5423725_20260111_153047.json
```

## Verification

Inspect the generated payloads:
```bash
# View latest payload
ls -t tests/*.json | head -1 | xargs cat | jq

# Count payloads
ls tests/*.json | wc -l

# View specific payload
cat tests/payload_5423723_20260111_153045.json | jq
```

## Cleanup

Remove all test payloads:
```bash
rm tests/*.json
```
