# iTel Cabinet API Worker - Setup

## Environment Variables

### Required (OAuth2 / API)

| Variable | Description |
|----------|-------------|
| `ITEL_CABINET_API_BASE_URL` | iTel Cabinet API base URL (e.g. `https://api.qa.itelinc.net/cabinet-repair-orv`) |
| `ITEL_CABINET_API_TOKEN_URL` | OAuth2 token endpoint (e.g. `https://login.microsoftonline.com/<tenant-id>/oauth2/v2.0/token`) |
| `ITEL_CABINET_API_CLIENT_ID` | OAuth2 client ID |
| `ITEL_CABINET_API_CLIENT_CREDENTIAL` | OAuth2 client secret |
| `ITEL_CABINET_API_SCOPE` | OAuth2 scope |
| `ITEL_CABINET_API_CARRIER_ID` | Vendor-required carrier GUID (injected into API payload) |
| `ITEL_CABINET_API_SUBSCRIPTION_ID` | Vendor-required subscription ID (injected into API payload) |

### Optional

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9094` | Kafka broker addresses |
| `TEMP_DIR` | `/tmp` | Base directory for test output files |

## Dev Mode vs Production Mode

**Dev mode** (default): Writes JSON files to disk instead of calling the iTel API. Also fetches an OAuth2 token to verify credentials work end-to-end and saves the token metadata alongside the payload files.

```bash
python -m pipeline.plugins.itel_cabinet_api.itel_cabinet_api_worker
```

**Production mode**: Sends payloads to the live iTel API.

```bash
python -m pipeline.plugins.itel_cabinet_api.itel_cabinet_api_worker --prod
```

## Dev Mode Output

When processing a message in dev mode, the worker writes three files to `$TEMP_DIR/itel_cabinet_api/test/`:

| File | Contents |
|------|----------|
| `payload_{assignment_id}_{timestamp}.json` | Transformed vendor-schema API payload |
| `original_{assignment_id}_{timestamp}.json` | Original message payload (for debugging) |
| `token_{assignment_id}_{timestamp}.json` | OAuth2 token metadata (`expires_at`, `remaining_seconds`, `scope`, etc.) |

The token file confirms that the OAuth2 credentials are valid without submitting data to the API.
