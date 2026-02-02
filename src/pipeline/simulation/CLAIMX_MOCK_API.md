# ClaimX API Mock Client Documentation

## Overview

The `MockClaimXAPIClient` provides a drop-in replacement for `ClaimXApiClient` that returns realistic fixture data instead of making actual API calls. This enables local testing of the enrichment pipeline without access to production ClaimX API.

## Features

- **Fixture-based responses**: Loads realistic data from JSON files
- **Deterministic fake data**: Generates consistent data for unknown project IDs
- **Schema compatibility**: Returns data matching production API schemas
- **Localhost file server URLs**: All download URLs point to `localhost:8765`
- **No external dependencies**: Works in simulation mode without API credentials
- **iTel Cabinet support**: Includes task_id=32513 with form responses

## Architecture

```
pipeline/simulation/
├── claimx_api_mock.py          # Mock API client implementation
├── fixtures/                    # Fixture data directory
│   ├── claimx_projects.json    # 10 sample projects
│   ├── claimx_media.json       # Media files for projects
│   ├── claimx_tasks.json       # Tasks including iTel Cabinet (32513)
│   └── claimx_contacts.json    # Contact information
└── test_mock_simple.py         # Standalone test script
```

## Usage

### Basic Usage

```python
from pipeline.simulation import MockClaimXAPIClient

# Initialize with default fixtures directory
client = MockClaimXAPIClient()

# Or specify custom fixtures directory
from pathlib import Path
fixtures_dir = Path("/path/to/fixtures")
client = MockClaimXAPIClient(fixtures_dir=fixtures_dir)

# Use same interface as real ClaimXApiClient
async with client:
    project = await client.get_project(12345)
    media = await client.get_project_media(12345)
    task = await client.get_custom_task(67890)
    contacts = await client.get_project_contacts(12345)
```

### API Methods

The mock client implements the same interface as `ClaimXApiClient`:

#### `async def get_project(project_id: int) -> Dict[str, Any]`
Get full project details. Returns fixture data if available, otherwise generates deterministic fake data.

```python
project = await client.get_project(12345)
# Returns: { project_id, project_name, status, policyholder_name, ... }
```

#### `async def get_project_media(project_id: int, media_ids: Optional[List[int]] = None) -> List[Dict[str, Any]]`
Get media metadata for a project. Optionally filter by specific media IDs.

```python
media = await client.get_project_media(12345)
# Returns: [{ media_id, file_name, download_url, file_size, ... }, ...]
```

#### `async def get_custom_task(assignment_id: int) -> Dict[str, Any]`
Get custom task assignment details. Includes form responses for iTel Cabinet tasks.

```python
task = await client.get_custom_task(67890)
# Returns: { task_id, task_name, status, form_response: { responses, attachments }, ... }
```

#### `async def get_project_contacts(project_id: int) -> List[Dict[str, Any]]`
Get contacts for a project (policyholder, adjuster, contractors).

```python
contacts = await client.get_project_contacts(12345)
# Returns: [{ contact_id, name, email, phone, role }, ...]
```

#### `async def get_project_tasks(project_id: int) -> List[Dict[str, Any]]`
Get all tasks for a project.

```python
tasks = await client.get_project_tasks(12345)
# Returns: [{ task_assignment_id, task_id, task_name, status, ... }, ...]
```

## Fixture Data

### Projects (`claimx_projects.json`)

10 sample insurance claim projects with realistic data:

- **Project IDs**: `proj_1A2B3C4D5E6F`, `proj_9Z8Y7X6W5V4U`, etc.
- **Damage types**: Water, Fire, Wind, Hail, Lightning, Flood, etc.
- **Insurance companies**: State Farm, Progressive, GEICO, etc.
- **Adjusters**: Realistic names and contact info
- **Loss amounts**: $8,500 - $55,000 range

Example project:
```json
{
  "project_id": "proj_1A2B3C4D5E6F",
  "project_name": "Insurance Claim 2451",
  "status": "active",
  "policyholder_name": "James Wilson",
  "property_address": "1234 Oak St, Austin, TX 78701",
  "damage_type": "Water Damage",
  "estimated_loss": 15000.00
}
```

### Media (`claimx_media.json`)

25+ media files across projects:

- **File types**: JPG photos, PDF documents, MP4 videos
- **Realistic file names**: `exterior_damage_1.jpg`, `fire_report.pdf`, etc.
- **File sizes**: 189 KB - 15 MB range
- **Download URLs**: All point to `localhost:8765`

Example media:
```json
{
  "media_id": "media_100001",
  "project_id": "proj_1A2B3C4D5E6F",
  "file_name": "exterior_damage_1.jpg",
  "content_type": "image/jpeg",
  "file_size": 2048000,
  "download_url": "http://localhost:8765/files/proj_1A2B3C4D5E6F/media_100001/exterior_damage_1.jpg"
}
```

### Tasks (`claimx_tasks.json`)

8 sample tasks including **2 iTel Cabinet tasks** (task_id=32513):

#### iTel Cabinet Task Example
```json
{
  "task_assignment_id": "task_assign_10001",
  "task_id": 32513,
  "task_name": "iTel Cabinet Repair Form",
  "status": "completed",
  "form_response": {
    "responses": [
      {
        "question_key": "customer_first_name",
        "answer": "James"
      },
      {
        "question_key": "lower_cabinets_damaged",
        "answer": "Yes"
      },
      {
        "question_key": "lower_cabinets_lf",
        "answer": "12.5"
      }
    ],
    "attachments": [
      {
        "media_id": "media_cab_001",
        "question_key": "overview_photos",
        "file_name": "overview_1.jpg",
        "download_url": "http://localhost:8765/files/proj_1A2B3C4D5E6F/media_cab_001/overview_1.jpg"
      }
    ]
  }
}
```

**iTel Cabinet Tasks:**
1. `task_assign_10001` - Completed, water damage, 3 lower cabinet boxes damaged
2. `task_assign_20001` - Completed, fire damage, both upper and lower cabinets damaged
3. `task_assign_50001` - Assigned (not completed), no form response yet

**Other Tasks:**
- Review initial photos
- Schedule inspection
- Contact policyholder
- Approve repair scope
- Process payment

### Contacts (`claimx_contacts.json`)

26 contacts across all projects with three roles:

- **Policyholder**: Property owner filing claim
- **Adjuster**: Insurance company representative
- **Contractor**: Service provider (plumbers, roofers, electricians, etc.)

Example:
```json
{
  "contact_id": "contact_10001",
  "project_id": "proj_1A2B3C4D5E6F",
  "role": "policyholder",
  "name": "James Wilson",
  "email": "james.wilson@gmail.com",
  "phone": "(512) 555-1234"
}
```

## Deterministic Fake Data Generation

For project IDs not found in fixtures, the mock client generates deterministic fake data using the project ID as a seed:

```python
# Same project ID always returns same data
project1 = await client.get_project(999999)
project2 = await client.get_project(999999)
assert project1 == project2  # ✓ Deterministic!
```

This ensures:
- **Reproducible tests**: Same input always produces same output
- **Debugging**: Easy to reproduce specific scenarios
- **No external state**: All data derived from input parameters

## Download URL Format

All download URLs follow this format:
```
http://localhost:8765/files/{project_id}/{media_id}/{file_name}
```

This matches the dummy file server expected in Work Package 1 simulation infrastructure.

## Testing

### Run Simple Test

```bash
python3 pipeline/simulation/test_mock_simple.py
```

Expected output:
```
================================================================================
Simple Mock API Client Test
================================================================================

Fixtures directory: .../fixtures
Exists: True

Loaded:
  Projects: 10
  Media projects: 10
  Tasks: 8
  Contact projects: 10

...

All tests passed! ✓
```

### Test Coverage

The test verifies:
- ✓ Fixture data loads correctly
- ✓ Projects, media, tasks, contacts are accessible
- ✓ iTel Cabinet task (32513) exists with form responses
- ✓ Download URLs point to localhost:8765
- ✓ Deterministic fake data generation
- ✓ Schema compatibility with ClaimX API

## Integration with Enrichment Worker

To use the mock client in enrichment worker (Work Package 4):

```python
from pipeline.simulation import MockClaimXAPIClient

# In create_simulation_enrichment_worker factory:
mock_client = MockClaimXAPIClient()

worker = ClaimXEnrichmentWorker(
    config=config,
    # Pass mock client instead of real API client
    api_client=mock_client,
)
```

The mock client implements the same interface, so no code changes are needed in handlers.

## Event Type Support

The fixture data supports all ClaimX event types:

- ✓ `PROJECT_CREATED` - 10 projects in fixtures
- ✓ `PROJECT_FILE_ADDED` - 25+ media files
- ✓ `PROJECT_MFN_ADDED` - Supported via project lookup
- ✓ `CUSTOM_TASK_ASSIGNED` - 8 tasks including iTel Cabinet
- ✓ `CUSTOM_TASK_COMPLETED` - Multiple completed tasks
- ✓ `POLICYHOLDER_INVITED` - Supported via contacts
- ✓ `POLICYHOLDER_JOINED` - Supported via contacts
- ✓ `VIDEO_COLLABORATION_*` - Returns empty (not commonly used)

## Schema Compatibility

The mock responses match production ClaimX API schemas:

### Project Schema
- `project_id` (string)
- `project_name` (string)
- `status` (string: "active", "pending", "completed")
- `policyholder_name` (string)
- `policyholder_email` (string)
- `policyholder_phone` (string)
- `property_address` (string)
- `date_of_loss` (ISO date string)
- `created_at` (ISO datetime string)
- `updated_at` (ISO datetime string)

### Media Schema
- `media_id` (string)
- `project_id` (string)
- `file_name` (string)
- `content_type` (string)
- `file_size` (integer bytes)
- `file_type` (string extension)
- `download_url` (string URL)
- `uploaded_at` (ISO datetime string)

### Task Schema (iTel Cabinet)
- `task_assignment_id` (string)
- `task_id` (integer: 32513 for iTel Cabinet)
- `task_name` (string)
- `project_id` (string)
- `status` (string: "assigned" or "completed")
- `assigned_at` (ISO datetime string)
- `completed_at` (ISO datetime string or null)
- `form_id` (string)
- `form_response_id` (string or null)
- `form_response` (object with `responses` and `attachments` arrays)

### Contact Schema
- `contact_id` (string)
- `project_id` (string)
- `role` (string: "policyholder", "adjuster", "contractor")
- `name` (string)
- `email` (string)
- `phone` (string)

## Extending Fixture Data

To add more fixture data:

1. **Edit fixture files**: Add entries to JSON files in `fixtures/` directory
2. **Follow schema**: Ensure new data matches existing structure
3. **Use localhost URLs**: All download URLs must use `localhost:8765`
4. **Maintain relationships**: Link media/tasks/contacts to valid project IDs

Example - Adding a new project:
```json
{
  "project_id": "proj_NEW_PROJECT_ID",
  "project_name": "New Insurance Claim",
  "status": "active",
  "policyholder_name": "New Customer",
  "policyholder_email": "new.customer@example.com",
  "property_address": "123 Main St, City, ST 12345",
  "date_of_loss": "2026-01-20",
  "created_at": "2026-01-20T10:00:00Z",
  "updated_at": "2026-01-24T10:00:00Z"
}
```

## Best Practices

1. **Use fixtures for critical test cases**: Add specific scenarios to fixtures
2. **Let deterministic generation handle edge cases**: Unknown IDs auto-generate
3. **Verify download URLs**: Ensure all URLs point to localhost:8765
4. **Match production schemas**: Keep fixture data aligned with real API
5. **Test iTel Cabinet workflows**: Use task_id=32513 tasks for plugin testing

## Troubleshooting

### Fixtures not loading

Check that fixtures directory exists and contains JSON files:
```python
from pathlib import Path
fixtures_dir = Path(__file__).parent / "fixtures"
print(fixtures_dir.exists())
print(list(fixtures_dir.glob("*.json")))
```

### Invalid JSON in fixtures

Validate JSON files:
```bash
python3 -m json.tool fixtures/claimx_projects.json > /dev/null
```

### Wrong download URLs

Verify file_server_url is set correctly:
```python
client = MockClaimXAPIClient()
print(client.file_server_url)  # Should be: http://localhost:8765
```

## Future Enhancements

Potential improvements for future work packages:

- [ ] Dynamic fixture generation from templates
- [ ] Support for custom fixture directories via config
- [ ] Fixture data validation against Pydantic schemas
- [ ] Performance metrics (response times, cache hits)
- [ ] Circuit breaker simulation (artificial failures)
- [ ] Rate limiting simulation
- [ ] More iTel Cabinet task variations
- [ ] Additional damage type scenarios

## Related Documentation

- Work Package 1: Simulation Infrastructure
- Work Package 3: Dummy Data Generator (produces ClaimX events)
- Work Package 4: Simulation Enrichment Worker (uses this mock client)
- ClaimX API Documentation (production API schemas)
