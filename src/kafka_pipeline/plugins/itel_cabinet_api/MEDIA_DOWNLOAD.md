# iTel Cabinet Media Downloader

## Overview

The media downloader automatically downloads media files (images, PDFs) from ClaimX during enrichment and uploads them to OneLake storage. Media files are stored alongside Delta tables for complete data archival.

## Architecture

```
ClaimX Enrichment Flow:
1. Fetch task details from ClaimX API
2. Parse form data and attachments
3. FOR EACH attachment with media_id:
   a. Fetch media metadata from ClaimX (/export/project/{project_id}/media?mediaIds={id})
   b. Extract fullDownloadLink from metadata
   c. Download file content via HTTP
   d. Upload to OneLake: {base_path}/{project_id}/{assignment_id}/media/{media_id}.{ext}
   e. Update attachment.blob_path with OneLake path
4. Write attachments to Delta table with blob_path populated
```

## Configuration

### Enable Media Downloads

In `config/plugins/itel_cabinet_api/workers.yaml`:

```yaml
workers:
  itel_cabinet_tracking:
    pipeline:
      download_media: true  # Enable media downloads
```

### Environment Variables

```bash
# Required for media downloads
export ITEL_ATTACHMENTS_PATH="abfss://7729c159-0674-42ba-abf7-a6756723c62f@onelake.dfs.fabric.microsoft.com/584315ee-2f41-4943-949f-901d438bcd36/Files/veriskPipeline/itel_cabinet_form/attachments"

# ClaimX API access (already configured)
export CLAIMX_API_BASE_PATH="https://www.claimxperience.com/service/cxedirest"
export CLAIMX_API_TOKEN="your-basic-token"
```

## Storage Structure

Media files are organized in OneLake:

```
{ITEL_ATTACHMENTS_PATH}/
  {project_id}/
    {assignment_id}/
      media/
        {media_id_1}.jpg
        {media_id_2}.png
        {media_id_3}.pdf
```

Example:
```
abfss://.../attachments/
  5395115/
    5423878/
      media/
        12345.jpg    # Lower cabinet damage photo
        12346.jpg    # Upper cabinet damage photo
        12347.pdf    # Cabinet specification document
```

## File Naming

Files are named using ClaimX media_id with detected extension:
- **Format**: `{media_id}.{extension}`
- **Extension Detection**:
  1. From `mediaType` field in ClaimX response
  2. Fall back to parsing from `mediaName`
  3. Default to `.jpg` if unknown

## Error Handling

### Partial Failure Resilience

The downloader continues processing even if individual files fail:

```python
# Example: 10 attachments, 2 fail to download
- Attachment 1-8: ✓ Downloaded and blob_path populated
- Attachment 9:   ✗ Failed (logged as ERROR, blob_path = None)
- Attachment 10:  ✓ Downloaded and blob_path populated

Result: 8 attachments with blob_path, 2 without
        Pipeline continues and writes all attachments to Delta
```

### Error Logging

Failed downloads log detailed context:
```json
{
  "level": "ERROR",
  "msg": "Failed to download and upload media: HTTP 404 from ...",
  "media_id": 12345,
  "project_id": 5395115,
  "assignment_id": 5423878
}
```

## API Usage

### ClaimX Media Metadata Endpoint

**Endpoint**: `GET /export/project/{project_id}/media`

**Query Parameters**:
- `mediaIds`: Comma-separated list of media IDs (e.g., "12345")

**Response**:
```json
{
  "data": [
    {
      "mediaID": 12345,
      "mediaType": "jpg",
      "mediaName": "cabinet_damage.jpg",
      "fullDownloadLink": "https://storage.claimx.com/files/...",
      "expiresAt": "2024-12-31T23:59:59Z"
    }
  ]
}
```

### Individual Calls

Media files are downloaded individually (not batched):
- **Reason**: Simpler error handling and retry logic
- **Performance**: Concurrent downloads via asyncio.gather()
- **Resilience**: One failure doesn't block others

## Delta Schema Integration

### Attachments Table Updates

Successful downloads populate `blob_path` in `claimx_itel_attachments`:

```sql
-- Before download
media_id: 12345
blob_path: NULL

-- After successful download
media_id: 12345
blob_path: "abfss://.../attachments/5395115/5423878/media/12345.jpg"
```

### Schema Fields

Related fields in attachments table:
- `media_id`: ClaimX media ID (used for download)
- `blob_path`: Full OneLake path (populated after upload)
- `media_type`: MIME type (e.g., "image/jpeg", "application/pdf")
- `is_active`: Always true for new downloads

## Implementation Details

### MediaDownloader Class

**Location**: `kafka_pipeline/plugins/itel_cabinet_api/media_downloader.py`

**Key Methods**:
- `download_and_upload()`: Process all attachments concurrently
- `_download_and_upload_single()`: Handle individual media file
- `_fetch_media_metadata()`: Get download URL from ClaimX
- `_download_file()`: Download file content via HTTP
- `_detect_file_extension()`: Extract extension from metadata
- `_detect_mime_type()`: Determine MIME type for storage

**Dependencies**:
- `ConnectionManager`: For ClaimX API access
- `OneLakeClient`: For Azure Data Lake uploads
- `aiohttp`: For direct file downloads

### Pipeline Integration

**Location**: `kafka_pipeline/plugins/itel_cabinet_api/pipeline.py`

The downloader is integrated into the enrichment flow:

```python
async def _enrich_completed_task(self, event):
    # 1. Fetch task data
    task_data = await self._fetch_claimx_assignment(...)

    # 2. Parse form and attachments
    submission = parse_cabinet_form(...)
    attachments = parse_cabinet_attachments(...)

    # 3. Download media if enabled
    if self.download_media and attachments:
        attachments = await self._download_media_files(
            attachments, project_id, assignment_id
        )

    # 4. Write to Delta with blob_path populated
    await self._write_to_delta(...)
```

### Lazy Initialization

MediaDownloader is created on first use:
- Avoids unnecessary OneLake client creation if disabled
- Reuses same downloader instance for all tasks in worker lifecycle

## Testing

### Dev Mode Testing

Test media downloads without affecting production:

```bash
# 1. Set ITEL_ATTACHMENTS_PATH to test OneLake path
export ITEL_ATTACHMENTS_PATH="abfss://.../test/attachments"

# 2. Enable downloads in config
# workers.yaml: download_media: true

# 3. Run tracking worker
python -m kafka_pipeline.plugins.itel_cabinet_api.itel_cabinet_tracking_worker
```

### Verification

Check media uploads:
```python
# Query Delta table for attachments with blob_path
SELECT
    assignment_id,
    media_id,
    blob_path,
    media_type
FROM claimx_itel_attachments
WHERE blob_path IS NOT NULL
```

Check OneLake storage:
```bash
# List files in OneLake (via Azure CLI or portal)
az storage fs file list \
  --file-system {container} \
  --path "veriskPipeline/itel_cabinet_form/attachments/{project_id}/{assignment_id}/media"
```

## Performance Considerations

### Concurrent Downloads

Downloads are processed concurrently using asyncio:
- All attachments for a task download in parallel
- Limited by aiohttp connection pool (default: 20)
- Each download has 60s timeout

### Network Efficiency

- **Individual API calls**: One ClaimX API call per media_id
- **Direct downloads**: Files downloaded from ClaimX CDN (fullDownloadLink)
- **Streaming uploads**: OneLake uploads stream content (no temp files)

### Bandwidth Usage

Example task with 10 photos (2MB each):
- ClaimX metadata API: 10 calls (~10KB each = 100KB total)
- File downloads: 10 files × 2MB = 20MB
- OneLake uploads: 20MB
- **Total**: ~20MB network traffic per task

## Troubleshooting

### Media Not Downloading

1. **Check config**: Verify `download_media: true` in workers.yaml
2. **Check env var**: Verify `ITEL_ATTACHMENTS_PATH` is set
3. **Check logs**: Look for "Media download" INFO messages
4. **Check ClaimX API**: Verify media metadata endpoint is accessible

### Blob Path Not Populated

1. **Check errors**: Search logs for media_id with ERROR level
2. **Verify metadata**: Check ClaimX API returns fullDownloadLink
3. **Test OneLake**: Verify ITEL_ATTACHMENTS_PATH is writable
4. **Check auth**: Verify Azure credentials for OneLake access

### Common Errors

**"Media metadata not found"**:
- ClaimX API returned empty response
- media_id doesn't exist in project
- Check ClaimX API access token

**"No download URL in media metadata"**:
- fullDownloadLink missing from API response
- Media file may not be uploaded to ClaimX yet
- Check ClaimX media upload status

**"Failed to upload to OneLake"**:
- OneLake path incorrect or not accessible
- Azure credentials expired or invalid
- Network connectivity issues

## Future Enhancements

Potential improvements:
- Retry logic for failed downloads with exponential backoff
- Download size limits and validation
- Media file deduplication across assignments
- Thumbnail generation for images
- Batch downloading for multiple assignments
