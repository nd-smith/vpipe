"""
Tests for async OneLake client wrapper.

Tests the async interface to Azure Data Lake Storage Gen2 (OneLake)
for uploading attachments. Uses mocks since we don't want to require
real Azure credentials for unit tests.
"""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from kafka_pipeline.common.storage.onelake_client import OneLakeClient


@pytest.fixture
def mock_sync_client():
    """Mock the sync OneLakeClient."""
    client = MagicMock()
    client.__enter__ = Mock(return_value=client)
    client.__exit__ = Mock(return_value=False)
    client.upload_file = Mock(return_value="abfss://workspace@onelake/lakehouse/Files/test/file.pdf")
    client.upload_bytes = Mock(return_value="abfss://workspace@onelake/lakehouse/Files/test/data.bin")
    client.exists = Mock(return_value=True)
    client.delete = Mock(return_value=True)
    client.close = Mock()
    return client


@pytest.mark.asyncio
async def test_client_initialization():
    """Test OneLake client initializes with correct parameters."""
    client = OneLakeClient(
        base_path="abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Files",
        max_pool_size=50,
        connection_timeout=300,
    )

    assert client.base_path == "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Files"
    assert client._max_pool_size == 50
    assert client._connection_timeout == 300
    assert client._sync_client is None


@pytest.mark.asyncio
async def test_client_context_manager(mock_sync_client):
    """Test async context manager creates and closes client."""
    with patch(
        "kafka_pipeline.common.storage.onelake_client.SyncOneLakeClient",
        return_value=mock_sync_client,
    ):
        client = OneLakeClient("abfss://workspace@onelake/lakehouse/Files")

        async with client:
            # Client should be initialized
            assert client._sync_client is not None

        # Client should be closed
        mock_sync_client.close.assert_called_once()


@pytest.mark.asyncio
async def test_upload_file_success(mock_sync_client, tmp_path):
    """Test successful file upload to OneLake."""
    # Create temporary test file
    test_file = tmp_path / "test.pdf"
    test_file.write_bytes(b"PDF content")

    with patch(
        "kafka_pipeline.common.storage.onelake_client.SyncOneLakeClient",
        return_value=mock_sync_client,
    ):
        client = OneLakeClient("abfss://workspace@onelake/lakehouse/Files")

        async with client:
            result = await client.upload_file(
                relative_path="claims/C-123/test.pdf",
                local_path=test_file,
                overwrite=True,
            )

        # Verify upload was called with correct parameters
        mock_sync_client.upload_file.assert_called_once_with(
            "claims/C-123/test.pdf",
            str(test_file),
            True,
        )

        # Verify result
        assert result == "abfss://workspace@onelake/lakehouse/Files/test/file.pdf"


@pytest.mark.asyncio
async def test_upload_file_not_found(mock_sync_client, tmp_path):
    """Test upload fails if local file doesn't exist."""
    with patch(
        "kafka_pipeline.common.storage.onelake_client.SyncOneLakeClient",
        return_value=mock_sync_client,
    ):
        client = OneLakeClient("abfss://workspace@onelake/lakehouse/Files")

        async with client:
            with pytest.raises(FileNotFoundError):
                await client.upload_file(
                    relative_path="claims/C-123/missing.pdf",
                    local_path=tmp_path / "missing.pdf",
                    overwrite=True,
                )

        # Upload should not have been called
        mock_sync_client.upload_file.assert_not_called()


@pytest.mark.asyncio
async def test_upload_bytes_success(mock_sync_client):
    """Test successful bytes upload to OneLake."""
    test_data = b"Binary file content"

    with patch(
        "kafka_pipeline.common.storage.onelake_client.SyncOneLakeClient",
        return_value=mock_sync_client,
    ):
        client = OneLakeClient("abfss://workspace@onelake/lakehouse/Files")

        async with client:
            result = await client.upload_bytes(
                relative_path="data/test.bin",
                data=test_data,
                overwrite=True,
            )

        # Verify upload was called with correct parameters
        mock_sync_client.upload_bytes.assert_called_once_with(
            "data/test.bin",
            test_data,
            True,
        )

        # Verify result
        assert result == "abfss://workspace@onelake/lakehouse/Files/test/data.bin"


@pytest.mark.asyncio
async def test_exists(mock_sync_client):
    """Test file existence check."""
    with patch(
        "kafka_pipeline.common.storage.onelake_client.SyncOneLakeClient",
        return_value=mock_sync_client,
    ):
        client = OneLakeClient("abfss://workspace@onelake/lakehouse/Files")

        async with client:
            result = await client.exists("claims/C-123/file.pdf")

        # Verify exists was called
        mock_sync_client.exists.assert_called_once_with("claims/C-123/file.pdf")

        # Verify result
        assert result is True


@pytest.mark.asyncio
async def test_delete(mock_sync_client):
    """Test file deletion."""
    with patch(
        "kafka_pipeline.common.storage.onelake_client.SyncOneLakeClient",
        return_value=mock_sync_client,
    ):
        client = OneLakeClient("abfss://workspace@onelake/lakehouse/Files")

        async with client:
            result = await client.delete("claims/C-123/old_file.pdf")

        # Verify delete was called
        mock_sync_client.delete.assert_called_once_with("claims/C-123/old_file.pdf")

        # Verify result
        assert result is True


@pytest.mark.asyncio
async def test_close_without_context_manager(mock_sync_client):
    """Test explicit close without using context manager."""
    with patch(
        "kafka_pipeline.common.storage.onelake_client.SyncOneLakeClient",
        return_value=mock_sync_client,
    ):
        client = OneLakeClient("abfss://workspace@onelake/lakehouse/Files")

        # Manually initialize and close
        await client._ensure_client()
        assert client._sync_client is not None

        await client.close()
        assert client._sync_client is None
        mock_sync_client.close.assert_called_once()


@pytest.mark.asyncio
async def test_multiple_operations(mock_sync_client, tmp_path):
    """Test multiple operations using same client."""
    test_file = tmp_path / "test.pdf"
    test_file.write_bytes(b"PDF content")

    with patch(
        "kafka_pipeline.common.storage.onelake_client.SyncOneLakeClient",
        return_value=mock_sync_client,
    ):
        client = OneLakeClient("abfss://workspace@onelake/lakehouse/Files")

        async with client:
            # Upload file
            await client.upload_file("claims/test.pdf", test_file)

            # Upload bytes
            await client.upload_bytes("data/test.bin", b"data")

            # Check existence
            exists = await client.exists("claims/test.pdf")
            assert exists

            # Delete file
            deleted = await client.delete("claims/old.pdf")
            assert deleted

        # Verify all operations were called
        assert mock_sync_client.upload_file.call_count == 1
        assert mock_sync_client.upload_bytes.call_count == 1
        assert mock_sync_client.exists.call_count == 1
        assert mock_sync_client.delete.call_count == 1


@pytest.mark.asyncio
async def test_upload_propagates_exceptions(mock_sync_client, tmp_path):
    """Test that upload exceptions are propagated to caller."""
    test_file = tmp_path / "test.pdf"
    test_file.write_bytes(b"PDF content")

    # Configure mock to raise exception
    mock_sync_client.upload_file.side_effect = Exception("Upload failed")

    with patch(
        "kafka_pipeline.common.storage.onelake_client.SyncOneLakeClient",
        return_value=mock_sync_client,
    ):
        client = OneLakeClient("abfss://workspace@onelake/lakehouse/Files")

        async with client:
            with pytest.raises(Exception, match="Upload failed"):
                await client.upload_file("claims/test.pdf", test_file)
