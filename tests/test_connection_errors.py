"""Tests for connection error handling."""

import pytest
from unittest.mock import AsyncMock, patch

from streamline_sdk import StreamlineClient
from streamline_sdk.exceptions import ConnectionError
from streamline_sdk.producer import Producer
from streamline_sdk.admin import Admin


class TestConnectionErrors:
    """Tests for connection failure scenarios."""

    @pytest.mark.asyncio
    async def test_server_unreachable(self):
        """Test that ConnectionError is raised when server is unreachable."""
        with patch.object(Producer, "start", new_callable=AsyncMock) as mock_start:
            mock_start.side_effect = OSError("Connection refused")

            client = StreamlineClient(bootstrap_servers="localhost:9092")
            with pytest.raises(ConnectionError, match="Failed to start client"):
                await client.start()

    @pytest.mark.asyncio
    async def test_timeout_error(self):
        """Test that ConnectionError is raised on connection timeout."""
        with patch.object(Producer, "start", new_callable=AsyncMock) as mock_start:
            mock_start.side_effect = TimeoutError("Connection timed out")

            client = StreamlineClient(bootstrap_servers="localhost:9092")
            with pytest.raises(ConnectionError, match="Failed to start client"):
                await client.start()

    @pytest.mark.asyncio
    async def test_dns_resolution_failure(self):
        """Test that ConnectionError is raised when DNS resolution fails."""
        with patch.object(Producer, "start", new_callable=AsyncMock) as mock_start:
            mock_start.side_effect = OSError("Name or service not known")

            client = StreamlineClient(bootstrap_servers="unknownhost:9092")
            with pytest.raises(ConnectionError, match="Failed to start client"):
                await client.start()

    @pytest.mark.asyncio
    async def test_client_closed_after_failed_connection(self):
        """Test that client is properly cleaned up after connection failure."""
        with patch.object(Producer, "start", new_callable=AsyncMock) as mock_start:
            mock_start.side_effect = OSError("Connection refused")

            client = StreamlineClient(bootstrap_servers="localhost:9092")
            with pytest.raises(ConnectionError):
                await client.start()

            assert client.is_connected is False

    @pytest.mark.asyncio
    async def test_reconnection_after_failure(self):
        """Test that client can attempt reconnection after failure."""
        with patch.object(Producer, "start", new_callable=AsyncMock) as mock_start:
            mock_start.side_effect = OSError("Connection refused")

            client = StreamlineClient(bootstrap_servers="localhost:9092")
            with pytest.raises(ConnectionError):
                await client.start()

        # Now try again with successful connection
        with patch.object(Producer, "start", new_callable=AsyncMock):
            with patch.object(Admin, "start", new_callable=AsyncMock):
                client2 = StreamlineClient(bootstrap_servers="localhost:9092")
                await client2.start()
                assert client2.is_connected is True
                await client2.close()

    @pytest.mark.asyncio
    async def test_context_manager_handles_connection_error(self):
        """Test that async context manager handles connection errors gracefully."""
        with patch.object(Producer, "start", new_callable=AsyncMock) as mock_start:
            mock_start.side_effect = OSError("Connection refused")

            with pytest.raises(ConnectionError):
                async with StreamlineClient(bootstrap_servers="localhost:9092"):
                    pass