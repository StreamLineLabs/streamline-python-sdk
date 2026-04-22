"""Tests for topic name validation."""

from unittest.mock import MagicMock

import pytest

from streamline_sdk.admin import Admin, TopicConfig
from streamline_sdk.client import ClientConfig, ConsumerConfig, ProducerConfig
from streamline_sdk.consumer import Consumer
from streamline_sdk.producer import Producer
from streamline_sdk.validation import ConfigurationError, validate_topic_name


class TestValidateTopicName:
    """Tests for the validate_topic_name function."""

    def test_valid_simple_name(self):
        """Simple alphanumeric names should pass."""
        validate_topic_name("my-topic")

    def test_valid_with_dots(self):
        """Names with dots should pass."""
        validate_topic_name("com.example.events")

    def test_valid_with_underscores(self):
        """Names with underscores should pass."""
        validate_topic_name("my_topic_v2")

    def test_valid_with_hyphens(self):
        """Names with hyphens should pass."""
        validate_topic_name("orders-v2-dlq")

    def test_valid_mixed(self):
        """Names with mixed valid characters should pass."""
        validate_topic_name("my.topic_name-v2")

    def test_valid_single_char(self):
        """Single character names should pass."""
        validate_topic_name("a")

    def test_valid_max_length(self):
        """Names at exactly 249 characters should pass."""
        validate_topic_name("a" * 249)

    def test_empty_name_raises(self):
        """Empty topic name should raise ConfigurationError."""
        with pytest.raises(ConfigurationError, match="cannot be empty"):
            validate_topic_name("")

    def test_too_long_raises(self):
        """Names exceeding 249 characters should raise."""
        with pytest.raises(ConfigurationError, match="exceeds maximum length"):
            validate_topic_name("a" * 250)

    def test_dot_raises(self):
        """'.' is reserved and should raise."""
        with pytest.raises(ConfigurationError, match="cannot be '\\.'"):
            validate_topic_name(".")

    def test_dotdot_raises(self):
        """'..' is reserved and should raise."""
        with pytest.raises(ConfigurationError, match="cannot be '\\.\\.'"):
            validate_topic_name("..")

    def test_space_raises(self):
        """Names with spaces should raise."""
        with pytest.raises(ConfigurationError, match="invalid characters"):
            validate_topic_name("my topic")

    def test_slash_raises(self):
        """Names with slashes should raise."""
        with pytest.raises(ConfigurationError, match="invalid characters"):
            validate_topic_name("my/topic")

    def test_colon_raises(self):
        """Names with colons should raise."""
        with pytest.raises(ConfigurationError, match="invalid characters"):
            validate_topic_name("topic:name")

    def test_special_chars_raise(self):
        """Names with special chars should raise."""
        with pytest.raises(ConfigurationError, match="invalid characters"):
            validate_topic_name("topic@name!")

    def test_error_has_hint(self):
        """ConfigurationError should include a helpful hint."""
        with pytest.raises(ConfigurationError) as exc_info:
            validate_topic_name("")
        assert exc_info.value.hint is not None
        assert "alphanumeric" in exc_info.value.hint

    def test_error_is_streamline_error(self):
        """ConfigurationError should be a subclass of StreamlineError."""
        from streamline_sdk.exceptions import StreamlineError
        with pytest.raises(StreamlineError):
            validate_topic_name("")


class TestProducerValidation:
    """Tests that Producer.send validates topic names."""

    @pytest.mark.asyncio
    async def test_send_empty_topic_raises(self):
        """Producer.send with empty topic should raise ConfigurationError."""
        producer = Producer(ClientConfig(), ProducerConfig())
        producer._producer = MagicMock()
        producer._started = True
        with pytest.raises(ConfigurationError, match="cannot be empty"):
            await producer.send("", value=b"msg")

    @pytest.mark.asyncio
    async def test_send_invalid_topic_raises(self):
        """Producer.send with invalid chars should raise ConfigurationError."""
        producer = Producer(ClientConfig(), ProducerConfig())
        producer._producer = MagicMock()
        producer._started = True
        with pytest.raises(ConfigurationError, match="invalid characters"):
            await producer.send("bad topic!", value=b"msg")

    @pytest.mark.asyncio
    async def test_send_dot_topic_raises(self):
        """Producer.send with '.' topic should raise ConfigurationError."""
        producer = Producer(ClientConfig(), ProducerConfig())
        producer._producer = MagicMock()
        producer._started = True
        with pytest.raises(ConfigurationError, match="cannot be '\\.'"):
            await producer.send(".", value=b"msg")


class TestConsumerValidation:
    """Tests that Consumer.subscribe validates topic names."""

    @pytest.mark.asyncio
    async def test_subscribe_empty_topic_raises(self):
        """Consumer.subscribe with empty topic should raise ConfigurationError."""
        consumer = Consumer(ClientConfig(), ConsumerConfig())
        consumer._consumer = MagicMock()
        consumer._started = True
        with pytest.raises(ConfigurationError, match="cannot be empty"):
            await consumer.subscribe([""])

    @pytest.mark.asyncio
    async def test_subscribe_invalid_topic_raises(self):
        """Consumer.subscribe with invalid chars should raise ConfigurationError."""
        consumer = Consumer(ClientConfig(), ConsumerConfig())
        consumer._consumer = MagicMock()
        consumer._started = True
        with pytest.raises(ConfigurationError, match="invalid characters"):
            await consumer.subscribe(["good-topic", "bad topic!"])

    @pytest.mark.asyncio
    async def test_subscribe_valid_topics_passes(self):
        """Consumer.subscribe with valid topics should not raise."""
        consumer = Consumer(ClientConfig(), ConsumerConfig())
        consumer._consumer = MagicMock()
        consumer._started = True
        await consumer.subscribe(["topic-a", "topic.b", "topic_c"])


class TestAdminValidation:
    """Tests that Admin create/delete validates topic names."""

    @pytest.mark.asyncio
    async def test_create_topic_empty_name_raises(self):
        """Admin.create_topic with empty name should raise ConfigurationError."""
        admin = Admin(ClientConfig())
        admin._admin = MagicMock()
        admin._started = True
        with pytest.raises(ConfigurationError, match="cannot be empty"):
            await admin.create_topic(TopicConfig(name=""))

    @pytest.mark.asyncio
    async def test_create_topic_invalid_name_raises(self):
        """Admin.create_topic with invalid chars should raise ConfigurationError."""
        admin = Admin(ClientConfig())
        admin._admin = MagicMock()
        admin._started = True
        with pytest.raises(ConfigurationError, match="invalid characters"):
            await admin.create_topic(TopicConfig(name="bad/topic"))

    @pytest.mark.asyncio
    async def test_create_topics_invalid_name_raises(self):
        """Admin.create_topics should validate all names."""
        admin = Admin(ClientConfig())
        admin._admin = MagicMock()
        admin._started = True
        with pytest.raises(ConfigurationError):
            await admin.create_topics([
                TopicConfig(name="good-topic"),
                TopicConfig(name=".."),
            ])

    @pytest.mark.asyncio
    async def test_delete_topic_empty_name_raises(self):
        """Admin.delete_topic with empty name should raise ConfigurationError."""
        admin = Admin(ClientConfig())
        admin._admin = MagicMock()
        admin._started = True
        with pytest.raises(ConfigurationError, match="cannot be empty"):
            await admin.delete_topic("")

    @pytest.mark.asyncio
    async def test_delete_topics_invalid_name_raises(self):
        """Admin.delete_topics should validate all names."""
        admin = Admin(ClientConfig())
        admin._admin = MagicMock()
        admin._started = True
        with pytest.raises(ConfigurationError):
            await admin.delete_topics(["valid-topic", "bad topic!"])
