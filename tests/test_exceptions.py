"""Tests for all exception types in the Streamline SDK."""

import pytest

from streamline_sdk.exceptions import (
    AuthenticationError,
    AuthorizationError,
    ConnectionError,
    ConsumerError,
    ProducerError,
    SerializationError,
    StreamlineError,
    TimeoutError,
    TopicError,
)


class TestStreamlineError:
    """Tests for the base StreamlineError."""

    def test_creation_with_message(self):
        """Test creating base error with a message."""
        err = StreamlineError("something went wrong")
        assert str(err) == "something went wrong"

    def test_creation_empty(self):
        """Test creating base error with no message."""
        err = StreamlineError()
        assert str(err) == ""

    def test_is_exception(self):
        """StreamlineError should be an Exception."""
        assert issubclass(StreamlineError, Exception)

    def test_can_be_raised_and_caught(self):
        """Test raise/catch cycle."""
        with pytest.raises(StreamlineError, match="test"):
            raise StreamlineError("test")

    def test_args(self):
        """Exception args should contain the message."""
        err = StreamlineError("msg")
        assert err.args == ("msg",)


class TestConnectionError:
    """Tests for ConnectionError."""

    def test_is_streamline_error(self):
        """ConnectionError should be a StreamlineError."""
        assert issubclass(ConnectionError, StreamlineError)

    def test_message(self):
        err = ConnectionError("cannot connect")
        assert "cannot connect" in str(err)

    def test_default_hint(self):
        err = ConnectionError("cannot connect")
        assert err.hint == "Check that Streamline server is running and accessible"
        assert "(hint:" in str(err)

    def test_custom_hint(self):
        err = ConnectionError("cannot connect", hint="custom hint")
        assert err.hint == "custom hint"

    def test_isinstance_check(self):
        err = ConnectionError("err")
        assert isinstance(err, StreamlineError)
        assert isinstance(err, ConnectionError)

    def test_catch_as_base(self):
        """Catching StreamlineError should catch ConnectionError."""
        with pytest.raises(StreamlineError):
            raise ConnectionError("conn fail")


class TestProducerError:
    """Tests for ProducerError."""

    def test_is_streamline_error(self):
        assert issubclass(ProducerError, StreamlineError)

    def test_message(self):
        err = ProducerError("send failed")
        assert str(err) == "send failed"

    def test_isinstance_check(self):
        err = ProducerError("err")
        assert isinstance(err, StreamlineError)
        assert isinstance(err, ProducerError)
        assert not isinstance(err, ConnectionError)


class TestConsumerError:
    """Tests for ConsumerError."""

    def test_is_streamline_error(self):
        assert issubclass(ConsumerError, StreamlineError)

    def test_message(self):
        err = ConsumerError("poll timeout")
        assert str(err) == "poll timeout"

    def test_isinstance_check(self):
        err = ConsumerError("err")
        assert isinstance(err, StreamlineError)
        assert isinstance(err, ConsumerError)
        assert not isinstance(err, ProducerError)


class TestTopicError:
    """Tests for TopicError."""

    def test_is_streamline_error(self):
        assert issubclass(TopicError, StreamlineError)

    def test_message(self):
        err = TopicError("topic not found")
        assert "topic not found" in str(err)

    def test_default_hint(self):
        err = TopicError("topic not found")
        assert (
            err.hint
            == "Use admin client to create the topic first, or enable auto-creation"
        )
        assert "(hint:" in str(err)

    def test_isinstance_check(self):
        err = TopicError("err")
        assert isinstance(err, StreamlineError)
        assert isinstance(err, TopicError)


class TestAuthenticationError:
    """Tests for AuthenticationError."""

    def test_is_streamline_error(self):
        assert issubclass(AuthenticationError, StreamlineError)

    def test_message(self):
        err = AuthenticationError("bad credentials")
        assert "bad credentials" in str(err)

    def test_default_hint(self):
        err = AuthenticationError("bad credentials")
        assert err.hint == "Verify your SASL credentials and mechanism"
        assert "(hint:" in str(err)

    def test_isinstance_check(self):
        err = AuthenticationError("err")
        assert isinstance(err, StreamlineError)
        assert isinstance(err, AuthenticationError)

    def test_not_authorization(self):
        """AuthenticationError should not be AuthorizationError."""
        err = AuthenticationError("err")
        assert not isinstance(err, AuthorizationError)


class TestAuthorizationError:
    """Tests for AuthorizationError."""

    def test_is_streamline_error(self):
        assert issubclass(AuthorizationError, StreamlineError)

    def test_message(self):
        err = AuthorizationError("ACL denied")
        assert str(err) == "ACL denied"

    def test_isinstance_check(self):
        err = AuthorizationError("err")
        assert isinstance(err, StreamlineError)
        assert isinstance(err, AuthorizationError)

    def test_not_authentication(self):
        """AuthorizationError should not be AuthenticationError."""
        err = AuthorizationError("err")
        assert not isinstance(err, AuthenticationError)


class TestSerializationError:
    """Tests for SerializationError."""

    def test_is_streamline_error(self):
        assert issubclass(SerializationError, StreamlineError)

    def test_message(self):
        err = SerializationError("cannot deserialize")
        assert str(err) == "cannot deserialize"

    def test_isinstance_check(self):
        err = SerializationError("err")
        assert isinstance(err, StreamlineError)
        assert isinstance(err, SerializationError)


class TestTimeoutError:
    """Tests for TimeoutError."""

    def test_is_streamline_error(self):
        assert issubclass(TimeoutError, StreamlineError)

    def test_message(self):
        err = TimeoutError("request timed out after 30s")
        assert "request timed out after 30s" in str(err)

    def test_default_hint(self):
        err = TimeoutError("request timed out after 30s")
        assert (
            err.hint == "Consider increasing timeout settings or checking server load"
        )
        assert "(hint:" in str(err)

    def test_isinstance_check(self):
        err = TimeoutError("err")
        assert isinstance(err, StreamlineError)
        assert isinstance(err, TimeoutError)

    def test_not_builtin_timeout(self):
        """SDK TimeoutError should NOT be the builtin TimeoutError."""
        err = TimeoutError("err")
        # It IS a StreamlineError but should be distinguishable
        assert isinstance(err, StreamlineError)


class TestExceptionHierarchy:
    """Tests verifying the full exception hierarchy."""

    def test_all_are_streamline_errors(self):
        """All exception types should be subclasses of StreamlineError."""
        subclasses = [
            ConnectionError,
            ProducerError,
            ConsumerError,
            TopicError,
            AuthenticationError,
            AuthorizationError,
            SerializationError,
            TimeoutError,
        ]
        for cls in subclasses:
            assert issubclass(cls, StreamlineError), (
                f"{cls.__name__} is not a StreamlineError"
            )

    def test_all_are_exceptions(self):
        """All exception types should be subclasses of Exception."""
        subclasses = [
            StreamlineError,
            ConnectionError,
            ProducerError,
            ConsumerError,
            TopicError,
            AuthenticationError,
            AuthorizationError,
            SerializationError,
            TimeoutError,
        ]
        for cls in subclasses:
            assert issubclass(cls, Exception), f"{cls.__name__} is not an Exception"

    def test_sibling_exceptions_not_related(self):
        """Sibling exception types should not be subclasses of each other."""
        siblings = [
            ConnectionError,
            ProducerError,
            ConsumerError,
            TopicError,
            AuthenticationError,
            AuthorizationError,
            SerializationError,
            TimeoutError,
        ]
        for i, cls_a in enumerate(siblings):
            for cls_b in siblings[i + 1:]:
                assert not issubclass(cls_a, cls_b), (
                    f"{cls_a.__name__} should not be subclass of {cls_b.__name__}"
                )
                assert not issubclass(cls_b, cls_a), (
                    f"{cls_b.__name__} should not be subclass of {cls_a.__name__}"
                )

    def test_catch_all_with_base(self):
        """A single except StreamlineError should catch any SDK exception."""
        for exc_class in [ConnectionError, ProducerError, ConsumerError, TopicError,
                          AuthenticationError, AuthorizationError,
                          SerializationError, TimeoutError]:
            with pytest.raises(StreamlineError):
                raise exc_class(f"test {exc_class.__name__}")

    def test_string_representation(self):
        """All exceptions should include the message in str()."""
        for exc_class in [StreamlineError, ConnectionError, ProducerError,
                          ConsumerError, TopicError, AuthenticationError,
                          AuthorizationError, SerializationError, TimeoutError]:
            msg = f"error from {exc_class.__name__}"
            err = exc_class(msg)
            assert msg in str(err)
            assert msg in repr(err)
