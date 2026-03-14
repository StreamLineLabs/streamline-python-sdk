"""Circuit breaker example for Streamline Python SDK.

The circuit breaker prevents your application from repeatedly attempting
operations against a failing server. After consecutive failures it "opens"
and rejects requests immediately, giving the server time to recover.

Run with:
    python examples/circuit_breaker.py
"""

import asyncio
import os

from streamline_sdk import StreamlineClient
from streamline_sdk.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerOpen,
    CircuitState,
)


async def main():
    print("Circuit Breaker Example")
    print("=" * 40)

    # Configure the circuit breaker
    cb_config = CircuitBreakerConfig(
        failure_threshold=5,       # Open after 5 consecutive failures
        success_threshold=2,       # Close after 2 successes in half-open
        open_timeout_s=10.0,       # Wait 10s before probing
        half_open_max_requests=3,  # Allow 3 probe requests in half-open
        on_state_change=lambda old, new: print(
            f"  [Circuit Breaker] {old.value} → {new.value}"
        ),
    )
    breaker = CircuitBreaker(config=cb_config)

    async with StreamlineClient(
        bootstrap_servers=os.environ.get("STREAMLINE_BOOTSTRAP_SERVERS", "localhost:9092"),
    ) as client:
        print(f"Connected. Circuit state: {breaker.state.value}")

        # Create topic
        from streamline_sdk.admin import TopicConfig

        try:
            await client.admin.create_topic(TopicConfig(name="cb-example", num_partitions=1))
        except Exception:
            pass  # topic may already exist

        # Send messages through the circuit breaker
        for i in range(20):
            if not breaker.allow():
                print(f"  Message {i}: REJECTED (circuit open)")
                await asyncio.sleep(1)
                continue

            try:
                result = await client.producer.send(
                    "cb-example",
                    value=f"message-{i}".encode(),
                    key=f"key-{i}".encode(),
                )
                breaker.record_success()
                print(
                    f"  Message {i}: sent to partition={result.partition} "
                    f"offset={result.offset} (circuit: {breaker.state.value})"
                )
            except CircuitBreakerOpen:
                print(f"  Message {i}: circuit breaker is OPEN")
            except Exception as e:
                breaker.record_failure()
                print(f"  Message {i}: FAILED ({e}) (circuit: {breaker.state.value})")

        # Show final state
        successes, failures = breaker.counts
        print(f"\nFinal circuit state: {breaker.state.value}")
        print(f"Successes: {successes}, Failures: {failures}")

        # Manual reset
        if breaker.state == CircuitState.OPEN:
            breaker.reset()
            print(f"Circuit manually reset to: {breaker.state.value}")

    print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
