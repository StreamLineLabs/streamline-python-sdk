# Examples

## Prerequisites

- Python 3.9 through 3.14, with `streamline-sdk` installed
- A running Streamline server (default: `localhost:9092`)

## Running

Start Streamline:

```bash
# Via Docker
docker run -p 9092:9092 -p 9094:9094 \
  ghcr.io/streamlinelabs/streamline:0.2.0 --playground

# Or via Homebrew
streamline --playground
```

Run an example:

```bash
python examples/basic_usage.py
python examples/query_usage.py
python examples/schema_registry.py
```

## Configuration

Set `STREAMLINE_BOOTSTRAP_SERVERS` to connect to a non-local server:

```bash
export STREAMLINE_BOOTSTRAP_SERVERS=my-server:9092
python examples/basic_usage.py
```
