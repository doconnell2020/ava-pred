# Ava Pred

An ETL pipeline for Canadian avalanche incident data and historical weather data, designed for ML prediction experiments.

> **Disclaimer**: This is an experimental project restructuring a masters thesis. It should not be used for any risk assessment related to avalanche safety.

## Project Structure

This project uses a [uv workspace](https://docs.astral.sh/uv/concepts/workspaces/) with three packages:

```
ava-pred/
├── packages/
│   ├── common/          # Shared config, models, and exceptions
│   ├── extract/         # Data extraction from APIs
│   └── transform/       # Data transformation and dataset creation
├── tests/               # Unit tests
├── pyproject.toml       # Root project configuration
└── plan.md              # Development plan
```

### Packages

| Package | Description |
|---------|-------------|
| `common` | Shared configuration (pydantic-settings), data models, and custom exceptions |
| `extract` | Fetches avalanche incidents from Canadian Avalanche API and weather data from Environment Canada |
| `transform` | Normalizes coordinates, aggregates weather data, and creates labeled ML datasets |

## Installation

Requires Python 3.13+ and [uv](https://docs.astral.sh/uv/).

```bash
# Clone and install
git clone <repo-url>
cd ava-pred
uv sync
```

## Usage

### CLI Commands

**Extract data:**
```bash
# Extract avalanche incidents
uv run ava-extract incidents

# Extract weather data (requires stations file)
uv run ava-extract weather --stations path/to/stations.csv

# Extract all data
uv run ava-extract all
```

**Transform data:**
```bash
# Normalize coordinate formats
uv run ava-transform coordinates

# Aggregate weather data
uv run ava-transform weather

# Create labeled ML dataset
uv run ava-transform dataset --balanced

# Run all transformations
uv run ava-transform all
```

### As a Library

```python
import asyncio
from extract import fetch_incidents
from transform.dataset import create_labeled_dataset

# Fetch incident data
df = asyncio.run(fetch_incidents())

# Create ML dataset
dataset = create_labeled_dataset(balanced=True)
```

## Configuration

Configuration is managed via environment variables or a `.env` file:

| Variable | Description | Default |
|----------|-------------|---------|
| `AVA_DATA_DIR` | Base data directory | `./data` |
| `AVA_RAW_DATA_PATH` | Raw data storage | `{data_dir}/raw` |
| `AVA_PROCESSED_DATA_PATH` | Processed data storage | `{data_dir}/processed` |
| `AVA_INCIDENTS_API_URL` | Avalanche API endpoint | Canadian Avalanche API |
| `AVA_MAX_CONCURRENT_REQUESTS` | API request concurrency | `10` |
| `AVA_API_REQUEST_DELAY` | Delay between requests (seconds) | `0.1` |

## Development

```bash
# Install dev dependencies
uv sync

# Run tests
uv run pytest

# Run linting
uv run ruff check packages/ tests/

# Run type checking
uv run mypy packages/

# Run pre-commit hooks
uv run pre-commit run --all-files
```

### Code Quality

- **Formatting**: [Ruff](https://docs.astral.sh/ruff/) (formatter + linter)
- **Type checking**: [mypy](https://mypy.readthedocs.io/) (strict mode)
- **Testing**: [pytest](https://docs.pytest.org/) with pytest-asyncio

## Data Pipeline

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Extract   │ ──> │  Transform  │ ──> │   Dataset   │
└─────────────┘     └─────────────┘     └─────────────┘
      │                   │                   │
      ▼                   ▼                   ▼
 incidents.parquet   normalized.csv    balanced_dataset.csv
 weather/*.csv       aggregated.csv    full_dataset.csv
```

1. **Extract**: Fetch incident data from Canadian Avalanche API and weather data from Environment Canada
2. **Transform**: Normalize coordinates (Lat/Lng, UTM), filter to Canada, aggregate weather by date
3. **Dataset**: Combine avalanche and non-avalanche weather data with labels for ML training

## License

MIT