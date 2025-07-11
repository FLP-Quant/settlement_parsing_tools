# ISONE Data Processing Utilities

A collection of utilities for processing and analyzing ISONE (Independent System Operator New England) data, particularly focused on real-time operations data.

## Project Structure

```
.
├── src/                    # Source code
│   └── parsers.py         # Data parsing utilities
├── examples/              # Example scripts
│   └── rtlocsum_example.py # Example for RTLOCSUM data processing
├── data/                  # Supporting data
│   └── maps/             # Mapping files for location data
└── tests/                 # Test files
```

## Features

- `RealTimeOps` class for processing RTLOCSUM data from ISONE MIS
- Location mapping utilities for ISONE assets
- Data summarization and analysis capabilities

## Quick Start

1. Open Anaconda Prompt and navigate to the project directory:
```bash
cd path/to/shared_utilities
```

2. Run an example:
```bash
python examples/rtlocsum_example.py
```

## Usage Examples

The `examples/rtlocsum_example.py` demonstrates how to:
- Process RTLOCSUM data from ISONE MIS
- Filter data for specific assets
- Export filtered data to clipboard for Excel analysis
- Generate summary statistics 