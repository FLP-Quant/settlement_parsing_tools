# Examples

This directory contains example scripts demonstrating how to use the ISONE data processing utilities.

## Available Examples

### RTLOCSUM Data Processing (`rtlocsum_example.py`)

This example demonstrates how to:
1. Process RTLOCSUM data from ISONE MIS
2. Filter data for specific assets (Shepaug, Stevenson, Falls Village, Bulls Bridge)
3. Export filtered data to clipboard for Excel analysis
4. Generate summary statistics including:
   - Number of records
   - Date range
   - Assets included

To run the example:
```bash
python rtlocsum_example.py
```

Note: You'll need to update the file path in the script to point to your RTLOCSUM data file. 