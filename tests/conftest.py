"""
Pytest configuration and shared fixtures for settlement_parsing_tools tests.
"""
import os
import sys
import tempfile
from pathlib import Path

import pandas as pd
import pytest

# Ensure project root and src are on path
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
src_path = project_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))


@pytest.fixture
def tz_eastern():
    """Eastern timezone string used across ISONE logic."""
    return "America/New_York"


@pytest.fixture
def sample_unique_combos_ops():
    """Minimal unique_combos DataFrame for ops tables (name, ops_type, service)."""
    return pd.DataFrame([
        {"name": "ASSET_A", "ops_type": "Generation", "service": "energy"},
    ])


@pytest.fixture
def sample_unique_combos_offers():
    """Minimal unique_combos for offers (name, market_type, ops_type, service)."""
    return pd.DataFrame([
        {"name": "*", "market_type": "day_ahead", "ops_type": "generation", "service": "energy"},
    ])


@pytest.fixture
def isone_mapping_csv(tmp_path):
    """Create a minimal ISONE Location Mapping CSV for tests that need mapping_path."""
    path = tmp_path / "ISONE_Location_Mapping.csv"
    content = """ISO-NE Name,FLP Asset Name
ASSET_A,Asset A
UNIT_X,Unit X
"""
    path.write_text(content, encoding="ISO-8859-1")
    return str(path)


@pytest.fixture
def sample_schedule_offers_columns():
    """Column names expected by process_schedule_offers_historic (minimal set)."""
    return [
        "hour_ending", "market", "mw_0", "price_0", "mw_1", "price_1", "mw_2", "price_2",
        "mw_3", "price_3", "mw_4", "price_4", "mw_5", "price_5", "mw_6", "price_6",
        "mw_7", "price_7", "mw_8", "price_8", "mw_9", "price_9",
        "unit_id", "unit_name", "iso_id", "timestamp", "sched_type_id", "firm",
    ]
