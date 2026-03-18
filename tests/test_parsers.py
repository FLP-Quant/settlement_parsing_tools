"""
Unit tests for src.parsers (retrieve_isone_location_map, filter_duplicate_rows).
"""
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from src.parsers import filter_duplicate_rows, retrieve_isone_location_map


class TestRetrieveIsoneLocationMap:
    """Tests for retrieve_isone_location_map."""

    def test_file_not_found_raises(self):
        with pytest.raises(FileNotFoundError, match="Mapping file not found"):
            retrieve_isone_location_map("/nonexistent/path/mapping.csv")

    def test_reads_valid_csv(self, tmp_path):
        path = tmp_path / "mapping.csv"
        path.write_text(
            "ISO-NE Name,FLP Asset Name\nASSET_A,Asset A\nUNIT_X,Unit X\n",
            encoding="ISO-8859-1",
        )
        result = retrieve_isone_location_map(str(path))
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "ISO-NE Name" in result.columns
        assert "FLP Asset Name" in result.columns
        assert list(result["ISO-NE Name"]) == ["ASSET_A", "UNIT_X"]


class TestFilterDuplicateRows:
    """Tests for filter_duplicate_rows."""

    def test_no_duplicates_returns_unchanged(self):
        df = pd.DataFrame({
            "datetime_he": [pd.Timestamp("2025-01-15 12:00:00")] * 2,
            "asset": ["A", "B"],
            "name": ["A", "B"],
            "ops_type": ["Generation", "Generation"],
            "service": ["energy", "energy"],
            "da_volume": [1, 2],
            "rt_volume": [0, 0],
        })
        result = filter_duplicate_rows(df)
        assert len(result) == 2
        assert result.equals(df)

    def test_duplicate_rows_kept_first(self):
        df = pd.DataFrame({
            "datetime_he": [pd.Timestamp("2025-01-15 12:00:00"), pd.Timestamp("2025-01-15 12:00:00")],
            "asset": ["A", "A"],
            "name": ["A", "A"],
            "ops_type": ["Generation", "Generation"],
            "service": ["energy", "energy"],
            "da_volume": [1, 99],
            "rt_volume": [0, 0],
        })
        result = filter_duplicate_rows(df)
        assert len(result) == 1
        assert result["da_volume"].iloc[0] == 1

    def test_subset_columns_used(self):
        """Duplicates are identified by datetime_he, asset, name, ops_type, service."""
        df = pd.DataFrame({
            "datetime_he": [pd.Timestamp("2025-01-15 12:00:00"), pd.Timestamp("2025-01-15 12:00:00")],
            "asset": ["A", "A"],
            "name": ["A", "A"],
            "ops_type": ["G", "G"],
            "service": ["e", "e"],
            "extra": [100, 200],
        })
        result = filter_duplicate_rows(df)
        assert len(result) == 1
        assert result["extra"].iloc[0] == 100
