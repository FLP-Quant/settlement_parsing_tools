"""
Unit tests for src.pharos_ams_query (base64_encode, process_schedule_offers_historic).
"""
import pandas as pd
import pytest

from src.pharos_ams_query import (
    base64_encode,
    process_schedule_offers_historic,
    process_ancillary_schedule_offers_historic,
)


class TestBase64Encode:
    """Tests for base64_encode."""

    def test_ascii_string(self):
        assert base64_encode("user:pass") == "dXNlcjpwYXNz"

    def test_utf8_fallback(self):
        # Non-ASCII character should use UTF-8
        encoded = base64_encode("user:passé")
        import base64
        decoded = base64.b64decode(encoded).decode("utf-8")
        assert decoded == "user:passé"

    def test_returns_no_newline(self):
        result = base64_encode("hello")
        assert "\n" not in result
        assert result.isascii()


class TestProcessScheduleOffersHistoric:
    """Tests for process_schedule_offers_historic."""

    def test_empty_dataframe_returns_empty(self, isone_mapping_csv):
        result = process_schedule_offers_historic(
            pd.DataFrame(), isone_mapping_csv, tz="America/New_York"
        )
        assert result.empty
        assert isinstance(result, pd.DataFrame)

    def test_missing_price_9_raises(self, isone_mapping_csv):
        df = pd.DataFrame({"hour_ending": [1], "market": ["day_ahead"], "sched_type_id": [12]})
        with pytest.raises(ValueError, match="Expected column 'price_9'"):
            process_schedule_offers_historic(df, isone_mapping_csv)

    def test_missing_schedule_type_column_raises(self, isone_mapping_csv):
        cols = ["hour_ending", "market", "mw_0", "price_0", "mw_1", "price_1", "mw_2", "price_2",
                "mw_3", "price_3", "mw_4", "price_4", "mw_5", "price_5", "mw_6", "price_6",
                "mw_7", "price_7", "mw_8", "price_8", "mw_9", "price_9",
                "unit_id", "unit_name", "iso_id", "timestamp"]
        df = pd.DataFrame({c: [1] for c in cols})
        with pytest.raises(ValueError, match="sched_type_id|ard_sched_type_id|ard_schedule_type_id"):
            process_schedule_offers_historic(df, isone_mapping_csv)

    def test_filters_to_schedule_type_12_and_preserves_ops_type(
        self, isone_mapping_csv, sample_schedule_offers_columns
    ):
        """Process minimal valid DataFrame; assert sched_type_id==12 kept, ops_type preserved."""
        tz = "America/New_York"
        df = pd.DataFrame({
            "hour_ending": [12, 12],
            "market": ["day_ahead", "day_ahead"],
            "mw_0": [10, 20], "price_0": [1, 2], "mw_1": [0, 0], "price_1": [0, 0],
            "mw_2": [0, 0], "price_2": [0, 0], "mw_3": [0, 0], "price_3": [0, 0],
            "mw_4": [0, 0], "price_4": [0, 0], "mw_5": [0, 0], "price_5": [0, 0],
            "mw_6": [0, 0], "price_6": [0, 0], "mw_7": [0, 0], "price_7": [0, 0],
            "mw_8": [0, 0], "price_8": [0, 0], "mw_9": [0, 0], "price_9": [0, 0],
            "unit_id": [1, 2], "unit_name": ["ASSET_A", "UNIT_X"], "iso_id": [100, 101],
            "timestamp": ["2025-01-15T17:00:00Z", "2025-01-15T18:00:00Z"],
            "sched_type_id": [12, 99],  # 99 should be dropped
            "firm": [0, 0],
            "ops_type": ["Generation", "Pumping"],
        })
        result = process_schedule_offers_historic(df, isone_mapping_csv, tz=tz)
        assert len(result) == 1  # only sched_type_id==12 row kept; sched_type_id column is dropped
        assert "ops_type" in result.columns
        assert result["ops_type"].iloc[0] == "Generation"
        assert "datetime_he" in result.columns or "datetime_hb" in result.columns
        assert "market_type" in result.columns
        assert "name" in result.columns
        assert "asset" in result.columns
        assert "service" in result.columns

    def test_preserves_max_daily_award_limit_when_present(self, isone_mapping_csv):
        """MDAL / max daily award limit should pass through when the API includes it."""
        tz = "America/New_York"
        df = pd.DataFrame({
            "hour_ending": [12],
            "market": ["day_ahead"],
            "mw_0": [10], "price_0": [1], "mw_1": [0], "price_1": [0],
            "mw_2": [0], "price_2": [0], "mw_3": [0], "price_3": [0],
            "mw_4": [0], "price_4": [0], "mw_5": [0], "price_5": [0],
            "mw_6": [0], "price_6": [0], "mw_7": [0], "price_7": [0],
            "mw_8": [0], "price_8": [0], "mw_9": [0], "price_9": [0],
            "unit_id": [1], "unit_name": ["ASSET_A"], "iso_id": [100],
            "timestamp": ["2025-01-15T17:00:00Z"],
            "sched_type_id": [12], "firm": [0], "ops_type": ["Generation"],
            "max_daily_award_limit": [42.5],
        })
        result = process_schedule_offers_historic(df, isone_mapping_csv, tz=tz)
        assert "max_daily_award_limit" in result.columns
        assert result["max_daily_award_limit"].iloc[0] == 42.5

    def test_filters_out_default_hour_ending(self, isone_mapping_csv):
        """Rows with hour_ending == 'Default' should be dropped."""
        tz = "America/New_York"
        df = pd.DataFrame({
            "hour_ending": ["Default", 12],
            "market": ["day_ahead", "day_ahead"],
            "mw_0": [0, 10], "price_0": [0, 1], "mw_1": [0, 0], "price_1": [0, 0],
            "mw_2": [0, 0], "price_2": [0, 0], "mw_3": [0, 0], "price_3": [0, 0],
            "mw_4": [0, 0], "price_4": [0, 0], "mw_5": [0, 0], "price_5": [0, 0],
            "mw_6": [0, 0], "price_6": [0, 0], "mw_7": [0, 0], "price_7": [0, 0],
            "mw_8": [0, 0], "price_8": [0, 0], "mw_9": [0, 0], "price_9": [0, 0],
            "unit_id": [1, 2], "unit_name": ["ASSET_A", "ASSET_A"], "iso_id": [100, 100],
            "timestamp": ["2025-01-15T16:00:00Z", "2025-01-15T17:00:00Z"],
            "sched_type_id": [12, 12], "firm": [0, 0], "ops_type": ["Generation", "Generation"],
        })
        result = process_schedule_offers_historic(df, isone_mapping_csv, tz=tz)
        assert len(result) == 1
        # hour_ending is not in the canonical output columns; assert we kept the non-Default row.
        assert result["mw_0"].iloc[0] == 10
        assert result["price_0"].iloc[0] == 1
        expected_hb = pd.Timestamp("2025-01-15T17:00:00Z", tz="UTC").tz_convert(tz)
        assert result["datetime_hb"].iloc[0] == expected_hb

    def test_ard_sched_type_id_column_supported(self, isone_mapping_csv):
        """Pumping data uses ard_sched_type_id instead of sched_type_id."""
        df = pd.DataFrame({
            "hour_ending": [12],
            "market": ["day_ahead"],
            "mw_0": [10], "price_0": [1], "mw_1": [0], "price_1": [0],
            "mw_2": [0], "price_2": [0], "mw_3": [0], "price_3": [0],
            "mw_4": [0], "price_4": [0], "mw_5": [0], "price_5": [0],
            "mw_6": [0], "price_6": [0], "mw_7": [0], "price_7": [0],
            "mw_8": [0], "price_8": [0], "mw_9": [0], "price_9": [0],
            "unit_id": [1], "unit_name": ["ASSET_A"], "iso_id": [100],
            "timestamp": ["2025-01-15T17:00:00Z"],
            "ard_sched_type_id": [12], "firm": [0], "ops_type": ["Pumping"],
        })
        result = process_schedule_offers_historic(df, isone_mapping_csv)
        assert len(result) == 1
        assert "ops_type" in result.columns
        assert result["ops_type"].iloc[0] == "Pumping"


class TestProcessAncillaryScheduleOffersHistoric:
    """Tests for process_ancillary_schedule_offers_historic."""

    def test_empty_dataframe_returns_empty(self, isone_mapping_csv):
        result = process_ancillary_schedule_offers_historic(
            pd.DataFrame(), isone_mapping_csv, tz="America/New_York"
        )
        assert result.empty
        assert isinstance(result, pd.DataFrame)

    def test_missing_price_columns_raises(self, isone_mapping_csv):
        df = pd.DataFrame({
            "timestamp": ["2025-01-15T17:00:00Z"],
            "unit_name": ["ASSET_A"],
            "market": ["day_ahead"],
            "ops_type": ["Generation"],
            "MW": [10],
        })
        with pytest.raises(ValueError, match="service price column"):
            process_ancillary_schedule_offers_historic(df, isone_mapping_csv)

    def test_melts_service_prices_and_maps_fields(self, isone_mapping_csv):
        df = pd.DataFrame({
            "timestamp": ["2025-01-15T17:00:00Z"],
            "unit_name": ["ASSET_A"],
            "market": ["day_ahead"],
            "ops_type": ["Generation"],
            "MW": [10],
            "TmsrPrice": [1.25],
            "TmnsrPrice": [2.5],
            "TmorPrice": [3.75],
            "EirPrice": [4.0],
        })
        result = process_ancillary_schedule_offers_historic(df, isone_mapping_csv)
        assert len(result) == 4
        assert set(result["service"]) == {"TMSR", "TMNSR", "TMOR", "EIR"}
        assert (result["market_type"] == "DA").all()
        assert (result["da_volume"] == 10).all()
        assert result["price"].notna().all()
        assert "datetime_hb" in result.columns
        assert "datetime_he" in result.columns
        assert "max_daily_award_limit" in result.columns
        assert result["max_daily_award_limit"].isna().all()

    def test_filters_to_schedule_id_12(self, isone_mapping_csv):
        df = pd.DataFrame({
            "timestamp": ["2025-01-15T17:00:00Z", "2025-01-15T17:00:00Z"],
            "unit_name": ["ASSET_A", "ASSET_A"],
            "market": ["day_ahead", "day_ahead"],
            "ops_type": ["Generation", "Generation"],
            "schedule_id": [12, 99],
            "MW": [10, 20],
            "TmsrPrice": [1.25, 99.0],
            "max_daily_award_limit": [100.0, 200.0],
        })
        result = process_ancillary_schedule_offers_historic(df, isone_mapping_csv)
        assert len(result) == 1
        assert result["da_volume"].iloc[0] == 10
        assert result["price"].iloc[0] == 1.25
        assert result["max_daily_award_limit"].iloc[0] == 100.0
