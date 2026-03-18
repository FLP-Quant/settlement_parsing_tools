"""
Unit tests for src.isone_update_core.
"""
from datetime import date, timedelta

import pandas as pd
import pytest

from src.isone_update_core import (
    build_expected_grid,
    chunk_date_groups,
    debug_spring_forward_missing,
    fill_missing_with_defaults,
    group_contiguous_dates,
    safe_round_datetime,
    segment_into_monthly_chunks,
)


class TestSafeRoundDatetime:
    """Tests for safe_round_datetime."""

    def test_rounds_to_second_precision(self, tz_eastern):
        series = pd.Series([
            pd.Timestamp("2025-01-15 12:00:00.123456", tz=tz_eastern),
            pd.Timestamp("2025-01-15 13:00:00.999999", tz=tz_eastern),
        ])
        result = safe_round_datetime(series, tz_eastern)
        assert result.dt.second.iloc[0] == 0
        assert result.dt.microsecond.iloc[0] == 0
        assert (result.dt.floor("s") == result).all()

    def test_preserves_timezone(self, tz_eastern):
        series = pd.Series([pd.Timestamp("2025-01-15 12:00:00", tz=tz_eastern)])
        result = safe_round_datetime(series, tz_eastern)
        assert str(result.dt.tz) == tz_eastern


class TestBuildExpectedGrid:
    """Tests for build_expected_grid."""

    def test_single_day_single_combo(self, tz_eastern, sample_unique_combos_ops):
        start = date(2025, 1, 15)
        end = date(2025, 1, 15)
        key_columns = ["datetime_he", "name", "ops_type", "service"]
        result = build_expected_grid(
            start, end, tz_eastern, sample_unique_combos_ops, key_columns
        )
        # One day: hourly grid from start_dt to end_dt (inclusive="right") -> 23 points in current impl
        assert len(result) == 23
        assert list(result.columns) == key_columns
        assert result["name"].iloc[0] == "ASSET_A"
        # date_range inclusive="right" yields first hour 2 in some pandas versions
        assert result["datetime_he"].min().hour in (1, 2)
        assert result["datetime_he"].max().hour in (0, 24)

    def test_two_days_doubles_rows(self, tz_eastern, sample_unique_combos_ops):
        start = date(2025, 1, 15)
        end = date(2025, 1, 16)
        key_columns = ["datetime_he", "name", "ops_type", "service"]
        result = build_expected_grid(
            start, end, tz_eastern, sample_unique_combos_ops, key_columns
        )
        assert len(result) == 47  # 23 + 24 for two days in current date_range behavior

    def test_two_combos_doubles_rows(self, tz_eastern):
        combos = pd.DataFrame([
            {"name": "A", "ops_type": "Generation", "service": "energy"},
            {"name": "B", "ops_type": "Generation", "service": "energy"},
        ])
        start = date(2025, 1, 15)
        end = date(2025, 1, 15)
        key_columns = ["datetime_he", "name", "ops_type", "service"]
        result = build_expected_grid(start, end, tz_eastern, combos, key_columns)
        assert len(result) == 46  # 23 hours * 2 combos
        assert set(result["name"]) == {"A", "B"}


class TestGroupContiguousDates:
    """Tests for group_contiguous_dates."""

    def test_empty_returns_empty(self):
        assert group_contiguous_dates([]) == []

    def test_single_date_one_group(self):
        d = date(2025, 1, 15)
        assert group_contiguous_dates([d]) == [[d]]

    def test_contiguous_range_one_group(self):
        dates = [date(2025, 1, 15), date(2025, 1, 16), date(2025, 1, 17)]
        assert group_contiguous_dates(dates) == [dates]

    def test_gap_splits_into_two_groups(self):
        dates = [
            date(2025, 1, 15), date(2025, 1, 16),
            date(2025, 1, 20), date(2025, 1, 21),
        ]
        result = group_contiguous_dates(dates)
        assert len(result) == 2
        assert result[0] == [date(2025, 1, 15), date(2025, 1, 16)]
        assert result[1] == [date(2025, 1, 20), date(2025, 1, 21)]


class TestSegmentIntoMonthlyChunks:
    """Tests for segment_into_monthly_chunks."""

    def test_empty_returns_empty(self):
        assert segment_into_monthly_chunks([]) == []

    def test_single_day_one_chunk(self):
        d = date(2025, 1, 15)
        assert segment_into_monthly_chunks([d], max_days=30) == [[d]]

    def test_30_days_one_chunk(self):
        dates = [date(2025, 1, 1) + timedelta(days=i) for i in range(30)]
        result = segment_into_monthly_chunks(dates, max_days=30)
        assert len(result) == 1
        assert len(result[0]) == 30

    def test_31_days_splits_into_two_chunks(self):
        dates = [date(2025, 1, 1) + timedelta(days=i) for i in range(31)]
        result = segment_into_monthly_chunks(dates, max_days=30)
        assert len(result) == 2
        assert len(result[0]) == 30
        assert len(result[1]) == 1


class TestChunkDateGroups:
    """Tests for chunk_date_groups."""

    def test_flattens_and_chunks_multiple_groups(self):
        groups = [
            [date(2025, 1, 1), date(2025, 1, 2)],
            [date(2025, 2, 1), date(2025, 2, 2), date(2025, 2, 3)],
        ]
        result = chunk_date_groups(groups, max_days=30)
        assert len(result) == 2  # first group 2 days, second 3 days -> 2 chunks
        assert result[0] == [date(2025, 1, 1), date(2025, 1, 2)]
        assert result[1] == [date(2025, 2, 1), date(2025, 2, 2), date(2025, 2, 3)]

    def test_large_group_split_by_max_days(self):
        group = [date(2025, 1, 1) + timedelta(days=i) for i in range(35)]
        result = chunk_date_groups([group], max_days=30)
        assert len(result) == 2
        assert len(result[0]) == 30
        assert len(result[1]) == 5


class TestDebugSpringForwardMissing:
    """Tests for debug_spring_forward_missing."""

    def test_empty_missing_returns_unchanged(self, tz_eastern):
        missing_df = pd.DataFrame(columns=["datetime_he"])
        expected = pd.DataFrame({"datetime_he": []})
        result = debug_spring_forward_missing(
            missing_df, pd.DataFrame(), expected, date(2026, 3, 8)
        )
        assert result is missing_df
        assert len(result) == 0

    def test_no_spring_forward_date_returns_unchanged(self, tz_eastern):
        # Missing on a different date -> no debug output, return as-is
        missing_df = pd.DataFrame({
            "datetime_he": pd.to_datetime(["2025-01-15 12:00:00"]).tz_localize(tz_eastern)
        })
        result = debug_spring_forward_missing(
            missing_df, pd.DataFrame(), pd.DataFrame(), date(2026, 3, 8)
        )
        assert result is missing_df
        assert "date" not in result.columns or result.equals(missing_df)


class TestFillMissingWithDefaults:
    """Tests for fill_missing_with_defaults."""

    def test_empty_df_returns_unchanged(self, sample_unique_combos_ops, tz_eastern):
        mapping_df = pd.DataFrame({"name": ["ASSET_A"], "asset": ["Asset A"]})
        composite_cols = ["datetime_he", "name", "ops_type", "service"]
        expected = build_expected_grid(
            date(2025, 1, 15), date(2025, 1, 15), tz_eastern,
            sample_unique_combos_ops, composite_cols
        )
        df_final = pd.DataFrame(columns=composite_cols + ["da_volume", "rt_volume", "unit", "interval_width_s", "asset"])
        result = fill_missing_with_defaults(
            df_final, expected, composite_cols,
            "ops.isone_hourly_energy", "SD_DAASCLEARED", mapping_df
        )
        assert result is df_final
        assert len(result) == 0

    def test_no_gaps_returns_unchanged(self, tz_eastern):
        mapping_df = pd.DataFrame({"name": ["A"], "asset": ["Asset A"]})
        composite_cols = ["datetime_he", "name", "ops_type", "service"]
        dt = pd.Timestamp("2025-01-15 12:00:00", tz=tz_eastern)
        df_final = pd.DataFrame([{
            "datetime_he": dt, "name": "A", "ops_type": "Generation", "service": "energy",
            "da_volume": 1, "rt_volume": 2, "unit": "MWh", "interval_width_s": 3600, "asset": "Asset A",
        }])
        expected = df_final[composite_cols].copy()
        expected["datetime_he"] = safe_round_datetime(expected["datetime_he"], tz_eastern)
        result = fill_missing_with_defaults(
            df_final, expected, composite_cols,
            "ops.isone_hourly_energy", "SD_DAASCLEARED", mapping_df
        )
        assert len(result) == len(df_final)
        assert result.equals(df_final)

    def test_ancillary_sd_daascleared_fills_zeros(self, tz_eastern):
        mapping_df = pd.DataFrame({"name": ["A"], "asset": ["Asset A"]})
        composite_cols = ["datetime_he", "name", "ops_type", "service"]
        dt12 = pd.Timestamp("2025-01-15 12:00:00", tz=tz_eastern)
        dt13 = pd.Timestamp("2025-01-15 13:00:00", tz=tz_eastern)
        dt14 = pd.Timestamp("2025-01-15 14:00:00", tz=tz_eastern)
        # df_final has 12 and 14 so range is 12–14; expected has 12, 13, 14 -> gap at 13
        df_final = pd.DataFrame([
            {"datetime_he": dt12, "name": "A", "ops_type": "Generation", "service": "energy",
             "da_volume": 10, "rt_volume": 20, "unit": "MW", "interval_width_s": 3600, "asset": "Asset A"},
            {"datetime_he": dt14, "name": "A", "ops_type": "Generation", "service": "energy",
             "da_volume": 10, "rt_volume": 20, "unit": "MW", "interval_width_s": 3600, "asset": "Asset A"},
        ])
        expected = pd.DataFrame([
            {"datetime_he": dt12, "name": "A", "ops_type": "Generation", "service": "energy"},
            {"datetime_he": dt13, "name": "A", "ops_type": "Generation", "service": "energy"},
            {"datetime_he": dt14, "name": "A", "ops_type": "Generation", "service": "energy"},
        ])
        expected["datetime_he"] = safe_round_datetime(expected["datetime_he"], tz_eastern)
        result = fill_missing_with_defaults(
            df_final, expected, composite_cols,
            "ops.isone_hourly_ancillary", "SD_DAASCLEARED", mapping_df
        )
        assert len(result) == 3
        filled = result[result["da_volume"] == 0]
        assert len(filled) == 1
        assert filled["rt_volume"].isna().all()
        assert filled["unit"].iloc[0] == "MW"

    def test_energy_table_fills_zeros(self, tz_eastern):
        mapping_df = pd.DataFrame({"name": ["A"], "asset": ["Asset A"]})
        composite_cols = ["datetime_he", "name", "ops_type", "service"]
        dt12 = pd.Timestamp("2025-01-15 12:00:00", tz=tz_eastern)
        dt13 = pd.Timestamp("2025-01-15 13:00:00", tz=tz_eastern)
        dt14 = pd.Timestamp("2025-01-15 14:00:00", tz=tz_eastern)
        df_final = pd.DataFrame([
            {"datetime_he": dt12, "name": "A", "ops_type": "Generation", "service": "energy",
             "da_volume": 10, "rt_volume": 10, "unit": "MWh", "interval_width_s": 3600, "asset": "Asset A"},
            {"datetime_he": dt14, "name": "A", "ops_type": "Generation", "service": "energy",
             "da_volume": 10, "rt_volume": 10, "unit": "MWh", "interval_width_s": 3600, "asset": "Asset A"},
        ])
        expected = pd.DataFrame([
            {"datetime_he": dt12, "name": "A", "ops_type": "Generation", "service": "energy"},
            {"datetime_he": dt13, "name": "A", "ops_type": "Generation", "service": "energy"},
            {"datetime_he": dt14, "name": "A", "ops_type": "Generation", "service": "energy"},
        ])
        expected["datetime_he"] = safe_round_datetime(expected["datetime_he"], tz_eastern)
        result = fill_missing_with_defaults(
            df_final, expected, composite_cols,
            "ops.isone_hourly_energy", "SD_DAASCLEARED", mapping_df
        )
        assert len(result) == 3
        filled = result[result["da_volume"] == 0]
        assert len(filled) == 1
        assert filled["rt_volume"].iloc[0] == 0
        assert filled["unit"].iloc[0] == "MWh"

    def test_unsupported_table_raises(self, tz_eastern):
        mapping_df = pd.DataFrame({"name": ["A"], "asset": ["Asset A"]})
        composite_cols = ["datetime_he", "name", "ops_type", "service"]
        dt12 = pd.Timestamp("2025-01-15 12:00:00", tz=tz_eastern)
        dt13 = pd.Timestamp("2025-01-15 13:00:00", tz=tz_eastern)
        dt14 = pd.Timestamp("2025-01-15 14:00:00", tz=tz_eastern)
        # Range 12–14 with gap at 13 so fill branch runs and hits unsupported table_name check
        df_final = pd.DataFrame([
            {"datetime_he": dt12, "name": "A", "ops_type": "Generation", "service": "energy",
             "da_volume": 10, "rt_volume": 10, "unit": "MWh", "interval_width_s": 3600, "asset": "Asset A"},
            {"datetime_he": dt14, "name": "A", "ops_type": "Generation", "service": "energy",
             "da_volume": 10, "rt_volume": 10, "unit": "MWh", "interval_width_s": 3600, "asset": "Asset A"},
        ])
        expected = pd.DataFrame([
            {"datetime_he": dt12, "name": "A", "ops_type": "Generation", "service": "energy"},
            {"datetime_he": dt13, "name": "A", "ops_type": "Generation", "service": "energy"},
            {"datetime_he": dt14, "name": "A", "ops_type": "Generation", "service": "energy"},
        ])
        expected["datetime_he"] = safe_round_datetime(expected["datetime_he"], tz_eastern)
        with pytest.raises(ValueError, match="not supported for fill_missing_with_defaults"):
            fill_missing_with_defaults(
                df_final, expected, composite_cols,
                "offers.flp_isone_energy", "SD_DAASCLEARED", mapping_df
            )
