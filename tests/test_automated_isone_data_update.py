"""
Unit tests for automated_isone_data_update (input validation only).
These tests do not require a database; they only verify that invalid inputs raise ValueError
before any DB or API calls.
"""
import pytest

try:
    from src.automated_isone_data_update import automated_isone_data_update
except ImportError as e:
    automated_isone_data_update = None
    _import_error = e


def _skip_if_no_module():
    if automated_isone_data_update is None:
        pytest.skip(
            f"automated_isone_data_update not importable (e.g. flp_database_connector not on path): {_import_error}"
        )


class TestAutomatedIsoneDataUpdateValidation:
    """Validation tests that run without database."""

    def test_unsupported_table_raises(self):
        _skip_if_no_module()
        with pytest.raises(ValueError, match="not yet supported"):
            automated_isone_data_update(
                "user", "token", table_name="invalid.table", tz="America/New_York"
            )

    def test_ops_table_requires_mis_report(self):
        _skip_if_no_module()
        with pytest.raises(ValueError, match="mis_report is required"):
            automated_isone_data_update(
                "user", "token",
                table_name="ops.isone_hourly_ancillary",
                tz="America/New_York",
                mis_report=None,
            )

    def test_offers_table_invalid_market_raises(self):
        _skip_if_no_module()
        with pytest.raises(ValueError, match="market must be one of"):
            automated_isone_data_update(
                "user", "token",
                table_name="offers.flp_isone_energy",
                tz="America/New_York",
                market="invalid_market",
            )

    def test_offers_ancillary_table_invalid_market_raises(self):
        _skip_if_no_module()
        with pytest.raises(ValueError, match="market must be one of"):
            automated_isone_data_update(
                "user", "token",
                table_name="offers.flp_isone_ancillary",
                tz="America/New_York",
                market="invalid_market",
            )

    def test_offers_table_invalid_offers_ops_type_mode_raises(self):
        _skip_if_no_module()
        with pytest.raises(ValueError, match="offers_ops_type_mode must be one of"):
            automated_isone_data_update(
                "user", "token",
                table_name="offers.flp_isone_energy",
                tz="America/New_York",
                market="DA",
                offers_ops_type_mode="Invalid",
            )

    def test_ops_other_table_unsupported_raises(self):
        _skip_if_no_module()
        with pytest.raises(ValueError, match="not yet supported"):
            automated_isone_data_update(
                "user", "token", table_name="ops.other_table", tz="America/New_York",
                mis_report="SD_DAASCLEARED",
            )
