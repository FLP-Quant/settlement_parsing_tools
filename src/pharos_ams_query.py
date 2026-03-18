import base64
import io
import os
from urllib.parse import urlencode
import requests
import pandas as pd
from typing import Optional
from IPython.display import display, HTML

from src.parsers import retrieve_isone_location_map


def base64_encode(text: str) -> str:
    """
    Encode text to bytes (try ASCII, fall back to UTF-8),
    then base64-encode and return an ASCII string (no newline).
    """
    try:
        b = text.encode("ascii")
    except UnicodeEncodeError:
        b = text.encode("utf-8")
    return base64.b64encode(b).decode("ascii")


def query_ams_with_basic_auth(
    url_string: str,
    api_token: str,
    *,
    token_is_preencoded: bool = False,
    use_requests_auth_if_possible: bool = True,
    timeout: int = 30,
    save_to_file: Optional[str] = None,
    csv_read_kwargs: Optional[dict] = None,
    json_read_kwargs: Optional[dict] = None,
) -> pd.DataFrame:
    """
    Query AMS-Pharos API and return response as a pandas DataFrame.

    Parameters
    ----------
    url_string : str
        Full URL to query.
    api_token : str
        Either "username:password" or a pre-encoded Base64 token depending on
        token_is_preencoded.
    token_is_preencoded : bool, optional
        If True, `api_token` is used directly as the Base64 part of the
        Authorization header. Default False.
    use_requests_auth_if_possible : bool, optional
        If api_token contains ":" and this is True, uses requests' auth=(user, pass).
    timeout : int, optional
        Seconds to wait for the request. Default 30.
    save_to_file : Optional[str], optional
        If provided, the raw response (text or bytes) will be saved to this path.
    csv_read_kwargs : Optional[dict], optional
        Extra kwargs passed to `pd.read_csv` when parsing CSV (e.g. `sep`, `parse_dates`).
    json_read_kwargs : Optional[dict], optional
        Extra kwargs used when normalizing JSON via `pd.json_normalize`.

    Returns
    -------
    pd.DataFrame
        Parsed DataFrame.

    Raises
    ------
    requests.HTTPError
        If the HTTP request returns an error status.
    ValueError
        If the response cannot be parsed as CSV or JSON.
    """

    headers = {}
    # Prepare request and run it
    if token_is_preencoded:
        headers["Authorization"] = "Basic " + api_token
        resp = requests.get(url_string, headers=headers, timeout=timeout)
    else:
        if use_requests_auth_if_possible and ":" in api_token:
            user, passwd = api_token.split(":", 1)
            resp = requests.get(url_string, headers=headers, auth=(user, passwd), timeout=timeout)
        else:
            headers["Authorization"] = "Basic " + base64_encode(api_token)
            resp = requests.get(url_string, headers=headers, timeout=timeout)

    resp.raise_for_status()

    # Ensure resp.text is properly populated if empty
    if not resp.text and resp.content:
        resp.encoding = resp.encoding or 'utf-8'
        # Force re-decode
        try:
            text_content = resp.content.decode(resp.encoding)
        except UnicodeDecodeError:
            # Fallback to utf-8 if specified encoding fails
            text_content = resp.content.decode('utf-8', errors='replace')
    else:
        text_content = resp.text

    if resp.status_code != 200:
        raise ValueError(f"API request failed with status code: {resp.status_code}")
    elif len(resp.content) == 0 and len(text_content) == 0 and len(resp.text) == 0:
        # Debug: Check what we actually received
        print(f"Response status: {resp.status_code}")
        print(f"Content-Type: {resp.headers.get('Content-Type')}")
        print(f"First 200 bytes of resp.content: {resp.content[:200]}")
        raise ValueError("No content received from the API response. Check that URL is correct and data exists for the given date range.")

    # Optionally save raw response
    if save_to_file:
        # If it looks like binary (non-text), save bytes; else save text
        content_type = resp.headers.get("Content-Type", "").lower()
        try:
            if "text" in content_type or "json" in content_type or "csv" in content_type:
                # write text using resp.encoding if available
                encoding = resp.encoding or "utf-8"
                with open(save_to_file, "w", encoding=encoding) as fh:
                    fh.write(text_content)
            else:
                # fallback: write raw bytes
                with open(save_to_file, "wb") as fh:
                    fh.write(resp.content)
        except Exception as e:
            # non-fatal; warn
            print(f"Warning: failed to save response to {save_to_file}: {e}")

    # Parsing logic
    content_type = resp.headers.get("Content-Type", "").lower()

    csv_read_kwargs = csv_read_kwargs or {}
    json_read_kwargs = json_read_kwargs or {}

    # Helper function to find header row in CSV (looks for row starting with "H")
    def _try_csv_find_header(text: str, csv_read_kwargs: dict):
        """Try to find the header row by looking for a row starting with 'H'"""
        lines = text.split('\n')
        header_idx = None
        for i, line in enumerate(lines):
            # Look for header row (starts with "H," or similar pattern)
            stripped = line.strip()
            if stripped.startswith('"H",') or stripped.startswith("H,") or stripped.startswith('H,"'):
                header_idx = i
                break
        
        if header_idx is not None:
            # Skip rows before header, use header row as column names
            # The header row starts with "H" which is a code, not a column name
            # We'll let pandas parse it and it will become the first column name
            try:
                df = pd.read_csv(io.StringIO(text), skiprows=header_idx, header=0, **csv_read_kwargs)
                # If the first column is named "H" or similar, we could drop it, but for now keep it
                return df
            except Exception:
                # If that fails, try without the header row and let pandas infer
                return pd.read_csv(io.StringIO(text), skiprows=header_idx + 1, **csv_read_kwargs)
        return None
    
    # Helper function for ISONE RTLOCSUM format (same approach as RealTimeOps class)
    def _try_csv_rtlocsum_format(text: str, csv_read_kwargs: dict):
        """
        Parse CSV using the same approach as RealTimeOps class:
        - Read all rows with numbered columns (names=range(40))
        - Return raw DataFrame with numbered columns (RealTimeOps will process it)
        This matches how RealTimeOps expects the data: pd.read_csv(..., names=range(40))
        """
        try:
            # Read CSV with numbered columns (no header), handling jagged lines
            # This is exactly what RealTimeOps does: pd.read_csv(..., engine="python", names=range(40))
            df = pd.read_csv(io.StringIO(text), engine="python", names=range(40), **csv_read_kwargs)
            
            # Check if we have at least some rows
            if len(df) > 0:
                return df
            return None
        except Exception:
            return None
    
    # Helper for trying CSV
    def try_csv(text: str):
        if not text or len(text.strip()) == 0:
            return None
        
        # Normalize line endings (handle Windows/Unix/Mac line endings)
        text_normalized = text.replace('\r\n', '\n').replace('\r', '\n')
            
        # Try multiple parsing strategies
        strategies = [
            # Strategy 1: ISONE RTLOCSUM format (same as RealTimeOps class) - most reliable for this format
            lambda: _try_csv_rtlocsum_format(text_normalized, csv_read_kwargs),
            # Strategy 2: Try to find header row dynamically (look for row starting with "H")
            lambda: _try_csv_find_header(text_normalized, csv_read_kwargs),
            # Strategy 3: Skip first 4 metadata rows, use next row as header (common ISONE format)
            lambda: pd.read_csv(io.StringIO(text_normalized), skiprows=4, header=0, **csv_read_kwargs),
            # Strategy 4: Skip first 4 rows with default settings (let pandas infer header)
            lambda: pd.read_csv(io.StringIO(text_normalized), skiprows=4, **csv_read_kwargs),
            # Strategy 5: Standard parsing with user-provided kwargs (fallback)
            lambda: pd.read_csv(io.StringIO(text_normalized), **csv_read_kwargs),
        ]
        
        for strategy in strategies:
            try:
                df = strategy()
                if df is not None and len(df) > 0:
                    return df
            except Exception:
                # Continue to next strategy
                continue
        
        # If all strategies failed, return None (error will be raised by caller)
        return None

    # Helper for trying JSON
    def try_json():
        try:
            j = resp.json()
            # If it's a list/dict convertible into a table, normalize it
            if isinstance(j, list):
                return pd.json_normalize(j, **json_read_kwargs)
            elif isinstance(j, dict):
                # if dict contains a top-level array with a likely data key, attempt to find it
                # common patterns: {'data': [...]}, {'rows': [...]}, {'schedules': [...]}, etc.
                for candidate in ("data", "results", "rows", "items", "schedules"):
                    if candidate in j and isinstance(j[candidate], list):
                        return pd.json_normalize(j[candidate], **json_read_kwargs)
                # otherwise normalize the whole dict; json_normalize will produce a single-row frame
                return pd.json_normalize(j, **json_read_kwargs)
            else:
                return None
        except ValueError:
            return None

    # Prefer parsing based on content-type
    df = None
    if "application/json" in content_type or content_type.endswith("+json"):
        df = try_json()
        if df is None:
            # fallback to CSV parsing of text
            df = try_csv(text_content)
    elif "text/csv" in content_type or "csv" in content_type or "text/plain" in content_type:
        df = try_csv(text_content)
        if df is None:
            df = try_json()
    else:
        # Unknown content-type — try CSV first, then JSON
        df = try_csv(text_content)
        if df is None:
            df = try_json()

    if df is None:
        # Nothing parsed — raise detailed error with small snippet of response for debugging
        snippet = text_content[:1000] if text_content else "<binary content>"
        # Try to provide more context about why parsing failed
        error_msg = (
            f"Unable to parse response as CSV or JSON. "
            f"Content-Type: {resp.headers.get('Content-Type', 'unknown')!r}. "
            f"Response length: {len(text_content) if text_content else 0} characters. "
            f"First 1000 chars: {snippet!r}"
        )
        raise ValueError(error_msg)

    return df


# Default base URL for Pharos AMS API (can be overridden for testing)
DEFAULT_PHAROS_BASE_URL = "https://ams.pharos-ei.com"


def query_schedule_offers_historic(
    api_token: str,
    organization_key: str,
    start_date: str,
    market: str,
    *,
    end_date: Optional[str] = None,
    base_url: str = DEFAULT_PHAROS_BASE_URL,
    timeout: int = 30,
    save_to_file: Optional[str] = None,
    csv_read_kwargs: Optional[dict] = None,
    ops_type: Optional[str] = "Generation",
) -> pd.DataFrame:
    """
    Query the ISONE schedule offer price data (historic) API.

    GET /api/isone/schedule_offers/historic
    Returns the schedule offer price data as CSV.

    Parameters
    ----------
    api_token : str
        Either "username:password" or a pre-encoded Base64 token (see query_ams_with_basic_auth).
    organization_key : str
        Key to limit the query to only the firms in an organization or a specific firm.
    start_date : str
        Start date in YYYY-MM-DD format, or relative word: 'yesterday', 'today', 'tomorrow'.
        No more than 365 days may be requested.
    market : str
        One of: 'day_ahead', 'reoffer', 'real_time'.
    end_date : str, optional
        End date in YYYY-MM-DD format, or relative word. If empty, defaults to start_date.
        No more than 365 days may be requested.
    base_url : str, optional
        API base URL. Defaults to https://ams.pharos-ei.com.
    timeout : int, optional
        Request timeout in seconds. Default 30.
    save_to_file : str, optional
        If provided, the raw CSV response is saved to this path.
    csv_read_kwargs : dict, optional
        Extra kwargs passed to pd.read_csv when parsing the response.
    ops_type: Optional[str], optional
        One of: 'Generation', 'Pumping'. Default 'Generation'.

    Returns
    -------
    pd.DataFrame
        Schedule offer price data.

    Raises
    ------
    ValueError
        If market is not one of day_ahead, reoffer, real_time.
    """
    market = market.strip().lower()
    # API accepts day_ahead, real_time, reoffer (caller may pass these from DA/RT conversion)
    allowed = ("day_ahead", "real_time", "reoffer")
    if market not in allowed:
        raise ValueError(f"market must be one of {allowed!r}, got {market!r}")

    if ops_type == "Generation":
        path = "/api/isone/schedule_offers/historic"
    elif ops_type == "Pumping":
        # ARD endpoint (per docs): /api/isone/ard/schedule_offers/historic
        path = "/api/isone/ard/schedule_offers/historic"
    else:
        raise ValueError("ops_type must be one of ('Generation', 'Pumping')")

    params = {
        "organization_key": organization_key,
        "start_date": start_date.strip(),
        "market": market,
    }
    if end_date is not None and end_date.strip():
        params["end_date"] = end_date.strip()

    url = f"{base_url.rstrip('/')}{path}?{urlencode(params)}"

    df = query_ams_with_basic_auth(
        url,
        api_token,
        timeout=timeout,
        save_to_file=save_to_file,
        csv_read_kwargs=csv_read_kwargs or {},
    )
    if not df.empty:
        df = df.copy()
        df["ops_type"] = ops_type
    return df


def process_schedule_offers_historic(
    df: pd.DataFrame,
    mapping_path: str,
    *,
    tz: str = "America/New_York",
) -> pd.DataFrame:
    """
    Post-process raw schedule offers historic DataFrame to match project conventions.

    - Keeps only schedule_type_id == 12 (price-based; 95-99 are cost-based per ISO-NE).
      Generation uses sched_type_id; Pumping (ARD) uses ard_sched_type_id / ard_schedule_type_id.
    - Drops rows where hour_ending == "Default".
    - Maps unit_id/unit_name/iso_id to name and asset via ISONE Location Mapping; drops those raw columns.
    - Converts timestamp to datetime_hb with timezone.
    - Adds service="energy"; ops_type is set upstream in query_schedule_offers_historic. Renames market -> market_type.
    """
    if df.empty:
        return df.copy()

    if "price_9" not in df.columns:
        raise ValueError("Expected column 'price_9' in schedule offers data")

    # Per ISO-NE eMarket Users Guide: 0-94 are price-based schedules, 95-99 are
    # cost-based schedules reserved for use by ISO New England. Keep only 12.
    # Do this before dropping columns to the right of price_9 (sched_type_id lives there).
    sched_col = None
    if "sched_type_id" in df.columns:
        sched_col = "sched_type_id"
    elif "ard_sched_type_id" in df.columns:
        sched_col = "ard_sched_type_id"
    elif "ard_schedule_type_id" in df.columns:
        sched_col = "ard_schedule_type_id"
    if sched_col is None:
        raise ValueError(
            "Expected one of 'sched_type_id', 'ard_sched_type_id', or 'ard_schedule_type_id' "
            f"in schedule offers data. Columns: {list(df.columns)}"
        )
    df = df[df[sched_col] == 12].copy()

    # Filter out placeholder rows (before dropping columns)
    df = df[df["hour_ending"] != "Default"].copy()

    # Map unit_name -> name and asset using same ISONE Location Mapping as other tables
    mapping = retrieve_isone_location_map(mapping_path)
    mapping = mapping[["ISO-NE Name", "FLP Asset Name"]].drop_duplicates(
        subset=["ISO-NE Name"], keep="first"
    )
    df = df.merge(
        mapping,
        left_on="unit_name",
        right_on="ISO-NE Name",
        how="left",
    )
    df = df.rename(columns={"FLP Asset Name": "asset", "ISO-NE Name": "name"})
    df = df.drop(
        columns=["unit_id", "unit_name", "iso_id", "sched_type_id", "ard_sched_type_id", "ard_schedule_type_id", "firm"],
        errors="ignore",
    )

    # timestamp is hour-ending time; align with project convention as datetime_hb
    df["datetime_hb"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(tz)
    df = df.drop(columns=["timestamp"], errors="ignore")

    # Add columns consistent with other tables
    df["service"] = "energy"
    # ops_type is added upstream in query_schedule_offers_historic

    # Rename market for clarity and normalize to DA/RT (API returns day_ahead/real_time)
    df = df.rename(columns={"market": "market_type"})
    df["market_type"] = df["market_type"].replace({"day_ahead": "DA", "real_time": "RT"})

    # Enforce column order so Generation and Pumping outputs match (avoids append mismatches)
    SCHEDULE_OFFERS_COLUMN_ORDER = [
        "datetime_hb", "market_type", "mw_0", "price_0", "mw_1", "price_1", "mw_2", "price_2",
        "mw_3", "price_3", "mw_4", "price_4", "mw_5", "price_5", "mw_6", "price_6",
        "mw_7", "price_7", "mw_8", "price_8", "mw_9", "price_9",
        "name", "asset", "service", "ops_type", "datetime_he",
        "update_timestamp", "update_user", "date", "he",
    ]
    order_cols = [c for c in SCHEDULE_OFFERS_COLUMN_ORDER if c in df.columns]
    extra = [c for c in df.columns if c not in SCHEDULE_OFFERS_COLUMN_ORDER]
    df = df[order_cols + extra]

    return df