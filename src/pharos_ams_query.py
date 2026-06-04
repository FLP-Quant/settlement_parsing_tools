import base64
import json
import io
import os
import ast
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
    offer_product: str = "energy",
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
    offer_product : str, optional
        One of: 'energy', 'ancillary'. Controls the API endpoint path used.

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

    offer_product = offer_product.strip().lower()
    if offer_product not in ("energy", "ancillary"):
        raise ValueError("offer_product must be one of ('energy', 'ancillary')")

    if offer_product == "energy":
        if ops_type == "Generation":
            path = "/api/isone/schedule_offers/historic"
        elif ops_type == "Pumping":
            # ARD endpoint (per docs): /api/isone/ard/schedule_offers/historic
            path = "/api/isone/ard/schedule_offers/historic"
        else:
            raise ValueError("ops_type must be one of ('Generation', 'Pumping')")
    else:
        if ops_type == "Generation":
            path = "/api/isone/unit/ancillary_service/schedule_offer_hourly/historic"
        elif ops_type == "Pumping":
            path = "/api/isone/ard/ancillary_service/schedule_offer_hourly/historic"
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

    # Max Daily Award Limit (MDAL) — keep when the API provides it (alternate spellings).
    if "max_daily_award_limit" not in df.columns:
        for alt in ("MaxDailyAwardLimit", "maxDailyAwardLimit", "mdal", "MDAL"):
            if alt in df.columns:
                df = df.rename(columns={alt: "max_daily_award_limit"})
                break

    # Keep only the canonical table columns so new API fields do not break DB append.
    # This still preserves the expected schedule-offers payload used downstream.
    SCHEDULE_OFFERS_COLUMN_ORDER = [
        "datetime_hb", "market_type", "mw_0", "price_0", "mw_1", "price_1", "mw_2", "price_2",
        "mw_3", "price_3", "mw_4", "price_4", "mw_5", "price_5", "mw_6", "price_6",
        "mw_7", "price_7", "mw_8", "price_8", "mw_9", "price_9",
        "name", "asset", "service", "ops_type", "datetime_he",
        "max_daily_award_limit",
        "update_timestamp", "update_user", "date", "he",
    ]
    order_cols = [c for c in SCHEDULE_OFFERS_COLUMN_ORDER if c in df.columns]
    df = df[order_cols]

    if "max_daily_award_limit" in df.columns:
        df["max_daily_award_limit"] = pd.to_numeric(
            df["max_daily_award_limit"], errors="coerce"
        )

    return df


def process_ancillary_schedule_offers_historic(
    df: pd.DataFrame,
    mapping_path: str,
    *,
    tz: str = "America/New_York",
) -> pd.DataFrame:
    """
    Post-process raw ancillary schedule offers (generation + ARD) into DB-ready format.

    Output schema mirrors the standardized offers workflow:
    - datetime_hb (tz-aware), datetime_he
    - market_type (DA/RT), ops_type
    - name, asset, service
    - da_volume (from MW), price (service-specific ancillary offer price)
    - max_daily_award_limit (MDAL) when present in the API payload
    """
    if df.empty:
        return df.copy()

    df = df.copy()

    # Some ancillary endpoints return nested payload rows like:
    # columns ['schedule_offer_hourly', 'ops_type'] where schedule_offer_hourly
    # contains JSON/list records. Expand those into a flat DataFrame first.
    if "schedule_offer_hourly" in df.columns:
        expanded_rows = []
        for _, row in df.iterrows():
            payload = row.get("schedule_offer_hourly")
            row_ops_type = row.get("ops_type")

            # Parse payload if it's a JSON/string container.
            if isinstance(payload, str):
                parsed = None
                for parser in (json.loads, ast.literal_eval):
                    try:
                        parsed = parser(payload)
                        break
                    except Exception:
                        continue
                payload = parsed if parsed is not None else payload

            # Normalize payload into list[dict]
            records = []
            if isinstance(payload, dict):
                if "schedule_offer_hourly" in payload:
                    inner = payload["schedule_offer_hourly"]
                    if isinstance(inner, list):
                        records.extend(inner)
                    elif isinstance(inner, dict):
                        records.append(inner)
                else:
                    records.append(payload)
            elif isinstance(payload, list):
                records.extend(payload)

            for rec in records:
                if isinstance(rec, dict):
                    rec = rec.copy()
                    if "ops_type" not in rec and row_ops_type is not None:
                        rec["ops_type"] = row_ops_type
                    expanded_rows.append(rec)

        if expanded_rows:
            df = pd.json_normalize(expanded_rows)
        else:
            raise ValueError(
                "Ancillary offers payload appears nested, but could not be expanded. "
                f"Incoming columns: {list(df.columns)}"
            )

    if "max_daily_award_limit" not in df.columns:
        for alt in ("MaxDailyAwardLimit", "maxDailyAwardLimit", "mdal", "MDAL"):
            if alt in df.columns:
                df = df.rename(columns={alt: "max_daily_award_limit"})
                break

    # Filter out placeholder rows if present
    if "hour_ending" in df.columns:
        df = df[df["hour_ending"] != "Default"].copy()

    # Match the energy offers behavior: keep the standard price-based schedule.
    # Ancillary offers use schedule_id for the same schedule dimension that would
    # otherwise create multiple rows for the same hour/resource/service key.
    if "schedule_id" in df.columns:
        schedule_id = pd.to_numeric(df["schedule_id"], errors="coerce")
        before_schedule_filter = len(df)
        df = df[schedule_id == 12].copy()
        removed_schedule_rows = before_schedule_filter - len(df)
        if removed_schedule_rows > 0:
            print(
                f"Filtered ancillary offers to schedule_id == 12; "
                f"removed {removed_schedule_rows} non-target schedule rows."
            )

    # Normalize known service price columns from API/XML conventions
    service_price_map = {
        "tmsrprice": "TMSR",
        "tmsr_price": "TMSR",
        "tmsr": "TMSR",
        "tmnsrprice": "TMNSR",
        "tmnsr_price": "TMNSR",
        "tmnsr": "TMNSR",
        "tmorprice": "TMOR",
        "tmor_price": "TMOR",
        "tmor": "TMOR",
        "eirprice": "EIR",
        "eir_price": "EIR",
        "eir": "EIR",
    }
    normalized_cols = {c.lower(): c for c in df.columns}
    available_price_cols = []
    for k in service_price_map:
        if k in normalized_cols:
            available_price_cols.append(normalized_cols[k])
    if not available_price_cols:
        raise ValueError(
            "Expected at least one ancillary service price column in data "
            "(TmsrPrice/TmnsrPrice/TmorPrice/EirPrice). "
            f"Columns: {list(df.columns)}"
        )

    # Melt wide service columns to long service rows
    id_vars = [c for c in df.columns if c not in available_price_cols]
    melted = df.melt(
        id_vars=id_vars,
        value_vars=available_price_cols,
        var_name="service_col",
        value_name="price",
    )
    melted["service"] = (
        melted["service_col"]
        .astype(str)
        .str.lower()
        .map(service_price_map)
    )
    melted = melted[melted["service"].notna()].copy()
    melted = melted.drop(columns=["service_col"], errors="ignore")

    # Drop rows where price is unavailable (0 is valid, NaN is not)
    melted["price"] = pd.to_numeric(melted["price"], errors="coerce")
    melted = melted[melted["price"].notna()].copy()

    # Volume column from MW
    mw_col = None
    for candidate in ("MW", "mw", "da_volume", "volume"):
        if candidate in melted.columns:
            mw_col = candidate
            break
    if mw_col is None:
        raise ValueError(
            "Expected ancillary offers MW column ('MW' or 'mw') in data. "
            f"Columns: {list(melted.columns)}"
        )
    melted["da_volume"] = pd.to_numeric(melted[mw_col], errors="coerce")

    # Standardize market to market_type and DA/RT naming
    if "market" in melted.columns and "market_type" not in melted.columns:
        melted = melted.rename(columns={"market": "market_type"})
    if "market_type" in melted.columns:
        melted["market_type"] = (
            melted["market_type"]
            .replace({"day_ahead": "DA", "real_time": "RT"})
        )
    else:
        # Default to DA if upstream payload omits market_type (common in DA hourly exports)
        melted["market_type"] = "DA"

    # Map resource identifiers to standardized name + asset.
    # Ancillary payloads commonly use pnode_id/location instead of unit_name.
    mapping = retrieve_isone_location_map(mapping_path)
    mapping_name = mapping[["ISO-NE Name", "FLP Asset Name"]].drop_duplicates(
        subset=["ISO-NE Name"], keep="first"
    )

    mapped = False
    if "pnode_id" in melted.columns and "PNode ID" in mapping.columns:
        mapping_pnode = mapping[["PNode ID", "ISO-NE Name", "FLP Asset Name"]].dropna(
            subset=["PNode ID"]
        ).drop_duplicates(subset=["PNode ID"], keep="last")
        melted["pnode_id"] = melted["pnode_id"].astype(str)
        mapping_pnode["PNode ID"] = mapping_pnode["PNode ID"].astype(str)
        melted = melted.merge(
            mapping_pnode,
            left_on="pnode_id",
            right_on="PNode ID",
            how="left",
        )
        melted = melted.rename(columns={"ISO-NE Name": "name", "FLP Asset Name": "asset"})
        mapped = True

    # Fallback: map by location/name-like fields where pnode_id mapping did not apply.
    if not mapped or "name" not in melted.columns or melted["name"].isna().all():
        name_key = None
        for candidate in ("unit_name", "short_name", "name", "location"):
            if candidate in melted.columns:
                name_key = candidate
                break
        if name_key is None:
            raise ValueError(
                "Could not map ancillary offers to assets. Expected one of "
                "'pnode_id', 'unit_name', 'short_name', 'name', or 'location' in payload. "
                f"Columns: {list(melted.columns)}"
            )
        fallback = melted.merge(
            mapping_name,
            left_on=name_key,
            right_on="ISO-NE Name",
            how="left",
        ).rename(columns={"ISO-NE Name": "name_fallback", "FLP Asset Name": "asset_fallback"})

        if "name" in fallback.columns:
            fallback["name"] = fallback["name"].fillna(fallback["name_fallback"])
        else:
            fallback["name"] = fallback["name_fallback"]
        if "asset" in fallback.columns:
            fallback["asset"] = fallback["asset"].fillna(fallback["asset_fallback"])
        else:
            fallback["asset"] = fallback["asset_fallback"]
        melted = fallback.drop(columns=["name_fallback", "asset_fallback"], errors="ignore")

    # Convert timestamps
    time_col = "timestamp" if "timestamp" in melted.columns else ("time" if "time" in melted.columns else None)
    if time_col is None:
        raise ValueError(
            "Expected 'timestamp' or 'time' column in ancillary offers data. "
            f"Columns: {list(melted.columns)}"
        )
    melted["datetime_hb"] = pd.to_datetime(melted[time_col], utc=True).dt.tz_convert(tz)
    melted["datetime_he"] = melted["datetime_hb"] + pd.Timedelta(hours=1)

    if "max_daily_award_limit" in melted.columns:
        melted["max_daily_award_limit"] = pd.to_numeric(
            melted["max_daily_award_limit"], errors="coerce"
        )
    else:
        melted["max_daily_award_limit"] = pd.NA

    # Keep final standardized columns first, then drop raw extras.
    final_cols = [
        "datetime_hb", "datetime_he", "market_type", "name", "asset",
        "ops_type", "service", "da_volume", "price", "max_daily_award_limit",
    ]
    for col in final_cols:
        if col not in melted.columns:
            melted[col] = pd.NA
    melted = melted[final_cols]

    return melted