import time
import math
import requests
import json
from pathlib import Path
import pandas as pd
from datetime import date, datetime, timedelta, timezone
from supabase import create_client
import os
from dotenv import load_dotenv

# -----------------
#      METHODS
# -----------------
# ------------- API access helpers -----------------
def _token_is_valid() -> bool:
    # refresh 60s early to avoid edge cases
    return _token is not None and time.time() < (_token_expires_at - 60)

def _load_cached_token_file():
    if CACHE_FILE.exists():
        try:
            data = json.loads(CACHE_FILE.read_text())
            if time.time() < data.get("expires_at", 0) - 60:
                return data.get("access_token"), data.get("expires_at")
        except Exception:
            pass
    return None, 0

def _save_cached_token_file(token, expires_in):
    CACHE_FILE.write_text(json.dumps({
        "access_token": token,
        "expires_at": time.time() + int(expires_in),
    }))

def _fetch_token():
    global _token, _token_expires_at
    body = {
        "client_id": PRICENOW_CLIENT_ID,
        "client_secret": PRICENOW_CLIENT_SECRET,
        "audience": AUDIENCE,
        "grant_type": GRANT_TYPE,
    }
    headers = {
        "content-type": "application/json",
        "pratiq-api-version": AUTH_VERSION_HEADER,
    }
    resp = requests.post(AUTH_URL, json=body, headers=headers, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"Token request failed: {resp.status_code} {resp.text}")
    data = resp.json()
    _token = data["access_token"]
    expires_in = int(data.get("expires_in", 3600))  # default if missing
    _token_expires_at = time.time() + expires_in
    _save_cached_token_file(_token, expires_in)

def _get_token():
    global _token, _token_expires_at
    if _token_is_valid():
        return _token
    # try disk cache
    cached_token, cached_expires_at = _load_cached_token_file()
    if cached_token and time.time() < cached_expires_at - 60:
        _token, _token_expires_at = cached_token, cached_expires_at
        return _token
    # fetch fresh
    _fetch_token()
    return _token

# request wrapper (401 refresh)
def _authed_get(path, params=None):
    token = _get_token()
    url = f"{API_BASE}{path}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "pratiq-api-version": MAIN_API_VERSION,
    }
    resp = requests.get(url, headers=headers, params=params, timeout=30)
    if resp.status_code == 401:
        # refresh once and retry
        _fetch_token()
        headers["Authorization"] = f"Bearer {_get_token()}"
        resp = requests.get(url, headers=headers, params=params, timeout=30)
    return resp

# ------------- database upsert helpers -----------------
# Convert DF to list of dicts, replacing NaN with None.
def _df_to_records(df: pd.DataFrame) -> list[dict]:
    if df.empty:
        return []
    return (
        df.where(pd.notnull(df), None)  # NaN -> None
          .to_dict(orient="records")
    )

# Upsert a DataFrame into Supabase in chunks. on_conflict: column name or comma-separated string or list of column names.
def upsert_df(supabase_client, table: str, df: pd.DataFrame, on_conflict: str | list[str], chunk_size: int = 1000):
    records = _df_to_records(df)
    if not records:
        return

    if isinstance(on_conflict, list):
        on_conflict = ",".join(on_conflict)

    # chunked upserts
    for i in range(0, len(records), chunk_size):
        chunk = records[i:i+chunk_size]
        supabase_client.table(table).upsert(
            chunk,
            on_conflict=on_conflict
        ).execute()

# --------- product API methods ---------
# Returns parsed JSON (dict/list of products)
def get_products(page=0, order_by="name", order_dir="asc") -> dict:
    params = {
        "page": page,
        "orderBy": order_by,
        "orderDirection": order_dir,
    }
    resp = _authed_get("/api/products/admin/", params=params)
    resp.raise_for_status()
    return resp.json()  # dict or list

# ------ pricing API methods -----------
def _authed_get_pricing(path, params=None):
    token = _get_token()
    url = f"{API_BASE}{path}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "pratiq-api-version": PRICING_API_VERSION,
    }
    resp = requests.get(url, headers=headers, params=params, timeout=30)
    if resp.status_code == 401:
        _fetch_token()
        headers["Authorization"] = f"Bearer {_get_token()}"
        resp = requests.get(url, headers=headers, params=params, timeout=30)

    return resp

def get_prices(product_definition_ids, date_from, date_to):
    pid_csv = ",".join(str(x) for x in product_definition_ids) if isinstance(product_definition_ids, (list, tuple, set)) else str(product_definition_ids)
    params = {"productDefinitionIds": pid_csv, "from": date_from, "to": date_to}
    resp = _authed_get_pricing("/api/pricing/admin/prices", params)
    if not resp.ok:
        raise RuntimeError(f"Pricing request failed {resp.status_code}: {resp.text}")

    return resp.json()

# accepts a bare list or a dict wrapper with 'data'/'items'/'results'; returns a list of row dicts
def _extract_rows(payload):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for k in ("data", "items", "results"):
            v = payload.get(k)
            if isinstance(v, list):
                return v
    return []  # nothing usable

def get_prices_page(product_definition_ids, date_from, date_to, page=0, page_size=1000):
    pid_csv = ",".join(str(x) for x in product_definition_ids) if isinstance(product_definition_ids, (list, tuple, set)) else str(product_definition_ids)
    params = {
        "productDefinitionIds": pid_csv,
        "from": date_from,   # 'YYYY-MM-DD'
        "to": date_to,       # 'YYYY-MM-DD'
        "page": page,
        "pageSize": page_size,
    }
    # IMPORTANT: no trailing slash in the path
    resp = _authed_get_pricing("/api/pricing/admin/prices", params)
    if not resp.ok:
        raise RuntimeError(f"Pricing request failed {resp.status_code}: {resp.text}")
    return _extract_rows(resp.json())

# fetch all pages. Returns a flat list of change rows: [{ 'validAt': 'YYYY-MM-DD', 'price': int, 'productDefinitionId': int }, ...]
def get_prices_all(product_definition_ids, date_from, date_to, page_size=1000, max_pages=1000):
    all_rows = []
    page = 0
    while page < max_pages:
        rows = get_prices_page(product_definition_ids, date_from, date_to, page=page, page_size=page_size)
        if not rows:
            break
        all_rows.extend(rows)
        if len(rows) < page_size:
            break
        page += 1
    return all_rows

# inclusive date range generator: d0..d1
def _daterange(d0, d1):
    current = d0
    while current <= d1:
        yield current
        current += timedelta(days=1)

# Build a dense daily grid per productDefinitionId by forward-filling change points.
def forward_fill_daily_grid(change_rows, season_start, season_end):
    # group by productDefinitionId
    by_pid = {}
    for r in change_rows:
        pid = r.get("productDefinitionId")
        valid_at = r.get("validAt")
        price = r.get("price")
        if pid is None or valid_at is None or price is None:
            continue
        by_pid.setdefault(pid, []).append({"validAt": valid_at, "price": price})

    dense = []
    for pid, rows in by_pid.items():
        # sort changes by date
        rows.sort(key=lambda x: x["validAt"])
        # walk the calendar and advance the pointer when we hit the next change
        idx = -1
        current_price = None

        # fast-forward idx/current_price to the last change <= season_start
        for i, r in enumerate(rows):
            if r["validAt"] <= season_start.isoformat():
                idx = i
                current_price = r["price"]
            else:
                break

        for day in _daterange(season_start, season_end):
            # if the next change is today or earlier, advance
            while (idx + 1) < len(rows) and rows[idx + 1]["validAt"] <= day.isoformat():
                idx += 1
                current_price = rows[idx]["price"]

            # Only emit if we actually have a price as of this day
            if current_price is not None:
                dense.append({
                    "productDefinitionId": pid,
                    "valid_from": day.isoformat(),
                    "price": current_price,
                })
    return dense

# --------------- database upsert methods ----------------
# products table upsert
def upsert_pricenow_products(supabase_client, df: pd.DataFrame):
    if df.empty:
        return

    # sanity: product_id must be non-null
    if df["product_id"].isnull().any():
        missing = df[df["product_id"].isnull()]
        raise ValueError(f"Null product_id in products rows: {missing}")

    upsert_df(
        supabase_client,
        table="pricenow_products",
        df=df[["product_id", "category", "age", "duration", "updated_at"]],
        on_conflict="product_id",
        chunk_size=1000,
    )

# prices table upsert
def upsert_pricenow_prices(supabase_client, df: pd.DataFrame):
    if df.empty:
        return

    # ensure valid_from is ISO date
    if "valid_from" in df.columns:
        df["valid_from"] = pd.to_datetime(df["valid_from"]).dt.strftime("%Y-%m-%d")

    # sanity: product_id and valid_from must be non-null (primary key)
    for col in ("product_id", "valid_from"):
        if df[col].isnull().any():
            missing = df[df[col].isnull()]
            raise ValueError(f"Null {col} in prices rows: {missing}")

    upsert_df(
        supabase_client,
        table="pricenow_prices",
        df=df[["product_id", "valid_from", "price", "active", "updated_at"]],
        on_conflict="product_id,valid_from",
        chunk_size=1000,
    )


# ------------- main methods -------------
# get metadata from product_API
def make_pricenow_products_df(updated_at: datetime) -> pd.DataFrame:
    # declare output df for the table pricenow_products
    df = pd.DataFrame(columns=('product_id', 'category', 'age', 'duration', 'updated_at'))

    # declare list of product_ids; used to save prices later
    product_ids = []

    # no forced token fetch here; first API call will load cache or fetch as needed
    data = get_products()
    all_products = data["data"]

    # iterate through all_products (skitickets & wintercard are separated here)
    for p in all_products:
        category = p.get('name') # skitickets or wintercard

        product_definitions = p['productDefinitions'] # contains all info for the actual specific products

        # iterate through products (age category & duration)
        for p_d in product_definitions:
            product_id = p_d.get('id') # product_id is taken directly from p_d

            attributes = p_d['attributes']
            age_dict = attributes.get('age') # age attribute is a dictionary
            age = age_dict.get('value') # which contains the actual age category (adult, child or small_child) in value

            duration_dict = attributes.get('duration') # duration attribute is a dictionary
            duration = duration_dict.get('value') # which contains the actual duration (4h, 1d, 2d, ..., 13d) in value

            if duration == '4h':
                duration_int = 1
            else:
                duration_int = int(str.replace(duration, 'd', '')) # remove d, coerce to integer

            duration_map[product_id] = duration_int # save duration to dictionary with product_id as key, to later use with price_df

            if age != 'small_child': # prevent small_child values from being saved; we don't sell those tickets
                df.loc[len(df)] = [product_id, category, age, duration, updated_at] # save results to output df
                product_ids.append(product_id)  # also save product_id to list of product_ids -> call later to get prices

    return df

# get live price data from pricing API
def make_pricenow_prices_df(product_ids: list, updated_at: datetime) -> pd.DataFrame:
    # define season dates
    season_start = date(2025, 12, 13)
    season_end   = date(2026, 4, 12)

    # fetch all change points across the season for all products (paginated)
    change_rows = get_prices_all(product_ids, season_start.isoformat(), season_end.isoformat(), page_size=1000)

    # forward-fill into a dense daily grid
    dense_rows = forward_fill_daily_grid(change_rows, season_start, season_end)

    # build dataframe
    # columns: ('product_id', 'valid_from', 'price', 'active', 'updated_at')
    records = []
    for r in dense_rows:
        pid = r["productDefinitionId"]
        valid_from = r["valid_from"]            # 'YYYY-MM-DD'
        price = r["price"]                      # integer minor units

        # calculate active value
        duration = duration_map.get(pid)  # get duration for ticket product_id
        valid_from_date_obj = datetime.strptime(valid_from, "%Y-%m-%d").date() # make valid_from into a date object for calculating active column

        # set days left in season manually for pre-season dates
        if valid_from_date_obj == date(2025, 12, 13):
            days_between = 2
        elif valid_from_date_obj == date(2025, 12, 14):
            days_between = 1
        elif date(2025, 12, 14) < valid_from_date_obj < date(2025, 12, 19):
            days_between = 0 # resort is closed in the week
        # calculate it for all other dates
        else:
            days_between = (season_end - valid_from_date_obj).days + 1 # subtract start date from season end, + 1 for comparison to duration

        if days_between >= duration:
            active = True

        else:
            active = False

        records.append((pid, valid_from, price, active, updated_at))

    df = pd.DataFrame.from_records(
        records,
        columns=('product_id', 'valid_from', 'price', 'active', 'updated_at')
    )

    return df


# -----------------------------
# CONFIG
# -----------------------------
#ENV = "staging"  # uncomment for staging
ENV = "prod" # comment if staging

if ENV == "staging":
    AUTH_URL = "https://pricenow-staging.eu.auth0.com/oauth/token"
    API_BASE = "https://api.test.pricenow.dev"
    MAIN_API_VERSION = "2024-01-01"
else:  # prod
    AUTH_URL = "https://auth.pricenow.ch/oauth/token"
    API_BASE = "https://api.pricenow.dev"
    MAIN_API_VERSION = "2024-01-01"

# get env variables
load_dotenv()
PRICENOW_CLIENT_ID = os.getenv("PRICENOW_CLIENT_ID")
PRICENOW_CLIENT_SECRET = os.getenv("PRICENOW_CLIENT_SECRET")
AUDIENCE = os.getenv("AUDIENCE")
GRANT_TYPE = os.getenv("GRANT_TYPE")
AUTH_VERSION_HEADER = os.getenv("AUTH_VERSION_HEADER")
PRICING_API_VERSION = os.getenv("PRICING_API_VERSION")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not PRICENOW_CLIENT_SECRET or not PRICENOW_CLIENT_SECRET:
    raise RuntimeError("Missing Pricenow credentials")

if not AUDIENCE or not GRANT_TYPE or not AUTH_VERSION_HEADER or not PRICING_API_VERSION:
    raise RuntimeError("Missing Pricenow headers")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase credentials")

# -----------------------------
# TOKEN CACHE (memory + disk)
# -----------------------------
CACHE_FILE = Path(".pricenow_token_cache.json")
_token = None
_token_expires_at = 0  # epoch seconds

# -----------------------------
# MAIN CODE
# -----------------------------
duration_map = {}

if __name__ == "__main__":
    print(f"Environment: {ENV}")
    print(f"Auth URL:    {AUTH_URL}")
    print(f"API Base:    {API_BASE}")

    # get timestamp info - both tables use it (saved in UTC, match local time at output)
    updated_at = datetime.now(timezone.utc).isoformat()

    # make table for pricenow products
    pricenow_products_df = make_pricenow_products_df(updated_at)
    print("Pulled products data")

    # get product_ids from pricenow_products_df to pass to pricenow_prices method
    product_ids = pricenow_products_df['product_id'].tolist()

    # make table for pricenow_prices
    pricenow_prices_df = make_pricenow_prices_df(product_ids, updated_at)
    print("Pulled pricing data")

    # connect to supabase LeukerbaDB project
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    upsert_pricenow_prices(supabase, pricenow_prices_df)
    print("Updated prices table")

    # upsert both tables
    upsert_pricenow_products(supabase, pricenow_products_df)
    print("Updated products table")


