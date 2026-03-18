"""
KICKZ EMPIRE — Transform (Silver Layer)
=========================================
TP1 — Step 2: Clean and conform Bronze data → Silver.

This module reads tables from the Bronze schema, applies cleaning
transformations, and loads the results into the Silver schema.

Transformations applied:
    - Remove internal columns (prefixed with `_`)
    - Normalize data types
    - Remove PII (Personally Identifiable Information)
    - Validate values (statuses, amounts, etc.)

Silver tables created:
    1. silver.dim_products   ← bronze.products (cleaned)
    2. silver.dim_users      ← bronze.users (PII removed)
    3. silver.fct_orders     ← bronze.orders (conformed)
    4. silver.fct_order_lines ← bronze.order_line_items (conformed)
"""

import pandas as pd
from sqlalchemy import text

from src.database import get_engine, BRONZE_SCHEMA, SILVER_SCHEMA


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _read_bronze(table_name: str) -> pd.DataFrame:
    """
    Read a table from the Bronze schema via SQL.

    Args:
        table_name (str): Bronze table name (e.g. "products").

    Returns:
        pd.DataFrame: The Bronze table contents.

    Hint: use pd.read_sql() with a SELECT * query
    Docs: https://pandas.pydata.org/docs/reference/api/pandas.read_sql.html
    """
    engine = get_engine()
    query = f"SELECT * FROM {BRONZE_SCHEMA}.{table_name}"
    return pd.read_sql(query, engine)


def _drop_internal_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop all columns whose name starts with '_'.
    These columns are internal data that should not be exposed.

    Args:
        df (pd.DataFrame): The source DataFrame.

    Returns:
        pd.DataFrame: The DataFrame without internal columns.

    Example:
        Columns before: ['product_id', 'brand', '_internal_cost_usd', '_supplier_id']
        Columns after:  ['product_id', 'brand']
    """
    internal_columns = [column for column in df.columns if column.startswith("_")]
    cleaned_df = df.drop(columns=internal_columns)
    print(f"    🧹 Internal columns removed: {len(internal_columns)}")
    return cleaned_df


def _load_to_silver(df: pd.DataFrame, table_name: str, if_exists: str = "replace"):
    """
    Load a DataFrame into a Silver schema table.

    Args:
        df (pd.DataFrame): The cleaned data.
        table_name (str): Target table name (without the schema).
        if_exists (str): "replace" or "append"
    """
    engine = get_engine()
    df.to_sql(
        name=table_name,
        con=engine,
        schema=SILVER_SCHEMA,
        if_exists=if_exists,
        index=False,
    )
    print(f"    ✅ {SILVER_SCHEMA}.{table_name} — {len(df)} rows loaded")


# ---------------------------------------------------------------------------
# Transformations per table
# ---------------------------------------------------------------------------
def transform_products() -> pd.DataFrame:
    """
    Transform bronze.products → silver.dim_products.

    Transformations:
        1. Drop internal columns (_internal_cost_usd, _supplier_id, etc.)
        2. Parse `available_sizes_json`: it's a JSON string inside the CSV
           → keep as-is for now (or validate)
        3. Normalize `tags`: replace the '|' separator with ','
        4. Ensure `price_usd` is a valid float (no negatives)
        5. Convert `is_active` and `is_hype_product` to booleans

    Returns:
        pd.DataFrame: The cleaned catalog.
    """
    print("  📦 Transform: products → dim_products")
    df = _read_bronze("products")

    df = _drop_internal_columns(df)

    if "tags" in df.columns:
        df["tags"] = df["tags"].fillna("").astype(str).str.replace("|", ",", regex=False)

    if "price_usd" in df.columns:
        df["price_usd"] = pd.to_numeric(df["price_usd"], errors="coerce")
        invalid_count = int((df["price_usd"] <= 0).sum())
        if invalid_count:
            print(f"    ⚠️ Invalid prices removed: {invalid_count}")
        df = df[df["price_usd"] > 0].copy()

    for column in ["is_active", "is_hype_product"]:
        if column in df.columns:
            df[column] = (
                df[column]
                .astype(str)
                .str.strip()
                .str.lower()
                .map({"true": True, "false": False, "1": True, "0": False, "yes": True, "no": False})
                .fillna(False)
                .astype(bool)
            )

    _load_to_silver(df, "dim_products")

    return df


def transform_users() -> pd.DataFrame:
    """
    Transform bronze.users → silver.dim_users.

    Transformations:
        1. Drop internal columns (_hashed_password, _ga_client_id,
           _fbp, _device_fingerprint, _last_ip, _failed_login_count,
           _account_flags, _internal_segment_id)
        2. Replace NULL loyalty_tier with 'none' (unclassified)
        3. Normalize emails to lowercase
        4. Remove/mask unnecessary PII (phone → keep only the country)

    ⚠️  Warning about sensitive data: NEVER expose passwords,
        IPs, or fingerprints in the Silver layer.

    Returns:
        pd.DataFrame: The cleaned users (without sensitive PII).
    """
    print("  👤 Transform: users → dim_users")
    df = _read_bronze("users")

    df = _drop_internal_columns(df)

    if "loyalty_tier" in df.columns:
        df["loyalty_tier"] = df["loyalty_tier"].fillna("none")

    if "email" in df.columns:
        df["email"] = df["email"].fillna("").astype(str).str.strip().str.lower()

    _load_to_silver(df, "dim_users")

    return df


def transform_orders() -> pd.DataFrame:
    """
    Transform bronze.orders → silver.fct_orders.

    Transformations:
        1. Drop internal columns (_stripe_*, _paypal_*, _fraud_score, etc.)
        2. Validate the `status` field (must be in the allowed list)
           Valid statuses: delivered, shipped, processing, returned, cancelled, chargeback
        3. Convert `order_date` to datetime
        4. Verify that total_usd = subtotal_usd - discount_amount_usd + shipping_cost_usd + tax_usd
           (tolerance of 0.01 for rounding)
        5. Replace NULL coupon_code with '' (empty string)

    Returns:
        pd.DataFrame: The cleaned orders.
    """
    print("  🛍️ Transform: orders → fct_orders")
    df = _read_bronze("orders")

    df = _drop_internal_columns(df)

    if "status" in df.columns:
        valid_statuses = {"delivered", "shipped", "processing", "returned", "cancelled", "chargeback"}
        status_series = df["status"].fillna("").astype(str).str.strip().str.lower()
        invalid_count = int((~status_series.isin(valid_statuses)).sum())
        if invalid_count:
            print(f"    ⚠️ Invalid statuses removed: {invalid_count}")
        df = df[status_series.isin(valid_statuses)].copy()
        df["status"] = status_series[status_series.isin(valid_statuses)]

    if "order_date" in df.columns:
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

    if "coupon_code" in df.columns:
        df["coupon_code"] = df["coupon_code"].fillna("")

    _load_to_silver(df, "fct_orders")

    return df


def transform_order_line_items() -> pd.DataFrame:
    """
    Transform bronze.order_line_items → silver.fct_order_lines.

    Transformations:
        1. Drop internal columns (_warehouse_id, _internal_batch_code, _pick_slot)
        2. Verify that line_total_usd ≈ unit_price_usd * quantity
        3. Ensure quantity > 0
        4. Check referential integrity: all order_id values must exist in fct_orders

    Returns:
        pd.DataFrame: The cleaned order line items.
    """
    print("  📋 Transform: order_line_items → fct_order_lines")
    df = _read_bronze("order_line_items")

    df = _drop_internal_columns(df)

    if "quantity" in df.columns:
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
        invalid_qty_count = int((df["quantity"] <= 0).sum())
        if invalid_qty_count:
            print(f"    ⚠️ Invalid quantities removed: {invalid_qty_count}")
        df = df[df["quantity"] > 0].copy()

    for column in ["unit_price_usd", "line_total_usd"]:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    if {"line_total_usd", "unit_price_usd", "quantity"}.issubset(df.columns):
        expected_total = df["unit_price_usd"] * df["quantity"]
        discrepancy_count = int((df["line_total_usd"] - expected_total).abs().gt(0.01).sum())
        print(f"    🔎 Line total discrepancies (>0.01): {discrepancy_count}")

    _load_to_silver(df, "fct_order_lines")

    return df


# ---------------------------------------------------------------------------
# Main function
# ---------------------------------------------------------------------------
def transform_all() -> dict[str, pd.DataFrame]:
    """
    Run the full Bronze → Silver transformation.

    Returns:
        dict: {table_name: DataFrame} for each transformed table.
    """
    print(f"\n{'='*60}")
    print(f"  🥈 TRANSFORM → Silver ({SILVER_SCHEMA})")
    print(f"{'='*60}\n")

    results = {}

    results["dim_products"] = transform_products()
    results["dim_users"] = transform_users()
    results["fct_orders"] = transform_orders()
    results["fct_order_lines"] = transform_order_line_items()

    print(f"\n  ✅ Transformation complete — {len(results)} tables in {SILVER_SCHEMA}")
    return results


# ---------------------------------------------------------------------------
# Entry point for testing the transformation standalone
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    transform_all()
