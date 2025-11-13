import pandas as pd
from airflow.exceptions import AirflowException
from airflow.providers.mysql.hooks.mysql import MySqlHook

#Fetch column names of  MySQL table to keep code schema-driven.
def get_table_columns(table_name: str, mysql_conn_id: str = "mysql_ecommerce"):
    hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        cur.execute(f"SHOW COLUMNS FROM {table_name}")
        cols = [row[0] for row in cur.fetchall()]
        if not cols:
            raise AirflowException(f"Table {table_name} exists but has no columns?")
        return cols
    finally:
        cur.close()
        conn.close()

#Keep only the columns that exist in the MySQL table, in the exact table order.
def align_to_table(df: pd.DataFrame, table_name: str, mysql_conn_id: str = "mysql_ecommerce"):
    table_cols = get_table_columns(table_name, mysql_conn_id)

    #Keep only columns that exist in the target table
    intersect_cols = [c for c in table_cols if c in df.columns]
    missing_in_df = [c for c in table_cols if c not in df.columns]

    if missing_in_df:
        raise AirflowException(
            f"[{table_name}] Missing required columns in DataFrame: {missing_in_df}. "
            f"Available DF columns: {list(df.columns)}"
        )
    return df[intersect_cols].copy()

#Insert new rows, update existing ones
def upsert_table(df: pd.DataFrame, table_name: str, mysql_conn_id: str = "mysql_ecommerce"):
    if df.empty:
        print(f"[{table_name}] no data to upsert - Dataframe is empty.")
        return
    # Align DataFrame to match table schema and replace NaN with None
    df_aligned = align_to_table(df, table_name, mysql_conn_id)
    df_aligned = df_aligned.where(pd.notnull(df_aligned), None)

    hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    conn = hook.get_conn()
    cur = conn.cursor()

    cols = list(df_aligned.columns)
    col_csv = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    update_csv = ", ".join([f"{c}=VALUES({c})" for c in cols])

    sql = f"""
        INSERT INTO {table_name} ({col_csv})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {update_csv}
    """

    rows = list(df_aligned.itertuples(index=False, name=None))
    try:
        cur.executemany(sql, rows)
        conn.commit()
        print(f"[{table_name}] Upserted {len(rows)} rows.")
    finally:
        cur.close()
        conn.close()


def build_fact_orders(mysql_conn_id: str = "mysql_ecommerce"):
    hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    df_orders = hook.get_pandas_df("SELECT * FROM stg_orders")
    df_items  = hook.get_pandas_df("SELECT * FROM stg_order_items")

    df_orders["order_purchase_timestamp"] = pd.to_datetime(df_orders["order_purchase_timestamp"], errors="coerce")
    df_orders["order_delivered_customer_date"] = pd.to_datetime(df_orders["order_delivered_customer_date"], errors="coerce")

    #Aggregate order items to get count and subtotal per order
    if df_items.empty:
        df_items_agg = pd.DataFrame(columns=["order_id", "num_items", "items_subtotal"])
    else:
        df_items["price"] = pd.to_numeric(df_items["price"], errors="coerce").fillna(0.0)
        df_items_agg = (
            df_items.groupby("order_id", as_index=False)
            .agg(
                num_items=("order_item_id", "count"),
                items_subtotal=("price", "sum"),
            )
        )

    #Join aggregated data with orders
    df_fact = df_orders.merge(df_items_agg, on="order_id", how="left")
    df_fact["num_items"] = df_fact["num_items"].fillna(0).astype(int)
    df_fact["items_subtotal"] = df_fact["items_subtotal"].fillna(0.0)
    df_fact["order_revenue"] = df_fact["items_subtotal"]
    df_fact["order_date"] = df_fact["order_purchase_timestamp"].dt.date
    df_fact["delivered_date"] = df_fact["order_delivered_customer_date"].dt.date

    upsert_table(df_fact, "fact_order", mysql_conn_id)

def build_fact_order_items(mysql_conn_id="mysql_ecommerce"):
    hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    # Source items from staging
    df_items = hook.get_pandas_df(
        "SELECT order_id, order_item_id, product_id, seller_id, price FROM stg_order_items"
    )
    if df_items.empty:
        print("[fact_order_item] No items found in staging, skipping load.")
        return
    df_items["price"] = pd.to_numeric(df_items["price"], errors="coerce").fillna(0.0)

    df_orders = hook.get_pandas_df("SELECT order_id FROM fact_order")
    if df_orders.empty:
        print("[fact_order_item] ffact_order table is empty, cannot load items due to missing parent keys.")
        return

    # Keep only items whose order_id exists in fact_order
    df_joined = df_items.merge(df_orders, on="order_id", how="inner")
    dropped = len(df_items) - len(df_joined)
    if dropped > 0:
        print(f"[fact_order_item] Skipping {dropped} orphan item rows without a parent order.")

    if df_joined.empty:
        print("[fact_order_item] No valid rows left after filtering, skipping load.")
        return

    upsert_table(df_joined, "fact_order_item", mysql_conn_id)

def build_dim_customer(mysql_conn_id: str = "mysql_ecommerce"):
    hook = MySqlHook(mysql_conn_id=mysql_conn_id)

    df_cust = hook.get_pandas_df("SELECT * FROM stg_customers")
    if df_cust.empty:
        print("[dim_customer] no customers in staging, skip")
        return

    
    df_ord = hook.get_pandas_df("SELECT order_id, customer_id, order_purchase_timestamp FROM stg_orders")
    #If no orders exist, create basic customer records with 0 metrics
    if df_ord.empty:
        df_dim = df_cust.rename(columns={"customer_city": "city", "customer_state": "state"}).copy()
        df_dim["first_order_date"] = pd.NaT
        df_dim["last_order_date"] = pd.NaT
        df_dim["total_orders"] = 0
        df_dim["total_revenue"] = 0.0
        upsert_table(df_dim, "dim_customer", mysql_conn_id)
        return
    # Convert timestamps and extract order dates
    df_ord["order_purchase_timestamp"] = pd.to_datetime(df_ord["order_purchase_timestamp"], errors="coerce")
    df_ord["order_date"] = df_ord["order_purchase_timestamp"].dt.date

    # Revenue per order from items
    df_items = hook.get_pandas_df("SELECT order_id, price FROM stg_order_items")
    if df_items.empty:
        df_rev_per_order = pd.DataFrame(columns=["order_id", "order_revenue"])
    else:
        df_items["price"] = pd.to_numeric(df_items["price"], errors="coerce").fillna(0.0)
        df_rev_per_order = (df_items.groupby("order_id", as_index=False).agg(order_revenue=("price", "sum")))

    ## Merge order revenue with order info
    df_ord2 = df_ord.merge(df_rev_per_order, on="order_id", how="left")
    df_ord2["order_revenue"] = df_ord2["order_revenue"].fillna(0.0)

    # Aggregate to customer
    df_metrics = (
        df_ord2.groupby("customer_id", as_index=False)
        .agg(
            first_order_date=("order_date", "min"),
            last_order_date=("order_date", "max"),
            total_orders=("order_id", "nunique"),
            total_revenue=("order_revenue", "sum"),
        )
    )

    # Join with customer attributes
    df_dim = df_cust.merge(df_metrics, on="customer_id", how="left")
    df_dim["total_orders"] = df_dim["total_orders"].fillna(0).astype(int)
    df_dim["total_revenue"] = df_dim["total_revenue"].fillna(0.0)
    df_dim = df_dim.rename(columns={"customer_city": "city", "customer_state": "state"})
    upsert_table(df_dim, "dim_customer", mysql_conn_id)

def build_orders_mart(mysql_conn_id: str = "mysql_ecommerce"):
    hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    df_fact = hook.get_pandas_df("SELECT * FROM fact_order")
    if df_fact.empty:
        print("fact_order is empty, cannot build final_orders. Skipping.")
        return
    df_dim = hook.get_pandas_df("SELECT * FROM dim_customer")

    df_mart = df_fact.merge(df_dim, on="customer_id", how="left")
    df_mart = df_mart.rename(columns={"city": "customer_city", "state": "customer_state"})

    upsert_table(df_mart, "final_orders", mysql_conn_id)


def validate_orders_mart(mysql_conn_id: str = "mysql_ecommerce"):
    hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    df = hook.get_pandas_df("SELECT order_id, customer_id, num_items, order_revenue FROM final_orders")

    if df.empty:
        raise AirflowException("Data quality failed: final_orders is empty.")
    if df["order_id"].isna().any():
        raise AirflowException("Data quality failed: NULL order_id found.")
    if df["customer_id"].isna().any():
        raise AirflowException("Data quality failed: NULL customer_id found.")
    if (df["num_items"] < 0).any():
        raise AirflowException("Data quality failed: negative num_items found.")
    if (df["order_revenue"] < 0).any():
        raise AirflowException("Data quality failed: negative order_revenue found.")

    print("Data quality passed: final_orders table is consistent and valid.")
