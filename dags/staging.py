import os
import csv
from airflow.providers.mysql.hooks.mysql import MySqlHook

DATA_DIR = "/opt/airflow/data"

REQUIRED_RAW_FILES = [
    "customers_data.csv",
    "orders_data.csv",
    "order_items_data.csv",
]

def branch_on_raw_files(**context) -> str:
    missing = []
    for filename in REQUIRED_RAW_FILES:
        path = os.path.join(DATA_DIR, filename)
        if not os.path.exists(path):
            missing.append(path)

    if missing:
        print(f"Missing required raw files: {', '.join(missing)}")
        print("Branching to 'stop_etl' to avoid partial load.")
        return "stop_etl"

    print("All required raw files present. Proceeding with ETL branch.")
    return "proceed_etl"

def truncate_and_insert(rows, insert_sql, table_name, mysql_conn_id="mysql_ecommerce"):
    hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(f"TRUNCATE TABLE {table_name};")
        if rows:
            cursor.executemany(insert_sql, rows)
        conn.commit()
        print(f"[{table_name}] Inserted {len(rows)} rows.")
    finally:
        cursor.close()
        conn.close()

def load_stg_customers(mysql_conn_id: str = "mysql_ecommerce"):
    file_path = os.path.join(DATA_DIR, "customers_data.csv")
    rows = []

    with open(file_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            zip_prefix = row["customer_zip_code_prefix"]
            zip_prefix = int(zip_prefix) if zip_prefix not in (None, "", "NULL") else None

            rows.append((
                row["customer_id"],
                row["customer_unique_id"],
                zip_prefix,
                row["customer_city"],
                row["customer_state"]
                ))

    insert_sql = """
        INSERT INTO stg_customers
        (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
        VALUES (%s, %s, %s, %s, %s)
    """
    truncate_and_insert(rows, insert_sql, "stg_customers", mysql_conn_id)
    
def load_stg_orders(mysql_conn_id: str = "mysql_ecommerce"):
    file_path = os.path.join(DATA_DIR, "orders_data.csv")
    rows = []

    with open(file_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append((
                row["order_id"],
                row["customer_id"],
                row["order_status"],
                row["order_purchase_timestamp"],
                row["order_approved_at"] or None,
                row["order_delivered_carrier_date"] or None,
                row["order_delivered_customer_date"] or None,
                row["order_estimated_delivery_date"] or None
                ))

    insert_sql = """
        INSERT INTO stg_orders
        (order_id,customer_id,order_status,order_purchase_timestamp,order_approved_at,order_delivered_carrier_date,order_delivered_customer_date,order_estimated_delivery_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    truncate_and_insert(rows, insert_sql, "stg_orders", mysql_conn_id)


def load_stg_order_items(mysql_conn_id: str = "mysql_ecommerce"):
    file_path = os.path.join(DATA_DIR, "order_items_data.csv")
    rows = []
    
    with open(file_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append((
                row["order_id"],
                int(row["order_item_id"]),
                row["product_id"],
                row["seller_id"],
                row["shipping_limit_date"],
                float(row["price"]),
                float(row["freight_value"])
                ))

    insert_sql = """
        INSERT INTO stg_order_items
        (order_id,order_item_id,product_id,seller_id,shipping_limit_date,price,freight_value)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    truncate_and_insert(rows, insert_sql, "stg_order_items", mysql_conn_id)