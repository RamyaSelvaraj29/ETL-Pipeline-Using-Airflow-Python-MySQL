from airflow.providers.mysql.hooks.mysql import MySqlHook


DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS stg_customers (
        customer_id VARCHAR(50) PRIMARY KEY,
        customer_unique_id VARCHAR(50),
        customer_zip_code_prefix INT,
        customer_city VARCHAR(255),
        customer_state VARCHAR(10)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS stg_orders (
        order_id VARCHAR(50) PRIMARY KEY,
        customer_id VARCHAR(50),
        order_status VARCHAR(50),
        order_purchase_timestamp DATETIME,
        order_approved_at DATETIME NULL,
        order_delivered_carrier_date DATETIME NULL,
        order_delivered_customer_date DATETIME NULL,
        order_estimated_delivery_date DATETIME NULL,
        INDEX idx_stg_orders_customer_id (customer_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS stg_order_items (
        order_id VARCHAR(50),
        order_item_id INT,
        product_id VARCHAR(50),
        seller_id VARCHAR(50),
        shipping_limit_date DATETIME,
        price DECIMAL(10,2),
        freight_value DECIMAL(10,2),
        PRIMARY KEY (order_id, order_item_id),
        INDEX idx_stg_order_items_order_id (order_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,

    #DIM / FACT TABLES
    """
    CREATE TABLE IF NOT EXISTS dim_customer (
        customer_id VARCHAR(50) PRIMARY KEY,
        customer_unique_id VARCHAR(50),
        city VARCHAR(255),
        state VARCHAR(10),
        first_order_date DATE,
        last_order_date DATE,
        total_orders INT,
        total_revenue DECIMAL(12,2)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_order (
        order_id VARCHAR(50) PRIMARY KEY,
        customer_id VARCHAR(50),
        order_date DATE,
        order_status VARCHAR(50),
        num_items INT,
        items_subtotal DECIMAL(12,2),
        order_revenue DECIMAL(12,2),
        delivered_date DATE,
        INDEX idx_fact_order_customer_id (customer_id),
        CONSTRAINT fk_fact_order_customer
            FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id)
            ON DELETE SET NULL
            ON UPDATE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_order_item (
        order_id VARCHAR(50),
        order_item_id INT,
        product_id VARCHAR(50),
        seller_id VARCHAR(50),
        price DECIMAL(10,2),
        PRIMARY KEY (order_id, order_item_id),
        INDEX idx_fact_order_item_order_id (order_id),
        CONSTRAINT fk_fact_order_item_order
            FOREIGN KEY (order_id) REFERENCES fact_order(order_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,

    # FINAL TABLE 
    """
    CREATE TABLE IF NOT EXISTS final_orders (
        order_id VARCHAR(50) PRIMARY KEY,
        order_date DATE,
        order_status VARCHAR(50),
        num_items INT,
        items_subtotal DECIMAL(12,2),
        order_revenue DECIMAL(12,2),
        delivered_date DATE,
        customer_id VARCHAR(50),
        customer_unique_id VARCHAR(50),
        customer_city VARCHAR(255),
        customer_state VARCHAR(10),
        first_order_date DATE,
        last_order_date DATE,
        total_orders INT,
        total_revenue DECIMAL(12,2),

        INDEX idx_final_orders_customer_id (customer_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
]

def create_all_tables(mysql_conn_id: str = "mysql_ecommerce"):
    hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()

    for ddl in DDL_STATEMENTS:
        cursor.execute(ddl)

    conn.commit()
    cursor.close()
    conn.close()
