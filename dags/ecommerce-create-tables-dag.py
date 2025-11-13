from datetime import datetime
from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from create_tables import create_all_tables
from staging import (
    branch_on_raw_files,
    load_stg_customers,
    load_stg_orders,
    load_stg_order_items
)
from warehouse_etl import (
    build_fact_orders,
    build_fact_order_items,
    build_dim_customer,
    build_orders_mart,
    validate_orders_mart
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="ecommerce_create_tables",
    default_args=default_args,
    start_date=datetime(2025, 7, 11),
    catchup=False,
    tags=["ecommerce", "setup"],
) as dag:
    check_required_files = BranchPythonOperator(
        task_id="check_required_files",
        python_callable=branch_on_raw_files,
    )
    
    proceed_etl = EmptyOperator(task_id="proceed_etl")
    stop_etl = EmptyOperator(task_id="stop_etl")

    create_tables = PythonOperator(
        task_id="create_all_tables",
        python_callable=create_all_tables,
    )
    load_stg_customers = PythonOperator(
        task_id="load_stg_customers",
        python_callable=load_stg_customers,
    )

    load_stg_orders = PythonOperator(
        task_id="load_stg_orders",
        python_callable=load_stg_orders,
    )

    load_stg_order_items = PythonOperator(
        task_id="load_stg_order_items",
        python_callable=load_stg_order_items,
    )

    build_fact_orders = PythonOperator(
        task_id="build_fact_orders",
        python_callable=build_fact_orders,
    )
    build_fact_order_items = PythonOperator(
        task_id="build_fact_order_items",
        python_callable=build_fact_order_items,
    )

    build_dim_customer = PythonOperator(
        task_id="build_dim_customer",
        python_callable=build_dim_customer,
    )

    build_orders_mart = PythonOperator(
        task_id="build_orders_mart",
        python_callable=build_orders_mart,
    )
    
    validate_orders_mart = PythonOperator(
        task_id="validate_orders_mart",
        python_callable=validate_orders_mart,
    )

    check_required_files >> [proceed_etl, stop_etl]
    proceed_etl >> create_tables
    create_tables >> [load_stg_customers, load_stg_orders, load_stg_order_items]
    [load_stg_customers, load_stg_orders, load_stg_order_items] >> build_dim_customer
    [load_stg_orders, load_stg_order_items, build_dim_customer] >> build_fact_orders
    [load_stg_order_items, build_fact_orders] >> build_fact_order_items
    [build_fact_orders, build_dim_customer] >> build_orders_mart
    build_orders_mart >> validate_orders_mart

