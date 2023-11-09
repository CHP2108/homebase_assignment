import airflow
from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.clickhouse.transfers.postgres_to_clickhouse import PostgresToClickHouseOperator # Maybe need to custom Operator

default_args = {
    'owner': 'hoi.phan',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'postgres_to_clickhouse_trans',
    default_args=default_args,
    description='DAG transfer data from PostgreSQL to ClickHouse',
    start_date=datetime(2023, 11, 9),
    schedule_interval="0 * * * *",
    dagrun_timeout=timedelta(minutes=5), 
    is_paused_upon_creation= True,
    catchup= False,
    tags=['Postgres', 'ClickHouse'], 
) as dag:
    
    postgres_to_clickhouse_task = PostgresToClickHouseOperator(
    task_id='transfer_data',
    postgres_conn_id='postgres_conn_homebase_db',  # Specify your PostgreSQL connection ID
    sql='SELECT * FROM heart_disease',
    destination_conn_id='clickhouse_conn_homebase_db',  # Specify your ClickHouse connection ID
    destination_table='heart_disease',
    dag=dag,
)