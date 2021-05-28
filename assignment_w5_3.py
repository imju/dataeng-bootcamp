from airflow import DAG
from airflow.operators import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
from datetime import timedelta
import requests
import logging
import psycopg2


def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()



def refresh(**context):
    summary_table = context["params"]["summary_table"]
    sql = context["params"]["ctas_sql"]
    logging.info("load in")
    logging.info("summary_table:"+summary_table)
    logging.info("sql:"+sql)    
    
    cur = get_Redshift_connection()
    cur.execute(sql)


dag_w5_third_assignment = DAG(
    dag_id = 'w5_third_assignment',
    start_date = datetime(2021,5,25), # 날짜가 미래인 경우 실행이 안됨
    schedule_interval = '0 10 * * *',  # 적당히 조절
    max_active_runs = 1,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)


refresh = PythonOperator(
    task_id = 'refresh',
    python_callable = refresh,
    params = {
        'summary_table':  Variable.get("summary_table"),
        'ctas_sql': Variable.get("ctas_sql")
    },
    provide_context=True,
    dag = dag_w5_third_assignment)


