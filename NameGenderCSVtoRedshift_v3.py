from airflow import DAG
from airflow.operators import PythonOperator
from airflow.models import Variable

from datetime import datetime
from datetime import timedelta
import requests
import logging
import psycopg2

def get_Redshift_connection():
    host = "learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = "imju_hello"
    redshift_pass = "Imju_Hello!1"
    port = 5439
    dbname = "prod"
    conn = psycopg2.connect("dbname={dbname} user={user} host={host} password={password} port={port}".format(
        dbname=dbname,
        user=redshift_user,
        password=redshift_pass,
        host=host,
        port=port
    ))
    conn.set_session(autocommit=True)
    return conn.cursor()


def extract(**context):
    link = context["params"]["url"]
    task_instance = context['task_instance']
    execution_date = context['execution_date']
    logging.info("extract in")
    logging.info("link:"+link)
    logging.info(execution_date)
    f = requests.get(link)
    logging.info("extract out")
    return (f.text)


def transform(**context):
    logging.info("transform in")
    text = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")
    lines = text.split("\n")[1:]
    logging.info("transform out")
    return lines


def load(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]
    logging.info("load in")
    logging.info("schema:"+schema)
    logging.info("table:"+table)
    
    cur = get_Redshift_connection()
    lines = context["task_instance"].xcom_pull(key="return_value", task_ids="transform")
    lines = iter(lines)
    next(lines)
    sql = "BEGIN; DELETE FROM {schema}.{table};".format(schema=schema, table=table)
    for line in lines:
        if line != "":
            (name, gender) = line.split(",")
            print(name, "-", gender)
            sql += """INSERT INTO {schema}.{table} VALUES ('{name}', '{gender}');""".format(schema=schema, table=table, name=name, gender=gender)
    sql += "END;"

    logging.info(sql)
    cur.execute(sql)


dag_second_assignment = DAG(
    dag_id = 'second_assignment_v3',
    start_date = datetime(2021,5,25), # ????????? ????????? ?????? ????????? ??????
    schedule_interval = '0 2 * * *',  # ????????? ??????
    max_active_runs = 1,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)


extract = PythonOperator(
    task_id = 'extract',
    python_callable = extract,
    params = {
        'url':  Variable.get("csv_url")
    },
    provide_context=True,
    dag = dag_second_assignment)

transform = PythonOperator(
    task_id = 'transform',
    python_callable = transform,
    params = { 
    },  
    provide_context=True,
    dag = dag_second_assignment)

load = PythonOperator(
    task_id = 'load',
    python_callable = load,
    params = {
        'schema': 'imju_hello',
        'table': 'test_table_2'
    },
    provide_context=True,
    dag = dag_second_assignment)

extract >> transform >> load
