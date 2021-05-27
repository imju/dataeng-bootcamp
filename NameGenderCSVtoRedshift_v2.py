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


def extract(url):
    logging.info("Extract started")
    f = requests.get(url)
    logging.info("Extract done")
    return (f.text)


def transform(text):
    logging.info("transform started")
    # ignore the first line - header
    lines = text.split("\n")[1:]
    logging.info("transform done")
    return lines


def load(lines):
    logging.info("load started")
    cur = get_Redshift_connection()
    sql = "BEGIN;DELETE FROM raw_data.name_gender;"
    for l in lines:
        if l != '':
            (name, gender) = l.split(",")
            sql += "INSERT INTO raw_data.name_gender VALUES ('{name}', '{gender}');"
    sql += "END;"
    logging.info(sql)
    """
    Do we want to enclose try/catch here
    """
    cur.execute(sql)
    logging.info("load done")


def etl(**context):
    link = context["params"]["url"]
    # task 자체에 대한 정보 (일부는 DAG의 정보가 되기도 함)를 읽고 싶다면 context['task_instance'] 혹은 context['ti']를 통해 가능
    # https://airflow.readthedocs.io/en/latest/_api/airflow/models/taskinstance/index.html#airflow.models.TaskInstance
    task_instance = context['task_instance']
    execution_date = context['execution_date']

    logging.info(execution_date)

    data = extract(link)
    lines = transform(data)
    load(lines)


dag_second_assignment = DAG(
    dag_id = 'second_assignment_v2',
    start_date = datetime(2021,5,20), # 날짜가 미래인 경우 실행이 안됨
    schedule_interval = '0 2 * * *',  # 적당히 조절
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        'max_active_runs': 1
    }
)


task = PythonOperator(
	task_id = 'perform_etl',
	python_callable = etl,
        params = {
            'url': "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
        },
        provide_context=True,
	dag = dag_second_assignment)
