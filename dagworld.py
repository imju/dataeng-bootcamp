from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime
import requests
import psycopg2

def extract(url):
    f = requests.get(link)
    return (f.text)

def transform(text):
    lines = text.split("\n")
    return lines

# Redshift connection 함수
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

def load(lines):
    # BEGIN과 END를 사용해서 SQL 결과를 트랜잭션으로 만들어주는 것이 좋음
    # BEGIN;DELETE FROM (본인의스키마).name_gender;INSERT INTO TABLE VALUES ('KEEYONG', 'MALE');....;END;
    cur = get_Redshift_connection()
    sql = "BEGIN;DELETE FROM imju_hello.test_table_1;END;" #clean up for idempotent
    cur.execute(sql)
    for i in range(1,len(lines)) :
        if lines[i] != '':
            (name, gender) = lines[i].split(",")
            print(name, "-", gender)
            sql = "BEGIN;INSERT INTO imju_hello.test_table_1 VALUES ('{n}', '{g}');END;".format(n=name, g=gender)
            print(sql)
            cur.execute(sql)

dag = DAG(
	dag_id = 'dag_world',
	start_date = datetime(2021,5,21),
	schedule_interval = '0 20 * * *')



extract_op = PythonOperator(
	task_id = 'extract_data',
	#python_callable param points to the function you want to run 
	python_callable = extract("https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"),
	#dag param points to the DAG that this task is a part of
	dag = dag)

transform_op = PythonOperator(
	task_id = 'transform_data',
	python_callable = transform,
	dag = dag)

load_op = PythonOperator(
	task_id = 'load_data',
	python_callable = load,
	dag = dag)

#Assign the order of the tasks in our DAG
extract_op >> transform_op >> load_op