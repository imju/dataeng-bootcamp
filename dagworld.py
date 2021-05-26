from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime
import requests
import psycopg2



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

def extract(url):
    f = requests.get(link)
    return (f.text)

def transform(text):
    lines = text.split("\n")
    return lines

def load(lines):
    text = extract("https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv")
    lines = transform(text)
    # BEGIN과 END를 사용해서 SQL 결과를 트랜잭션으로 만들어주는 것이 좋음
    # BEGIN;DELETE FROM (본인의스키마).name_gender;INSERT INTO TABLE VALUES ('KEEYONG', 'MALE');....;END;
    cur = get_Redshift_connection()
    sql = "BEGIN;DELETE FROM imju_hello.test_table_1;" #clean up for idempotent
    cur.execute(sql)
    for i in range(1,len(lines)) :
        if lines[i] != '':
            (name, gender) = lines[i].split(",")
            print(name, "-", gender)
            sql += "INSERT INTO imju_hello.test_table_1 VALUES ('{n}', '{g}');".format(n=name, g=gender)
            print(sql)
            cur.execute(sql)
    sql += 'END;'        


dag = DAG(
	dag_id = 'dag_world',
	start_date = datetime(2021,5,21),
	schedule_interval = '0 20 * * *')


etl_op = PythonOperator(
	task_id = 'load_data',
	python_callable = load,
	dag = dag)

