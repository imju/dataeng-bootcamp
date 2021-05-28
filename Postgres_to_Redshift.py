from airflow import DAG
from airflow.macros import *
from airflow.models import Variable
from plugins.postgres_to_s3_operator import PostgresToS3Operator
from plugins.s3_to_redshift_operator import S3ToRedshiftOperator


DAG_ID = "Postgres_to_Redshift"
dag = DAG(
    DAG_ID,
    schedule_interval="@once",
    max_active_runs=1,
    concurrency=2,
    start_date=datetime(2021, 5, 26),
    catchup=False
)

tables = [
    "customer_features",
    "customer_variants",
    "item_features",
    "customer_interactions"
]


# s3_bucket, local_dir
s3_bucket = 'grepp-data-engineering'

#
# 다음은 각자 상황에 맞게 수정
#
local_dir = Variable.get('local_dir') # 이를 꼭 본인의 airflow 서버에서 디렉토리로 만들어주어야 함. 실제 프로덕션에서는 공간이 충분한 폴더 (volume)로 맞춰준다
s3_key_prefix = 'imju_hello'  # 본인의 ID에 맞게 수정
schema = 'imju_hello'    # 본인이 사용하는 스키마에 맞게 수정


prev_task = None

for table in tables:
    s3_key=s3_key_prefix+'/'+table+'.tsv'

    postgrestos3 = PostgresToS3Operator(
        table="public."+table,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        data_dir=local_dir,
        dag=dag,
        task_id="Postgres_to_S3"+"_"+table
    )

    s3toredshift = S3ToRedshiftOperator(
        schema=schema,
        table=table,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        copy_options="delimiter '\\t' COMPUPDATE ON",
        aws_conn_id='aws_s3_default',
        task_id='S3_to_Redshift'+"_"+table,
        dag=dag
    )
    if prev_task is not None:
        prev_task >> postgrestos3 >> s3toredshift
    else:
        postgrestos3 >> s3toredshift
    prev_task = s3toredshift
