import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from datetime import date, datetime

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
'owner': 'airflow',
'start_date':datetime(2020, 1, 1),
}

dag_spark = DAG(
dag_id = "datalake_etl",
default_args=default_args,
schedule_interval=None,
)

events_partitioned = SparkSubmitOperator(
task_id='events_partitioned',
dag=dag_spark,
application ='/home/username/partition_overwrite.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-05-31", "/user/master/data/events", "/user/username/data/events"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

verified_tags_candidates_d7 = SparkSubmitOperator(
task_id='verified_tags_candidates_d7',
dag=dag_spark,
application ='/home/username/verified_tags_candidates.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-05-31", "7", "100", "/user/username/data/events", "/user/master/data/snapshots/tags_verified/actual", "/user/username/data/analytics/verified_tags_candidates_d7"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

verified_tags_candidates_d84 = SparkSubmitOperator(
task_id='verified_tags_candidates_d84',
dag=dag_spark,
application ='/home/username/verified_tags_candidates.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-05-31", "84", "1000", "/user/username/data/events", "/user/master/data/snapshots/tags_verified/actual", "/user/username/data/analytics/verified_tags_candidates_d84"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

user_interests_d7 = SparkSubmitOperator(
task_id='user_interests_d7',
dag=dag_spark,
application ='/home/username/user_interests.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-05-31", "7", "/user/username/data/events", "/user/username/data/analytics/user_interests_d7"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

user_interests_d28 = SparkSubmitOperator(
task_id='user_interests_d28',
dag=dag_spark,
application ='/home/username/user_interests.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-05-31", "28", "/user/username/data/events", "/user/username/data/analytics/user_interests_d28"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

events_partitioned >> [verified_tags_candidates_d7, verified_tags_candidates_d84, user_interests_d7, user_interests_d28]