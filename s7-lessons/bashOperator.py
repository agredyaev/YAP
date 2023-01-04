import airflow
from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'


date = '2022-05-28'

default_args = {
                                'owner': 'airflow',
                                'start_date':datetime(2020, 1, 1),
                                }

dag_spark = DAG(
                        dag_id = "sparkoperator",
                        default_args=default_args,
                        schedule_interval=None,
                        )

# объявляем задачу с помощью SparkSubmitOperator
seven = SparkSubmitOperator(
                        task_id='seven',
                        dag=dag_spark,
                        application ='/lessons/cli_run.py' ,
                        conn_id= 'yarn_spark',
                        application_args = [date, '7', '100', 
                                            '/user/agredyaev/data/events', 
                                            '/user/master/data/snapshots/tags_verified/actual',
                                            '/user/prod/5.2.4/analytics/verified_tags_candidates_d7']

                        )

eighty_four = SparkSubmitOperator(
                        task_id='eighty_four',
                        dag=dag_spark,
                        application ='/lessons/cli_run.py' ,
                        conn_id= 'yarn_spark',
                        application_args = [date, '84', '1000', 
                                            '/user/agredyaev/data/events',
                                            '/user/master/data/snapshots/tags_verified/actual',
                                            '/user/prod/5.2.4/analytics/verified_tags_candidates_d84']

                        )

[seven, eighty_four]


