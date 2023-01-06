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





default_args = {
                                'owner': 'airflow',
                                'start_date':datetime(2020, 1, 1)
   
                                }


base_config = {
    "task_id":"load_2022-06-02",
    "conn_id":"yarn_spark",
    "application": "/lessons/job.py",
    "application_args": ['2022-06-02', 2, 1000, '/user/master/data/events', '/user/agredyaev/data/events'],
#     "executor-memory":"10G",
#     "driver-memory":"10G",
#     "executor-cores":2
    #"principal":"test-host@test",
    #"keytab":"/home/test-host.keytab",
    #"env_vars":{"SPARK_MAJOR_VERSION":2}
    }

spark_config = {
    "spark.master": "yarn",
    "spark.submit.deployMode": "client"
#     ,
#     "spark.yarn.queue":"test",
#     "spark.dynamicAllocation.minExecutors":5,
#     "spark.dynamicAllocation.maxExecutors":10, 
#     "spark.yarn.driver.memoryOverhead":5120,
#     "spark.driver.maxResultSize":"2G",a
#     "spark.yarn.executor.memoryOverhead":5120,
#     "spark.kryoserializer.buffer.max":"1000m",
#     "spark.executor.extraJavaOptions":"-XX:+UseG1GC",
#     "spark.network.timeout":"15000s",
#     "spark.executor.heartbeatInterval":"1500s",
#     "spark.task.maxDirectResultSize":"8G",
#     "spark.ui.view.acls":"*"
}

with DAG(
                        dag_id = "tags_update",
                        default_args=default_args,
                        schedule_interval=None,
                        ) as dag:


    spark_submit_local = SparkSubmitOperator(**base_config,conf=spark_config)

    spark_submit_local 