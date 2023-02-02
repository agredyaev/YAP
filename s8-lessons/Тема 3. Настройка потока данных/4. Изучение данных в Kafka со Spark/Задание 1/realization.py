from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, explode, split, count, col
from pyspark.sql.types import StructType, StructField, StringType


spark_jars_packages = ",".join(
        [
            'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0'
        ]
    )


spark = SparkSession.builder \
    .appName("read kafka topic") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()


config = {
    'kafka.bootstrap.servers': 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091',
    # 'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";',
    "subscribe": 'persist_topic'
}


df = spark.read \
    .format('kafka') \
    .options(**config) \
    .load()

print(df.printSchema())