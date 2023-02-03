from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType
from settings import TOPIC_NAME


print(TOPIC_NAME)

# необходимая библиотека с идентификатором в maven
# вы можете использовать ее с помощью метода .config и опции "spark.jars.packages"
spark_jars_packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"

# настройки security для кафки
# вы можете использовать из с помощью метода .options(**kafka_security_options)
config = {
    'kafka.bootstrap.servers': 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091',
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";',
    "subscribe": TOPIC_NAME
}


def spark_init() -> SparkSession:
    return SparkSession.builder \
        .appName("read kafka topic") \
        .config("spark.jars.packages", spark_jars_packages) \
        .getOrCreate()


def load_df(spark: SparkSession) -> DataFrame:
    return spark.readStream \
        .format('kafka') \
        .options(**config) \
        .load()


def transform(df: DataFrame) -> DataFrame:

    schema = StructType([

        StructField("lat", DoubleType()),
        StructField("timestamp", TimestampType()),
        StructField("lon", DoubleType()),
        StructField("client_id", StringType()),
    ])

    return df \
        .withColumn('key', F.col('key').cast(StringType())) \
        .withColumn('value', F.col('value').cast(StringType())) \
        .withColumn('event', F.from_json('value', schema=schema)) \
        .select('event.*', '*') \
        .withColumn('timestamp', F.from_unixtime(F.col('timestamp'), "yyyy-MM-dd' 'HH:mm:ss.SSS").cast(TimestampType())) \
        .drop('event')


spark = spark_init()

source_df = load_df(spark)
output_df = transform(source_df)

query = (output_df
            .writeStream
            .outputMode("append")
            .format("console")
            .option("truncate", False)
            .trigger(once=True)
            .start()
            )
try:
    query.awaitTermination()
finally:
    query.stop()
