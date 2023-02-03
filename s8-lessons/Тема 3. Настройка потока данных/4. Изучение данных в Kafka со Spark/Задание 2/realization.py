from pyspark.sql import SparkSession
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

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
    "subscribe": 'persist_topic'
}


def spark_init() -> SparkSession:
    return SparkSession.builder \
        .appName("read kafka topic") \
        .config("spark.jars.packages", spark_jars_packages) \
        .getOrCreate()


def load_df(spark: SparkSession) -> DataFrame:
    return spark.read \
        .format('kafka') \
        .options(**config) \
        .load()


def transform(df: DataFrame) -> DataFrame:
    
    schema = StructType([
        StructField("subscription_id", IntegerType()),
        StructField("name", StringType()),
        StructField("description", StringType()),
        StructField("price", DoubleType()),
        StructField("currency", StringType()),
    ])

    return df \
        .withColumn('key', F.col('key').cast(StringType())) \
        .withColumn('value', F.col('value').cast(StringType())) \
        .withColumn('event', F.from_json('value', schema=schema)) \
        .select('event.*', '*') \
        .drop('event')


spark = spark_init()

source_df = load_df(spark)
df = transform(source_df)


df.printSchema()
df.show(truncate=False)
