from datetime import datetime
from time import sleep

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType

TOPIC_NAME_91 = 'student.topic.cohort5.agredyaev.out'
TOPIC_NAME_IN = 'student.topic.cohort5.agredyaev'


spark_jars_packages = ",".join(
    [
        "org.postgresql:postgresql:42.4.0",
        'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0'
    ]
)

postgres_config = {
    'url': 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de',
    'driver': 'org.postgresql.Driver',
    'schema': 'public',
    'dbtable': 'marketing_companies',
    'user': 'student',
    'password': 'de-student'
}

kafka_config = {
    'kafka.bootstrap.servers': 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091',
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";'
}


def get_distance(lat1, lat2, lon1, lon2):
    """Calculates distance between two points coordinates

    Args:
        lat1 (DoubleType): latitude of the first point
        lat2 (DoubleType): latitude of the second point
        lon1 (DoubleType): longitude of the first point
        lon2 (DoubleType): longitude of the second point

    Returns:
        DoubleType: distance
    """
    R = 6371

    def __func(a, b):
        return F.pow(F.sin(a - b) / F.lit(2), F.lit(2))

    return F.lit(2) * R * F.asin(F.sqrt(__func(lat1, lat2) + F.cos(lat1) * F.cos(lat2) * __func(lon1, lon2)))


def spark_init(test_name: str) -> SparkSession:
    return SparkSession.builder \
        .appName(test_name) \
        .config("spark.jars.packages", spark_jars_packages) \
        .getOrCreate()


def read_marketing(spark: SparkSession) -> DataFrame:
    return spark.read \
        .format('jdbc') \
        .options(**postgres_config) \
        .load()


def read_client_stream(spark: SparkSession) -> DataFrame:
    return spark.readStream \
        .format('kafka') \
        .options(**kafka_config) \
        .option("subscribe", TOPIC_NAME_IN) \
        .load()


def transform_client_stream_df(df: DataFrame) -> DataFrame:

    schema = StructType([

        StructField("lat", DoubleType()),
        StructField("timestamp", DoubleType()),
        StructField("lon", DoubleType()),
        StructField("client_id", StringType())
    ])

    return df \
        .withColumn('key', F.col('key').cast(StringType())) \
        .withColumn('value', F.col('value').cast(StringType())) \
        .withColumn('event', F.from_json('value', schema)) \
        .select('event.*', 'offset') \
        .drop_duplicates(['client_id', 'timestamp']) \
        .withColumn('timestamp', F.from_unixtime(F.col('timestamp'), "yyyy-MM-dd' 'HH:mm:ss.SSS").cast(TimestampType())) \
        .withWatermark('timestamp', '10 minutes')


def add_prefix(prefix: str, df: DataFrame) -> DataFrame:
    return df.select(
        [F.col(name).alias(f'{prefix}_{name}') for name in df.columns]
    )


def convert_coordinates(df: DataFrame) -> DataFrame:

    for col in ('lat', 'adv_campaign_point_lat', 'lon', 'adv_campaign_point_lon'):
        df = df.withColumn(col, F.radians(F.col(col)))

    return df


def join(user_df: DataFrame, marketing_df: DataFrame) -> DataFrame:

    marketing_df = add_prefix('adv_campaign', marketing_df)
    crossed = user_df.crossJoin(F.broadcast(marketing_df))
    crossed = convert_coordinates(crossed)

    return crossed \
        .withColumn('distance', get_distance(F.col('lat'), F.col('adv_campaign_point_lat'), F.col('lon'), F.col('adv_campaign_point_lon'))) \
        .where((F.col('distance') <= F.col('adv_campaign_radius'))) \
        .drop_duplicates(['client_id', 'adv_campaign_id']) \
        .withColumn('value', F.to_json(F.struct(
            F.col("client_id"),
            F.col("distance"),
            F.col("adv_campaign_id"),
            F.col("adv_campaign_name"),
            F.col("adv_campaign_description"),
            F.col("adv_campaign_start_time"),
            F.col("adv_campaign_end_time"),
            F.col("adv_campaign_point_lat"),
            F.col("adv_campaign_point_lon"),
            F.current_timestamp().alias("created_at")
        ))) \
        .select('value')


def run_query(df: DataFrame):
    return df \
            .writeStream \
            .outputMode("append") \
            .format("kafka") \
            .options(**kafka_config) \
            .option("topic", TOPIC_NAME_91) \
            .option("checkpointLocation", "test_query") \
            .trigger(processingTime="30 seconds") \
            .start()


if __name__ == "__main__":
    spark = spark_init('join stream')
    client_stream = read_client_stream(spark)
    client_stream = transform_client_stream_df(client_stream)
    client_stream.printSchema()
    marketing_df = read_marketing(spark)
    marketing_df.printSchema()
    output = join(client_stream, marketing_df)
    output.printSchema()
    query = run_query(output)

    while query.isActive:
        print(
            f"query information: runId={query.runId}, "
            f"status is {query.status}, "
            f"recent progress={query.recentProgress}")
        sleep(30)

    query.awaitTermination()
