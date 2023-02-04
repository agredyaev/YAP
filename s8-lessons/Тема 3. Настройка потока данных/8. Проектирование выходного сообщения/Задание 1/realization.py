from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType, BooleanType

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
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";',
    "subscribe": 'student.topic.cohort5.agredyaev'
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


def spark_init(test_name) -> SparkSession:
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
        df = df.withColumn(col, F.radian(F.col(col)))


def join(user_df: DataFrame, marketing_df: DataFrame) -> DataFrame:

    marketing_df = add_prefix('adv_campaign', marketing_df)
    crossed = user_df.crossJoin(F.broadcast(marketing_df))
    crossed = convert_coordinates(crossed)

    return crossed \
        .withColumn('distance', get_distance(F.col('lat'), F.col('adv_campaign_point_lat'), F.col('lon'), F.col('adv_campaign_point_lon'))) \
        .where((F.col('distance') <= F.col('adv_campaign_radius'))) \
        .withColumn("created_at", F.current_timestamp()) \
        .select(
            "client_id",  # идентификатор клиента
            "adv_campaign_id",  # идентификатор рекламной акции
            "adv_campaign_name",  # описание рекламной акции
            "adv_campaign_description",  # описание рекламной акции
            "adv_campaign_start_time",  # время начала акции
            "adv_campaign_end_time",  # время окончания акции
            "adv_campaign_point_lat",  # расположение ресторана/точки широта
            "adv_campaign_point_lon",  # расположение ресторана/долгота широта
            "created_at",  # время создания выходного ивента
            "offset",  # офсет оригинального сообщения из Kafka
        ) \
        .drop_duplicates(['client_id', 'adv_campaign_id'])


def get_output(df: DataFrame):
    return df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()


if __name__ == "__main__":
    spark = spark_init('join stream')
    client_stream = read_client_stream(spark)
    client_stream = transform_client_stream_df(client_stream)
    marketing_df = read_marketing(spark)
    result = join(client_stream, marketing_df)

    query = get_output(result)
    query.awaitTermination()
