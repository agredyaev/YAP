from pyspark.sql import SparkSession, DataFrame

spark_jar_packages = ','.join([
    "org.postgresql:postgresql:42.4.0"
])

postgres_config = {
    'url': 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de',
    'driver': 'org.postgresql.Driver',
    'schema': 'public',
    'dbtable': 'marketing_companies',
    'user': 'student',
    'password': 'de-student'
}

def session_init() -> SparkSession:
    return SparkSession.builder \
        .appName("join data") \
        .config("spark.jars.packages", spark_jar_packages) \
        .getOrCreate()


def read_postgres(spark: SparkSession) -> DataFrame:
        return spark \
                .read \
                .format('jdbc') \
                .options(**postgres_config) \
                .load()


spark = session_init()
df = read_postgres(spark=spark)
df.count()
