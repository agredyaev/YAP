from datetime import datetime
from time import sleep

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType

TOPIC_NAME_91 = 'student.topic.cohort<номер когорты>.<username>.out'  # Это топик, в который Ваше приложение должно отправлять сообщения. Укажите здесь название Вашего топика student.topic.cohort<номер когорты>.<username>.out
TOPIC_NAME_IN = 'student.topic.cohort<номер когорты>.<username>' # Это топик, из которого Ваше приложение должно читать сообщения. Укажите здесь название Вашего топика student.topic.cohort<номер когорты>.<username>

# При первом запуске ваш топик student.topic.cohort<номер когорты>.<username>.out может не существовать в Kafka и вы можете увидеть такие сообщения:
# ERROR: Topic student.topic.cohort<номер когорты>.<username>.out error: Broker: Unknown topic or partition
# Это сообщение говорит о том, что тест начал проверять работу Вашего приложение, но так как Ваше приложение ещё не отправило туда сообщения, то топик ещё не создан. Нужно подождать несколько минут.

def spark_init(test_name) -> SparkSession:
    pass


postgresql_settings = {
    'user': 'student',
    'password': 'de-student'
}


def read_marketing(spark: SparkSession) -> DataFrame:
    pass


kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";'
}


def read_client_stream(spark: SparkSession) -> DataFrame:
    pass # В реализации этого метода нужно будет указать входной топик TOPIC_NAME_IN


def join(user_df, marketing_df) -> DataFrame:
    pass


def run_query(df):
    return (df
            .writeStream
            .outputMode("append")
            .format("kafka")
            .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
            .options(**kafka_security_options)
            .option("topic", TOPIC_NAME_91)
            .option("checkpointLocation", "test_query")
            .trigger(processingTime="15 seconds")
            .start())


if __name__ == "__main__":
    spark = spark_init('join stream')
    client_stream = read_client_stream(spark)
    marketing_df = read_marketing(spark)
    output = join(client_stream, marketing_df)
    query = run_query(output)

    while query.isActive:
        print(f"query information: runId={query.runId}, "
              f"status is {query.status}, "
              f"recent progress={query.recentProgress}")
        sleep(30)

    query.awaitTermination()
