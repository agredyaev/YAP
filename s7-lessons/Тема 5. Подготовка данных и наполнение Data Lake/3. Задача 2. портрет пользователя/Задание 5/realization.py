from datetime import datetime, timedelta
import findspark

findspark.init()
findspark.find()

import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr'
os.environ['SPARK_HOME'] = '/usr/lib/spark'
os.environ['PYTHONPATH'] = '/usr/local/lib/python3.8'

import sys
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pyspark.sql.functions as F


def input_paths(date: str, depth: str, base_link: str) -> list:
    path = '{base_link}/date={date}'
    dd = datetime.strptime(date, "%Y-%m-%d")

    return [path.format(
        base_link = base_link,
        date=(dd - timedelta(i)).date()
        ) for i in range(int(depth))]


def calculate_user_interests(date: str, depth: str, events_base_path: str, output_base_path: str, spark) -> None:

    message_paths = input_paths(date, depth, events_base_path)
    tag_tops = spark.read \
        .option("basePath", events_base_path) \
        .parquet(*message_paths) \
        .where("event_type = 'message'") \
        .where("event.message_channel_to is not null") \
        .select(F.col("event.message_id").alias("message_id"),
                F.col("event.message_from").alias("user_id"),
                F.explode(F.col("event.tags")).alias("tag")) \
        .groupBy("user_id", "tag") \
        .agg(F.count("*").alias("tag_count")) \
        .withColumn("rank", F.row_number().over(Window.partitionBy("user_id") \
                                                .orderBy(F.desc("tag"), F.desc("tag_count")))) \
        .where("rank <= 3") \
        .groupBy("user_id") \
        .pivot("rank", [1, 2, 3]) \
        .agg(F.first("tag")) \
        .withColumnRenamed("1", "tag_top_1") \
        .withColumnRenamed("2", "tag_top_2") \
        .withColumnRenamed("3", "tag_top_3")

    reaction_paths = input_paths(date, depth, events_base_path)
    reactions = spark.read \
        .option("basePath", events_base_path) \
        .parquet(*reaction_paths) \
        .where("event_type='reaction'")

    all_message_tags = spark.read.parquet(events_base_path) \
        .where("event_type='message' and event.message_channel_to is not null") \
        .select(F.col("event.message_id").alias("message_id"),
                F.col("event.message_from").alias("user_id"),
                F.explode(F.col("event.tags")).alias("tag")
                ).cache()

    reaction_tags = reactions \
        .select(F.col("event.reaction_from").alias("user_id"),
                F.col("event.message_id").alias("message_id"),
                F.col("event.reaction_type").alias("reaction_type")
                ).join(all_message_tags.select("message_id", "tag"), "message_id")

    reaction_tops = reaction_tags \
        .groupBy("user_id", "tag", "reaction_type") \
        .agg(F.count("*").alias("tag_count")) \
        .withColumn("rank", F.row_number().over(Window.partitionBy("user_id", "reaction_type") \
                                                .orderBy(F.desc("tag_count"), F.desc("tag")))) \
        .where("rank <= 3") \
        .groupBy("user_id", "reaction_type") \
        .pivot("rank", [1, 2, 3]) \
        .agg(F.first("tag")) \
        .cache()

    like_tops = reaction_tops \
        .where("reaction_type = 'like'") \
        .drop("reaction_type") \
        .withColumnRenamed("1", "like_tag_top_1") \
        .withColumnRenamed("2", "like_tag_top_2") \
        .withColumnRenamed("3", "like_tag_top_3")

    dislike_tops = reaction_tops \
        .where("reaction_type = 'dislike'") \
        .drop("reaction_type") \
        .withColumnRenamed("1", "dislike_tag_top_1") \
        .withColumnRenamed("2", "dislike_tag_top_2") \
        .withColumnRenamed("3", "dislike_tag_top_3")

    reaction_tag_tops = like_tops \
        .join(dislike_tops, "user_id", "full_outer")

    result = tag_tops.join(reaction_tag_tops, 'user_id', 'full_outer')

    result.write.mode("overwrite").parquet(f"{output_base_path}/date={date}")




def main():

    date=sys.argv[1],
    depth=sys.argv[2],
    events_base_path=sys.argv[3],
    output_base_path=sys.argv[4]

    conf = SparkConf() \
    .setAppName(f'VerifiedTagsCandidatesJob-{date}-d{depth}') \
    .set("spark.sql.inMemoryColumnarStorage.compressed", True) \
    .set("spark.sql.inMemoryColumnarStorage.batchSize", 10000) \
    .set("spark.sql.shuffle.partitions", "100") \
    .set("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .set("spark.hadoop.yarn.timeline-service.enabled", False)

    sc = SparkContext(conf=conf)
    spark = SQLContext(sc)



    calculate_user_interests(
        date=date,
        depth=depth,
        events_base_path=events_base_path,
        output_base_path=output_base_path,
        spark=spark 
    )


if __name__ == "__main__":
    main()