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

conf = SparkConf() \
    .setAppName('dna') \
    .set("spark.sql.inMemoryColumnarStorage.compressed", True) \
    .set("spark.sql.inMemoryColumnarStorage.batchSize", 10000) \
    .set("spark.sql.shuffle.partitions", "100") \
    .set("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .set("spark.hadoop.yarn.timeline-service.enabled", False)

sc = SparkContext(conf=conf)
sql = SQLContext(sc)



def input_paths(date: str, depth: int) -> list:
    path = '/user/agredyaev/data/events/date={date}/event_type=message'
    dd = datetime.strptime(date, "%Y-%m-%d")

    return [path.format(date=(dd - timedelta(i)).date()) for i in range(depth)]




data = sql.read.parquet(*input_paths('2022-05-31', 7))

all_tags = data.where("event.message_channel_to is not null") \
            .selectExpr(["event.message_from as user", "explode(event.tags) as tag"]) \
            .groupBy("tag") \
            .agg(F.expr("count(distinct user) as suggested_count")) \
            .where("suggested_count >= 100")


verified_tags = sql.read.parquet("/user/master/data/snapshots/tags_verified/actual")
candidates = all_tags.join(verified_tags, "tag", "left_anti")

candidates.write.parquet('/user/agredyaev/data/analytics/candidates_d7_pyspark')