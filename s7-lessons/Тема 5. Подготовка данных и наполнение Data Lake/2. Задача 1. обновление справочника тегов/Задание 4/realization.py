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



def main():

    date = sys.argv[1]
    depth = sys.argv[2] 
    threshold = sys.argv[3]
    raw_data_base_link = sys.argv[4]
    verified_tags_link =sys.argv[5]
    output_link = sys.argv[6]

    def input_paths(date: str, depth: str) -> list:
        path = '{base_link}/date={date}/event_type=message'
        dd = datetime.strptime(date, "%Y-%m-%d")

        return [path.format(
            base_link = raw_data_base_link,
            date=(dd - timedelta(i)).date()
            ) for i in range(int(depth))
            ]

    conf = SparkConf() \
        .setAppName(f'VerifiedTagsCandidatesJob-{date}-d{depth}-cut{threshold}') \
        .set("spark.sql.inMemoryColumnarStorage.compressed", True) \
        .set("spark.sql.inMemoryColumnarStorage.batchSize", 10000) \
        .set("spark.sql.shuffle.partitions", "100") \
        .set("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .set("spark.hadoop.yarn.timeline-service.enabled", False)

    sc = SparkContext(conf=conf)
    spark = SQLContext(sc)

    data = spark.read.parquet(*input_paths(date=date, depth=depth))

    all_tags = data.where("event.message_channel_to is not null") \
                .selectExpr(["event.message_from as user", "explode(event.tags) as tag"]) \
                .groupBy("tag") \
                .agg(F.expr("count(distinct user) as suggested_count")) \
                .where(f"suggested_count >= {threshold}")


    verified_tags = spark.read.parquet(verified_tags_link)
    candidates = all_tags.join(verified_tags, "tag", "left_anti")

    candidates.write.parquet(f'{output_link}/date={date}')

if __name__ == "__main__":
    main()