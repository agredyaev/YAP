import findspark
findspark.init()
findspark.find()
import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf' 


from pyspark.sql import SparkSession


spark = SparkSession.builder.config("spark.executor.memory", "4g").config("spark.driver.memory", "1g").config("spark.executor.cores", 4).config("spark.driver.cores", 2).appName("agrediaev").getOrCreate()


events = spark.read.json("/user/master/data/events").cache()

events.printSchema()

ods_dir = "/user/agredyaev/data/events"

events.write.option("header",True) \
        .partitionBy("date", "event_type") \
        .mode("overwrite") \
        .parquet(ods_dir)


events1 = spark.read.parquet(ods_dir)

import pyspark.sql.functions as F

events1.orderBy(F.col('event.datetime').desc()).show(10)