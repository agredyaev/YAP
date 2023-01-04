import findspark
findspark.init()
findspark.find()
import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf' 


from pyspark.sql import SparkSession


spark = SparkSession.builder.config("spark.executor.memory", "1g").config("spark.driver.memory", "1g").config("spark.executor.cores", 2).config("spark.driver.cores", 2).appName("agrediaev").getOrCreate()


events = spark.read.json("/user/master/data/events/").cache()
events.show(10)

import pyspark.sql.functions as F
events.withColumn('hour', F.hour(F.col('event.datetime'))).withColumn('minute', F.minute(F.col('event.datetime'))).withColumn('second', F.second(F.col('event.datetime'))).orderBy(F.col('event.datetime').desc()).show(10)


events.filter(F.col('event.message_to').isNotNull()).count()

events.count() - events.na.drop(subset='event.message_from').count()

events.filter(F.col('event_type')=='message').groupBy(F.col('event.message_from')).count().show()

event_from=events.filter(F.col('event_type')=='message').groupBy(F.col('event.message_from')).count()
event_from.select(F.max('count') ).show()


event_from=events.filter(F.col('event_type')=='reaction').groupBy(F.col('date')).count()
event_from.select(F.max('count').alias('max_count')).show()