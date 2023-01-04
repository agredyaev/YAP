import findspark
import pyspark
from pyspark.sql import SparkSession

findspark.init()
findspark.find()

import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'


def get_spark_session(name=""):
    return SparkSession \
    .builder \
        .master("yarn")\
        .config("spark.driver.memory", "8g") \
        .config("spark.driver.cores", 8) \
        .appName(f"{name} (agrediaev)") \
        .getOrCreate()

spark = get_spark_session("Functions")


events = spark.read.json('/user/data/master/events/').cache()




import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()
events = spark.read.json("/user/master/data/events/")
events.show(10) 