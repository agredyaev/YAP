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

from pyspark.sql.window import Window 
import pyspark.sql.functions as F

window = Window().partitionBy('event.message_to').orderBy('date')
dfWithLag = events.withColumn("lag_7",F.lag('event.message_to', 7).over(window))

dfWithLag.select('event.message_from', 'date', "lag_7") \
    .filter(dfWithLag.lag_7.isNotNull()) \
    .orderBy(F.col('date').desc()) \
    .show(10, False)


data = [('2021-01-06', 3744, 63, 322),
        ('2021-01-04', 2434, 21, 382),
        ('2021-01-04', 2434, 32, 159),
        ('2021-01-05', 3744, 32, 159),
        ('2021-01-06', 4342, 32, 159),
        ('2021-01-05', 4342, 12, 259),
        ('2021-01-06', 5677, 12, 259),
        ('2021-01-04', 5677, 23, 499)
]

columns = ['dt', 'user_id', 'product_id', 'purchase_amount']

df = spark.createDataFrame(data=data, schema=columns)


window = Window().partitionBy('user_id')

df_window_agg= df.withColumn("max",F.max("purchase_amount").over(window)) \
                                 .withColumn("min",F.min("purchase_amount").over(window))

df_window_agg.select('user_id', 'max', 'min').show()