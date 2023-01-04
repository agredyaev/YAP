from pyspark.sql import SparkSession


spark = SparkSession.builder.master("yarn").appName("My second session").getOrCreate() 