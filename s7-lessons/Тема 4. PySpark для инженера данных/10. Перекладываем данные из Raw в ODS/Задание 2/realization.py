import sys
 
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
 
def main():
        date = sys.argv[1]
        base_input_path = sys.argv[2]
        base_output_path = sys.argv[3]

        conf = SparkConf().setAppName(f"EventsPartitioningJob-{date}")
        sc = SparkContext(conf=conf)
        sql = SQLContext(sc)

 # Напишите директорию чтения в общем виде
        events = sql.read....

# Напишите директорию записи
        events\
        .write\
        ...


if __name__ == "__main__":
        main()
