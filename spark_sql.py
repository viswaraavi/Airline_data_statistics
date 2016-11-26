import os
import sys
import time
from datetime import date


os.environ['SPARK_HOME'] = "/root/spark"
sys.path.append("/root/spark/python")
sys.path.append("/root/spark/python/lib/py4j-0.9-src.zip")
try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    from pyspark.sql import SQLContext
    from pyspark.sql import SparkSession
except ImportError as e:
    print ("error importing spark modules", e)
    sys.exit(1)



spark = SparkSession.builder \
    .appName("DIC FINAL PROJECT") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df = spark.read.csv('s3n://csc591-dic-airline-data/dateset_all.csv/*', header=True)

df.show()
