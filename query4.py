"""
Differences in delay while flying into an airport and flying out?
"""

import os
import sys

os.environ['SPARK_HOME'] = "/home/viswa/Downloads/spark-1.6.2-bin-hadoop2.6"
sys.path.append("/home/viswa/Downloads/spark-1.6.2-bin-hadoop2.6/python")
sys.path.append("/home/viswa/Downloads/spark-1.6.2-bin-hadoop2.6/python/lib/py4j-0.9-src.zip")
try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    from pyspark.sql import SQLContext
except ImportError as e:
    print ("error importing spark modules", e)
    sys.exit(1)

def helper(x):
    if(x== u'"ARR_DELAY"' or x==u'"DEP_DELAY"'):
        return 0
    y=str(x)
    z=y[1:len(x)-1]
    return int(z)

conf = SparkConf().setAppName("DIC FINAL PROJECT")
sc = SparkContext(conf=conf)
rdd=sc.textFile("/home/viswa/Music/2015.csv")
mapped_rdd=rdd.map(lambda line:line.split(","))
diff_delay=mapped_rdd.map(lambda list1:helper(list1[42])-helper(list1[31]))
total_diff_delay=diff_delay.reduce(lambda a,b:a+b)
print total_diff_delay