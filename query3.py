"""
Which time periods had maximum delays?

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



conf = SparkConf().setAppName("DIC FINAL PROJECT")
sc = SparkContext(conf=conf)
rdd=sc.textFile("/home/viswa/Music/2015.csv")
mapped_rdd=rdd.map(lambda line:line.split(","))

def calculate_delay(index,month):
    filter_19790 = mapped_rdd.filter(lambda list1: list1[index] == month)
    filter_mapped = filter_19790.map(lambda list1: list1[31])
    def helper(x):
        y = str(x)
        z = y[1:len(x) - 1]
        return int(z)
    filter_mapped = filter_mapped.map(lambda x: helper(x))
    total_delay = filter_mapped.reduce(lambda a, b: a + b)
    return total_delay

#Which month has more delays

months=range(1,13)
for element in months:
    calculate_delay(2,element)

#which day of week had more delays
weeks=range(1,8)
for element in weeks:
    calculate_delay(4,element)

#which year had more delays

Year=range(2000,2015)
for element in Year:
    calculate_delay(0,element)



