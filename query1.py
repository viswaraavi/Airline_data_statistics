import os
import sys


os.environ['SPARK_HOME'] = "/root/spark"
sys.path.append("/root/spark/python")
sys.path.append("/root/spark/python/lib/py4j-0.9-src.zip")
try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    from pyspark.sql import SQLContext
except ImportError as e:
    print ("error importing spark modules", e)
    sys.exit(1)



conf = SparkConf().setAppName("DIC FINAL PROJECT")
sc = SparkContext(conf=conf)

rdd=sc.textFile("s3n://csc591-dic-airline-data/??1?.csv")
mapped_rdd=rdd.map(lambda line:line.split(","))
mapped_rdd.cache()

def delay_airport(airport_id):
    filter_19790=mapped_rdd.filter(lambda list1:list1[7]==airport_id)
    def helper(x):
        y=str(x)
        z=y[1:len(x)-1]
        return int(z)
    filter_mapped=filter_19790.map(lambda list1:helper(list1[31]))
    total_delay=filter_mapped.reduce(lambda a,b:a+b)
    return total_delay

print delay_airport(u'19790')
print delay_airport(u'19789')
delay_airport(u'19788')










