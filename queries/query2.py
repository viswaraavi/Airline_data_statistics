"""
Which Career/Airport suffered most delays?

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

carriers=set()
def carriers_generator(list1):
    if(list1[6] in carriers):
        return False
    else:
        carriers.add(list1[6])
        return True

carriers_mapped=mapped_rdd.filter(lambda list1:carriers_generator(list1))
carriers_rdd=carriers_mapped.map(lambda list1:list1[6])
carriers_list=carriers_rdd.collect()

#Calculating delay for each carrier

def calculate_delay(carrier):
    filter_19790 = mapped_rdd.filter(lambda list1: list1[6] == carrier)
    filter_mapped = filter_19790.map(lambda list1: list1[31])
    def helper(x):
        y = str(x)
        z = y[1:len(x) - 1]
        return int(z)
    filter_mapped = filter_mapped.map(lambda x: helper(x))
    total_delay = filter_mapped.reduce(lambda a, b: a + b)
    return total_delay

list_delay=[]
for element in carriers_list[1:]:
    delay=calculate_delay(element)
    list_delay.append(delay)

print list_delay



