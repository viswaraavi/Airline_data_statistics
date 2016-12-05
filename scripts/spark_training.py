
import os
import sys
import pickle

os.environ['SPARK_HOME'] = "/usr/lib/spark"
sys.path.append("/usr/lib/spark/python")
sys.path.append("/usr/lib/spark/python/lib/py4j-0.10.3-src.zip")
try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    from pyspark.sql import SQLContext
    from pyspark.mllib.tree import RandomForest, RandomForestModel
    from pyspark.mllib.util import MLUtils
    from pyspark.mllib.regression import LabeledPoint
except ImportError as e:
    print ("error importing spark modules", e)
    sys.exit(1)

def subset(x):
    if(x[0]==u'"YEAR"'):
        return None
    return [subset1(x[33]),[int(x[2]),int(x[3]),int(x[4]),helper1(x[6]),helper1(x[14]),helper1(x[24]),float(x[56])]]

def helper(x):
    if(x==None):
        return False
    return True

def subset1(x):
    if(str(x)==''):
        return False
    return float(x)>15.0

def helper1(x):
    y = str(x)
    z = y[1:len(x) - 1]
    return str(z)




conf = SparkConf().setAppName("DIC FINAL PROJECT")
sc = SparkContext(conf=conf)
rdd=sc.textFile("s3n://csc591-dic-airline-data/??15.csv")
mapped_rdd=rdd.map(lambda line:line.split(","))
(trainingData, testData) = mapped_rdd.randomSplit([0.7, 0.3])

feature_rdd=trainingData.map(lambda x:subset(x))
filter_header=feature_rdd.filter(lambda x:helper(x))

carriers=set()
def carriers_generator(list1):
    if(list1[1][3] in carriers):
        return False
    else:
        print list1[1][3]
        carriers.add(list1[1][3])
        return True


carriers1=filter_header.filter(lambda x:carriers_generator(x)).map(lambda x:x[1][3]).collect()

src=set()

def source1(list1):
    if(list1[1][4] in src):
        return False
    else:
        print list1[1][4]
        src.add(list1[1][4])
        return True

source=filter_header.filter(lambda x:source1(x)).map(lambda x:x[1][4]).collect()

dest=set()
def source2(list1):
    if(list1[1][5] in dest):
        return False
    else:
        print list1[1][5]
        dest.add(list1[1][5])
        return True

destination=filter_header.filter(lambda x:source2(x)).map(lambda x:x[1][5]).collect()


carriers_list=enumerate(carriers1)
source_list=enumerate(source)
destination_list=enumerate(destination)

carriers_list=dict([reversed(x) for x in list(carriers_list)])
source_list=dict([reversed(x) for x in list(source_list)])
destination_list=dict([reversed(x) for x in list(destination_list)])

pickle.dump(carriers_list,open("carriers","wb"))
pickle.dump(source_list,open("source","wb"))
pickle.dump(destination_list,open("destination","wb"))














