import pickle
import os
import sys


os.environ['SPARK_HOME'] = "/usr/lib/spark/"
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

def subset2(x):
    if(x[0]==u'"YEAR"'):
        return None
    return [int(x[2]),int(x[3]),int(x[4]),helper1(x[6]),helper1(x[14]),helper1(x[24]),float(x[56])]

def subset1(x):

    if(str(x)==''):
        return False
    try:
        return float(x)>15.0
    except ValueError:
        return False


def subset3(x):
    if (str(x) == ''):
        return 0.0
    try:
        if(float(x) > 15.0):
            return 1.0
        else:
            return 0.0
    except ValueError:
        return 0.0

def helper1(x):
    y = str(x)
    z = y[1:len(x) - 1]
    return str(z)

def helper(x):
    if(x==None):
        return False
    return True

def construct_variables(x):
    return LabeledPoint(x[0],[x[1][0],x[1][1],x[1][2],carriers_list[x[1][3]],source_list[x[1][4]],destination_list[x[1][5]],x[1][6]])


carriers_list=pickle.load(open("carriers","rb"))
source_list=pickle.load(open("source","rb"))
destination_list=pickle.load(open("destination","rb"))

conf = SparkConf().setAppName("DIC FINAL PROJECT")
sc = SparkContext(conf=conf)
rdd=sc.textFile("s3n://csc591-dic-airline-data/??15.csv")
mapped_rdd=rdd.map(lambda line:line.split(","))
(trainingData, testData) = mapped_rdd.randomSplit([0.7, 0.3])

feature_rdd=trainingData.map(lambda x:subset(x))
filter_header=feature_rdd.filter(lambda x:helper(x))
training_data=filter_header.map(lambda x:construct_variables(x))

model = RandomForest.trainClassifier(training_data, numClasses=2, categoricalFeaturesInfo={},
                                     numTrees=3, featureSubsetStrategy="auto",
                                     impurity='gini', maxDepth=4, maxBins=32)

print "Done"
model.save(sc, "myRandomForestRegressionModel")
pickle.dump("randommodel",open("regressionmodel","wb"))
samemodel=pickle.load(open("regressionmodel","rb"))
samemodel.predict([12,3,4,5,6,7,1020.00]).collect()


