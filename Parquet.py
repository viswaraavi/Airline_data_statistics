try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    from pyspark.sql import SQLContext
	from pyspark.sql.types import *
except ImportError as e:
    print ("error importing spark modules", e)
    sys.exit(1)

def convert(sqlContext, filename, schema, tablename):
    df = sqlContext.read\
        .format("com.databricks.spark.csv")\
        .schema(schema)\
        .option("delimiter",",")\
        .option("nullValue","")\
        .option("treatEmptyValuesAsNulls","true")\
        .load(filename)
    df.write.parquet("s3n://csc591-dic-airline-data/"+tablename)

schema= StructType([\
        StructField("YEAR",        StringType(),False),\
        StructField("MONTH",        StringType(),False),\
        StructField("AIRLINE_ID",   StringType(),True),\
        StructField("FL_DATE",            StringType(),True),\
        StructField("ORIGIN",             StringType(),True),\
        StructField("CARRIER",         StringType(),True),\
        StructField("DEST",    StringType(),True),\
        StructField("WEATHER_DELAY",            StringType(),True),\
        StructField("CARRIER_DELAY",                   StringType(),True),\
	    StructField("NAS_DELAY",                   StringType(),True),\
	    StructField("SECURITY_DELAY",                   StringType(),True),\
	    StructField("LATE_AIRCRAFT_DELAY",                   StringType(),True)])

convert(sqlContext,"s3n://csc591-dic-airline-data/0100.csv",schema,"test_0100")
df = sqlContext.read.parquet("s3n://csc591-dic-airline-data/test_5years")
df.registerTempTable("airline_5")

spark.sql("select * from airline_5 where month = 1 and weather_delay>1")
