hadoop@ec2-54-89-121-213.compute-1.amazonaws.com

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
        .option("header", "true")\
        .option("delimiter",",")\
        .option("nullValue","")\
        .option("treatEmptyValuesAsNulls","True")\
        .load(filename)
    df.write.parquet("s3n://csc591-dic-airline-data/"+tablename)

convert(sqlContext,"s3n://csc591-dic-airline-data/??1?.csv","","dataset_5years")
df = sqlContext.read.parquet("s3n://csc591-dic-airline-data/dataset_5years")
df.registerTempTable("airline_tbl")

df_final = df.select([c for c in df.columns if c in {'YEAR','QUARTER','MONTH','DAY_OF_MONTH','DAY_OF_WEEK','FL_DATE' ,'UNIQUE_CARRIER' ,'AIRLINE_ID' ,'CARRIER','FL_NUM','ORIGIN_AIRPORT_ID','ORIGIN','ORIGIN_CITY_NAME','ORIGIN_STATE_ABR','ORIGIN_STATE_NM','ORIGIN_WAC','DEST_AIRPORT_ID','DEST_AIRPORT_SEQ_ID','DEST_CITY_MARKET_ID','DEST','DEST_CITY_NAME','DEST_STATE_ABR','DEST_STATE_NM','DEST_WAC','CRS_DEP_TIME','DEP_TIME','DEP_DELAY_NEW','ARR_DELAY','ARR_DELAY_NEW','CANCELLED','CANCELLATION_CODE','ACTUAl_ELAPSED_TIME','AIR_TIME','FLIGHTS','DISTANCE','CARRIER_DELAY','WEATHER_DELAY','NAS_DELAY','SECURITY_DELAY','LATE_AIRCRAFT_DELAY'}])
df_final.write.parquet("s3n://csc591-dic-airline-data/reduced_dataset");
df = sqlContext.read.parquet("s3n://csc591-dic-airline-data/reduced_dataset")
df.registerTempTable("airline_tbl")
spark.sql("select * from airline_5 where month = 1 and weather_delay>1")


def Delay_Statistics(start_year,end_year,start_month,end_month,airline_id=None,origin_city=None,dest_city=None):
    #returns (Arrival_delay,departure_delay,career_delay,weather_delay,nas_delay,sec_delay,late_aircraft_delay)
    start_date =  start_year + "-" + start_month + "-01"
    end_date = end_year+"-" + end_month + "-31"
    sql_query =  "SELECT SUM(CARRIER_DELAY),SUM(WEATHER_DELAY),SUM(NAS_DELAY),SUM(SECURITY_DELAY),SUM(LATE_AIRCRAFT_DELAY) \
            FROM airline_tbl\
            WHERE FL_DATE >='"+start_date +"' AND FL_DATE<='" + end_date + "'\
            "
    if airline_id != None and airline_id !="":
        sql_query = sql_query + "AND AIRLINE_ID = '" + airline_id + "'";
    if origin_city !=None and origin_city !="":
        sql_query = sql_query + "AND ORIGIN_CITY_NAME = '" + origin_city + "'";
    if dest_city !=None and dest_city !="":
        sql_query = sql_query + "AND DEST_CITY_NAME = '" + dest_city + "'";
    return spark.sql(sql_query)    
        
Delay_Statistics("2011","2011","1","3")
