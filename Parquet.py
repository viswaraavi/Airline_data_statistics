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

convert(sqlContext,"s3n://csc591-dic-airline-data/201?.csv","","dataset_5years")
df = sqlContext.read.parquet("s3n://csc591-dic-airline-data/dataset_5years")
df.registerTempTable("airline_tbl")

df_final = df.select([c for c in df.columns if c in {'YEAR','MONTH','FL_DATE' ,'UNIQUE_CARRIER' ,'CARRIER','FL_NUM','ORIGIN_CITY_NAME','DEST_CITY_NAME','DEP_DELAY_NEW','ARR_DELAY_NEW','CANCELLED','AIR_TIME','CARRIER_DELAY','WEATHER_DELAY','NAS_DELAY','SECURITY_DELAY','LATE_AIRCRAFT_DELAY'}])
df_final.write.parquet("s3n://csc591-dic-airline-data/super_reduced_dataset");
df = sqlContext.read.parquet("s3n://csc591-dic-airline-data/super_reduced_dataset")
df.registerTempTable("airline_tbl")
#spark.sql("select * from airline_5 where month = 1 and weather_delay>1")


def Delay_Statistics(start_date,end_date,carrier=None,origin_city=None,dest_city=None):
    
    sql_query =  "SELECT SUM(CARRIER_DELAY),SUM(WEATHER_DELAY),SUM(NAS_DELAY),SUM(SECURITY_DELAY),SUM(LATE_AIRCRAFT_DELAY) \
            FROM airline_tbl\
            WHERE FL_DATE >='"+start_date +"' AND FL_DATE<='" + end_date + "'\
            "
    if carrier != None and  carrier != "":
        sql_query = sql_query + " AND UNIQUE_CARRIER = '" + carrier + "'";
    if origin_city !=None and origin_city !="":
        sql_query = sql_query + " AND ORIGIN_CITY_NAME = '" + origin_city + "'";
    if dest_city !=None and dest_city !="":
        sql_query = sql_query + " AND DEST_CITY_NAME = '" + dest_city + "'";
    return spark.sql(sql_query)    

def MostDelaysByCarrier(start_date,end_date,origin_city = None,dest_city = None):
   
    sql_query =  "SELECT CARRIER,AVG(ARR_DELAY_NEW) as ARR_DELAY_AVG , AVG(DEP_DELAY_NEW) AS DEP_DELAY_AVG \
            FROM airline_tbl\
            WHERE FL_DATE >='"+start_date +"' AND FL_DATE<='" + end_date + "'\
          "
    if origin_city !=None and origin_city !="":
        sql_query = sql_query + " AND ORIGIN_CITY_NAME = '" + origin_city + "'";
    if dest_city !=None and dest_city !="":
        sql_query = sql_query + " AND DEST_CITY_NAME = '" + dest_city + "'";
    sql_query = sql_query + "GROUP BY CARRIER"
    intermediate_df = spark.sql(sql_query) 
    intermediate_df.registerTempTable("temp_aggregate_tbl")    
    sql_query = "SELECT CARRIER, ARR_DELAY_AVG,DEP_DELAY_AVG \
                FROM temp_aggregate_tbl\
                ORDER BY ARR_DELAY_AVG desc\
                LIMIT 10";
        
    return spark.sql(sql_query)    


def CarrierWithMaximumCancelledFlights(start_date,end_date,origin_city = None,dest_city = None):
    #returns carrier, cancelled_total, total
 
    sql_query =  "SELECT CARRIER, SUM(CANCELLED) as CANCELLED_TOTAL,COUNT(1) AS TOTAL \
            FROM airline_tbl\
            WHERE FL_DATE >='"+start_date +"' AND FL_DATE<='" + end_date + "'\
          "
    if origin_city !=None and origin_city !="":
        sql_query = sql_query + " AND ORIGIN_CITY_NAME = '" + origin_city + "'";
    if dest_city !=None and dest_city !="":
        sql_query = sql_query + " AND DEST_CITY_NAME = '" + dest_city + "'";
    sql_query = sql_query + "GROUP BY CARRIER"
    intermediate_df = spark.sql(sql_query) 
    intermediate_df.registerTempTable("temp_aggregate_tbl")    
    sql_query = "SELECT CARRIER, CANCELLED_TOTAL,TOTAL \
                FROM temp_aggregate_tbl\
                ORDER BY CANCELLED_TOTAL desc\
                LIMIT 10";   
    
    return spark.sql(sql_query)      

def CarrierWithMaximumAirTime(start_date,end_date):
   
    sql_query =  "SELECT CARRIER, Air_Time, FL_NUM , ORIGIN_CITY_NAME ,DEST_CITY_NAME \
            FROM airline_tbl\
            WHERE FL_DATE >='"+start_date +"' AND FL_DATE<='" + end_date + "'"
    sql_query = sql_query + "ORDER BY Air_Time desc limit 10"
    return spark.sql(sql_query)

def MostDelaysByMonth(origin_city = None,dest_city = None):
   
    sql_query =  "SELECT MONTH, AVG(ARR_DELAY_NEW) as ARR_DELAY_AVG , AVG(DEP_DELAY_NEW) AS DEP_DELAY_AVG \
            FROM airline_tbl"
    if origin_city !=None and origin_city !="":
        sql_query = sql_query + " AND ORIGIN_CITY_NAME = '" + origin_city + "'";
    if dest_city !=None and dest_city !="":
        sql_query = sql_query + " AND DEST_CITY_NAME = '" + dest_city + "'";
    sql_query = sql_query + " GROUP BY MONTH "
        
    return spark.sql(sql_query) 

Delay_Statistics("2011-01-01","2011-03-31").show()
CarrierWithMaximumCancelledFlights("2011-01-01","2011-03-31").show()
MostDelaysByCarrier("2011-01-01","2011-03-31").show()
MostDelaysByMonth().show()
CarrierWithMaximumAirTime("2011-01-01","2011-03-31").show()
