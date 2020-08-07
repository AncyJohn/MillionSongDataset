# Dfining schema for each dataset

import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import * 


spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()



schema_daily = StructType([
    StructField("ID", StringType(), True),
    StructField("DATE", StringType(), True),
    StructField("ELEMENT", StringType(), True),
    StructField("VALUE", FloatType(), True),
    StructField("MEASUREMENT_FLAG", StringType(), True),
    StructField("QUALITY_FLAG", StringType(), True),
    StructField("SOURCE_FLAG", StringType(), True),
    StructField("OBSERVATION_TIME", StringType(), True),
    ])
    
daily = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/2020.csv.gz")
    .limit(1000)   
)
# Reference: https://stackoverflow.com/questions/52122857/taking-a-data-frame-sample-using-limit-in-pyspark

daily.cache()
daily.show()

from pyspark.sql import functions as F
    
schema_stations = StructType([
    StructField("ID", StringType(), True),
    StructField("LATITUDE", FloatType(), True),
    StructField("LONGITUDE", FloatType(), True),
    StructField("ELEVATION", FloatType(), True),
    StructField("STATE", StringType(), True),
    StructField("NAME", StringType(), True),
    StructField("GSN_FLAG", StringType(), True),
    StructField("HCN_FLAG", StringType(), True),
    StructField("WMO_ID", StringType(), True),
    ])
  
stations = (
    spark.read.format("text")
    .load("hdfs:///data/ghcnd/stations")   
)

stations_ps = stations.select(
    F.trim(F.substring(F.col('value'),1,11)).alias('ID').cast(schema_stations['ID'].dataType),
    F.trim(F.substring(F.col('value'),13,8)).alias('LATITUDE').cast(schema_stations['LATITUDE'].dataType),
    F.trim(F.substring(F.col('value'),22,9)).alias('LONGITUDE').cast(schema_stations['LONGITUDE'].dataType),
    F.trim(F.substring(F.col('value'),32,6)).alias('ELEVATION').cast(schema_stations['ELEVATION'].dataType),
    F.trim(F.substring(F.col('value'),39,2)).alias('STATE').cast(schema_stations['STATE'].dataType),
    F.trim(F.substring(F.col('value'),42,30)).alias('NAME').cast(schema_stations['NAME'].dataType),
    F.trim(F.substring(F.col('value'),73,3)).alias('GSN_FLAG').cast(schema_stations['GSN_FLAG'].dataType),
    F.trim(F.substring(F.col('value'),77,3)).alias('CRN_FLAG').cast(schema_stations['HCN_FLAG'].dataType),
    F.trim(F.substring(F.col('value'),81,5)).alias('WMO_ID').cast(schema_stations['WMO_ID'].dataType)
)  

stations_ps.cache()
stations_ps.show()
stations_ps.count()
   
# Reference: https://stackoverflow.com/questions/41944689/pyspark-parse-fixed-width-text-file

# Count of stations do not have a 'WMO_ID'
counts = (
    stations_ps
    # Select only the columns that are needed
    .select(['ID', 'WMO_ID'])
    .filter('WMO_ID = ""')
)
counts.count()
   
schema_countries = StructType([
    StructField("CODE", StringType(), True),
    StructField("NAME", StringType(), True),
    ])
    
countries = (
    spark.read.format("text")
    .load("hdfs:///data/ghcnd/countries")   
)

countries_ps = countries.select(
    F.trim(F.substring(F.col('value'),1,2)).alias('CODE').cast(schema_countries['CODE'].dataType),
    F.trim(F.substring(F.col('value'),4,47)).alias('NAME').cast(schema_countries['NAME'].dataType),
)

countries_ps.cache()
countries_ps.show()
countries_ps.count()
    
schema_states = StructType([
    StructField("CODE", StringType(), True),
    StructField("NAME", StringType(), True),
    ])
    
states = (
    spark.read.format("text")
    .load("hdfs:///data/ghcnd/states")   
)

states_ps = states.select(
    F.trim(F.substring(F.col('value'),1,2)).alias('CODE').cast(schema_states['CODE'].dataType),
    F.trim(F.substring(F.col('value'),4,47)).alias('NAME').cast(schema_states['NAME'].dataType),
)

states_ps.cache()
states_ps.show()
states_ps.count()
    
schema_inventory = StructType([
    StructField("ID", StringType(), True),
    StructField("LATITUDE", FloatType(), True),
    StructField("LONGITUDE", FloatType(), True),
    StructField("ELEMENT", StringType(), True),
    StructField("FIRSTYEAR", IntegerType(), True),
    StructField("LASTYEAR", IntegerType(), True),
    ])
    

inventory = (
    spark.read.format("text")
    .load("hdfs:///data/ghcnd/inventory")   
)

inventory_ps = inventory.select(
    F.trim(F.substring(F.col('value'),1,11)).alias('ID').cast(schema_inventory['ID'].dataType),
    F.trim(F.substring(F.col('value'),13,8)).alias('LATITUDE').cast(schema_inventory['LATITUDE'].dataType),
    F.trim(F.substring(F.col('value'),22,9)).alias('LONGITUDE').cast(schema_inventory['LONGITUDE'].dataType),
    F.trim(F.substring(F.col('value'),32,4)).alias('ELEMENT').cast(schema_inventory['ELEMENT'].dataType),
    F.trim(F.substring(F.col('value'),37,4)).alias('FIRSTYEAR').cast(schema_inventory['FIRSTYEAR'].dataType),
    F.trim(F.substring(F.col('value'),42,4)).alias('LASTYEAR').cast(schema_inventory['LASTYEAR'].dataType),
)
inventory_ps.cache()
inventory_ps.show()
inventory_ps.count() # Number of records

