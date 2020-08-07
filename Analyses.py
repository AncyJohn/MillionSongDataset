hdfs dfs -mkdir /user/ajo139/outputs/ghcnd # To create a directory
hdfs dfs -ls /user/ajo139/outputs/ghcnd # To list the contents
hdfs dfs -rm -r /user/ajo139/outputs/ghcnd/ # to remove a file
hdfs dfs -du /data/ghcnd/daily # File size
hdfs dfs -copyToLocal /user/ajo139/outputs/ghcnd/daily_NZ.csv.gz /users/home/ajo139/outputs/data # copy to local directory
hdfs dfs -du /user/ajo139/outputs/ghcnd
cat /users/home/ajo139/outputs/data/daily_NZ.csv.gz/*.csv.gz | gunzip | wc -l
hdfs dfs -cat /user/ajo139/outputs/ghcnd/daily_NZ.csv.gz/*.csv.gz | gunzip | wc -l
hdfs dfs -copyToLocal /user/ajo139/outputs/ghcnd/NZ_plot.json /users/home/ajo139/outputs/data
cat /users/home/ajo139/outputs/data/daily_NZ.csv.gz/*.csv.gz | gunzip > NZplot.csv

import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql import functions as F
from pyspark.sql.functions import array_contains
from pyspark.sql.functions import first


spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()


# Analyses Q1

df.filter("LASTYEAR == 2020").count() # active stations
df.filter("GSN_FLAG == 'GSN'").count() # Stations with GSN flag
df.filter("CRN_FLAG == 'HCN'").count() # Stations with HCN flag
df.filter("CRN_FLAG == 'CRN'").count() # Stations with CRN flag
df.filter((F.col("CRN_FLAG") == 'HCN') & (F.col("GSN_FLAG") == 'GSN')).count() # Stations in more than one network
df.filter((F.col("CRN_FLAG") == 'CRN') & (F.col("GSN_FLAG") == 'GSN')).count()

# Total number of stations in each country
N_S = (
    df
    .select(['COUNTRY', 'ID'])
    .groupBy("COUNTRY")
    .agg({'ID': 'count'})
    .select(
        F.col('COUNTRY').alias('NAME'),
        F.col('count(ID)').alias('NO_STATIONS')
    )
)
# Modify countries
countries_ps = countries_ps.join(N_S, on=['NAME'], how='left')
countries_ps.write.parquet('hdfs:///user/ajo139/outputs/ghcnd/countries_enriched.parquet')

# Total number of stations in each state
N_S1 = (
    df
    .select(['STATE', 'ID'])
    .groupBy("STATE")
    .agg({'ID': 'count'})
    .select(
        F.col('STATE').alias('NAME'),
        F.col('count(ID)').alias('NO_STATIONS')
    )
)
# Modify countries
states_ps = states_ps.join(N_S1, on=['NAME'], how='left')
states_ps.write.parquet('hdfs:///user/ajo139/outputs/ghcnd/states_enriched.parquet')

df.filter("COUNTRY == 'New Zealand'").show()
df.filter((F.col("COUNTRY") == 'New Zealand') & (F.col("LASTYEAR") == 2020)).show()

df.filter("LATITUDE > 0").count() # Stations in northern hemisphere
df = df.where(F.col("COUNTRY").like("%[United States]")) # Territories of United States
df.select("ID").distinct().count()

df.filter("COUNTRY_CODE == 'US'").count()
df = df.withColumnRenamed('LASTSTYEAR', 'LASTYEAR')
df.write.parquet('hdfs:///user/ajo139/outputs/ghcnd/enriched_stations.parquet')

# Analyses Q2

# Geographical distance between two stations
from pyspark.sql import DataFrameWriter as W
from math import radians, cos, sin, asin, sqrt
spark = SparkSession.builder.appName('HDFS_Haversine_Fun').getOrCreate()



df = (
    spark.read.format("parquet")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("hdfs:///user/ajo139/outputs/ghcnd/stations.parquet") 
)


GD_test = (
    df
    .filter((F.col("COUNTRY_CODE") == 'IN') & (F.col("LASTYEAR") == 2020))
    .select(['ID','NAME','LATITUDE', 'LONGITUDE'])
    .limit(5)
)

# Python program for the haversine formula 

def haversine(lat1, lon1, lat2, lon2): 
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])  # Transform to radiance
    
    # distance between latitudes and longitudes  
    dLat = (lat2 - lat1) 
    dLon = (lon2 - lon1) 
    
    # apply formulae 
    a = (pow(sin(dLat / 2), 2) + 
         pow(sin(dLon / 2), 2) * cos(lat1) * cos(lat2)
         ); # Area
    r = 6371 # Radius
    c = 2 * asin(sqrt(a)) # Central angle
    d = r * c # To calculate distance
    return abs(round(d, 2)) # absolute value
  
udf_haversine = F.udf(haversine) # To convert python function to udf (user defined function)

# Cross join
GD_test = (
    GD_test
    .crossJoin(GD_test)
    .toDF('ID1', 'NAME1', 'LAT1', 'LON1', 'ID2', 'NAME2', 'LAT2', 'LON2') # To create a dataframe
)

GD_test = (GD_test.filter(GD_test.ID1 != GD_test.ID2)) # To clean up the rows


# Apply udf
GD_test = (
    GD_test
    .withColumn('DISTANCE', 
                 udf_haversine(GD_test.LAT1, 
                               GD_test.LON1,
                               GD_test.LAT2, 
                               GD_test.LON2
                               )
                )
)
from pyspark.sql.types import DoubleType
GD_test = GD_test.withColumn("DISTANCE", GD_test["DISTANCE"].cast(DoubleType())) # To change the default column type

# Pairwise distance between all stations in New Zealand
GD_NZ = (
    df
    .filter("COUNTRY_CODE == 'NZ'")
    .select(['ID','NAME','LATITUDE', 'LONGITUDE'])
)
GD_NZ = (
    GD_NZ
    .crossJoin(GD_NZ)
    .toDF('ID1', 'NAME1', 'LAT1', 'LON1', 'ID2', 'NAME2', 'LAT2', 'LON2') # To create a dataframe
)
GD_NZ = (GD_NZ.filter(GD_NZ.ID1 != GD_NZ.ID2))
GD_NZ = (
    GD_NZ
    .withColumn('DISTANCE', 
                 udf_haversine(GD_NZ.LAT1, 
                               GD_NZ.LON1,
                               GD_NZ.LAT2, 
                               GD_NZ.LON2
                               )
                )
)
GD_NZ = GD_NZ.withColumn("DISTANCE", GD_NZ["DISTANCE"].cast('double'))

from pyspark.sql import Row
GD_NZ = GD_NZ.dropDuplicates(['DISTANCE']) # To remove duplicates
GD_NZ = GD_NZ.orderBy('DISTANCE', ascending = False)
GD_NZ.write.parquet('hdfs:///user/ajo139/outputs/ghcnd/GD_NZ.parquet')

# Q3 Analyses 
# Number of observations in daily
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
    
daily2020 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/2020.csv.gz")   
)
daily2020.count()

daily2015 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/2015.csv.gz")   
)
daily2015.count()

daily5ys = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/{201[5-9],2020}.csv.gz")   
)
daily5ys.count()

print(daily5ys.rdd.getNumPartitions()) # To get the number of partitions

daily = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily")   
)
daily.count()

# Count of core observations
daily.filter('ELEMENT == "PRCP"').count()
daily.filter('ELEMENT == "SNOW"').count()
daily.filter('ELEMENT == "SNWD"').count()
daily.filter('ELEMENT == "TMAX"').count()
daily.filter('ELEMENT == "TMIN"').count()

# Checking with Q3 Processing
df = (
    spark.read.format("parquet")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("hdfs:///user/ajo139/outputs/ghcnd/stations.parquet") 
)

df = df.select(['ID', 'CORE_ELEMENTS']).withColumn('PRCP', array_contains(df.CORE_ELEMENTS, 'PRCP'))

from pyspark.sql.functions import first
# TMIN not having TMAX
daily1 = (
    daily
    .select(['ID','DATE','ELEMENT','VALUE'])
    .filter((F.col("ELEMENT") == 'TMAX') | (F.col("ELEMENT") == 'TMIN'))
    .groupBy('ID','DATE')
    .pivot('ELEMENT',['TMAX', 'TMIN'])
    .agg(first("VALUE"))
)
daily1.show(5)
daily1 = daily1.where((F.col("TMAX").isNull()) & (F.col("TMIN").isNotNull()))
daily1.show(5)
daily1.count()  

from pyspark.sql.functions import countDistinct  
daily1.agg(countDistinct('ID', 'DATE')).show() # To check if the columns are distinct
daily1.agg(countDistinct('ID')).show() # Distinct stations

# TMIN and TMAX in NZ

daily_NZ = (
    daily
    .withColumn('COUNTRY_CODE', F.substring(daily['ID'],1,2)) # To extract the country code
    .filter(((F.col("ELEMENT") == 'TMAX') | (F.col("ELEMENT") == 'TMIN')) & (F.col("COUNTRY_CODE") == 'NZ'))
    .drop('COUNTRY_CODE')
)
daily_NZ.write.parquet('hdfs:///user/ajo139/outputs/ghcnd/daily_NZ.parquet')
daily_NZ.count()


daily_NZ.rdd.getNumPartitions() # To check number of partitions
daily_NZ.coalesce(1).rdd.getNumPartitions() # To reduce number of partitions
daily_NZ.write.format("com.databricks.spark.csv").option("header", "true").option("codec", "org.apache.hadoop.io.compress.GzipCodec").save("hdfs:///user/ajo139/outputs/ghcnd/daily_NZ.csv.gz")

daily_NZ = (
    spark.read.format("parquet")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("hdfs:///user/ajo139/outputs/ghcnd/daily_NZ.parquet")   
)
# Years covered

daily_test = daily_NZ.withColumn('YEAR', F.substring(daily['DATE'],1,4).cast(IntegerType()))
year1 = daily_test.agg({'YEAR': 'max'}).collect()[0]
year2 = daily_test.agg({'YEAR': 'min'}).collect()[0]
Range = year1['max(YEAR)'] - year2['min(YEAR)']
print(year1['max(YEAR)'])
print(year2['min(YEAR)'])
print(Range)
# Temperature time series for each station
from pyspark.sql.functions import avg
from pyspark.sql.types import IntegerType


df_st = (
     spark.read.format("parquet")
     .option("header", "true")
     .option("inferSchema", "true")
     .load("hdfs:///user/ajo139/outputs/ghcnd/stations.parquet")
 )
df_st = df_st.select(['ID', 'NAME'])
df = daily_NZ.join(df_st, on=['ID'], how='left')
df =(
    df
   .withColumn('YEAR', F.substring(daily_NZ['DATE'],1,4).cast(IntegerType()))
   .groupBy('YEAR','ELEMENT')
   .pivot('NAME')
   .agg(first('VALUE'))
)
df.write.format("com.databricks.spark.csv").option("header", "true").option("codec", "org.apache.hadoop.io.compress.GzipCodec").save("hdfs:///user/ajo139/outputs/ghcnd/daily_NZ_pivoted.csv.gz")
df.select("COUNTRY").distinct().show()
df.printSchema()
df.write.format("com.databricks.spark.csv").mode('overwrite').option("header", "true").option("codec", "org.apache.hadoop.io.compress.GzipCodec").save("hdfs:///user/ajo139/outputs/ghcnd/daily_NZ.csv.gz")

# Average time series for the entire New Zealand
daily_NZ = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("hdfs:///user/ajo139/outputs/ghcnd/daily_NZ.csv.gz")   
)
df = (
    daily_NZ
    .withColumn('YEAR', F.substring(daily_NZ['DATE'],1,4).cast(IntegerType()))
    .select(['ID','YEAR','ELEMENT','VALUE'])
)
df.write.format("com.databricks.spark.csv").option("header", "true").option("codec", "org.apache.hadoop.io.compress.GzipCodec").save("hdfs:///user/ajo139/outputs/ghcnd/daily_NZ1.csv.gz")


# Precipitation

daily_prcp = (
    daily
    .where('ELEMENT == "PRCP"')
    .withColumn('COUNTRY_CODE', F.substring(daily['ID'],1,2))
    .withColumn('YEAR', F.substring(daily['DATE'],1,4))
    .groupBy('COUNTRY_CODE', 'YEAR')
    .agg(avg('VALUE').alias('AVERAGE_PRCP'))
) 

daily_prcp.write.parquet('hdfs:///user/ajo139/outputs/ghcnd/daily_prcp.parquet')
top = daily_prcp.agg({'AVERAGE_PRCP': 'max'}).collect()[0]
print(top['AVERAGE_PRCP'])
daily_prcp.filter(F.col('AVERAGE_PRCP') == top['max(AVERAGE_PRCP)']).show() # Highest rainfall
daily_prcp.agg(countDistinct('COUNTRY_CODE')).show()

df = (
    spark.read.format("parquet")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("hdfs:///user/ajo139/outputs/ghcnd/stations.parquet") 
)

df1 = (
    spark.read.format("parquet")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("hdfs:///user/ajo139/outputs/ghcnd/daily_prcp.parquet") 
)
df = (
    spark.read.format("parquet")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("hdfs:///user/ajo139/outputs/ghcnd/stations_enriched.parquet") 
)
df1 = df1.withColumnRenamed('COUNTRY_CODE', 'CODE')
df = df1.join('df', on=['CODE'], how='left')
df = df.drop('NO_STATIONS')
df.write.format("com.databricks.spark.csv").mode('overwrite').option("header", "true").option("codec", "org.apache.hadoop.io.compress.GzipCodec").save("hdfs:///user/ajo139/outputs/ghcnd/PRCP_map.csv.gz")

# Reference: https://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html
# Reference: https://www.geeksforgeeks.org/haversine-formula-to-find-distance-between-two-points-on-a-sphere/
# Reference: https://medium.com/@nikolasbielski/using-a-custom-udf-in-pyspark-to-compute-haversine-distances-d877b77b4b18
# Reference: https://stackoverflow.com/questions/37262762/filter-pyspark-dataframe-column-with-none-value