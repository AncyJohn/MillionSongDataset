hdfs dfs -ls /data/msd
hdfs dfs -cat /data/msd/audio/attributes/*.csv | wc -l
hdfs dfs -cat /data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv/*.csv.gz | gunzip | wc -l
hdfs dfs -cat /data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv/*.csv.gz | gunzip | wc -l
hdfs dfs -cat /data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv/*.csv.gz | gunzip | wc -l
hdfs dfs -cat /data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv/*.csv.gz | gunzip | wc -l
hdfs dfs -cat /data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv/*.csv.gz | gunzip | wc -l
hdfs dfs -cat /data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv/*.csv.gz | gunzip | wc -l
hdfs dfs -cat /data/msd/audio/features/msd-marsyas-timbral-v1.0.csv/*.csv.gz | gunzip | wc -l
hdfs dfs -cat /data/msd/audio/features/msd-mvd-v1.0.csv/*.csv.gz | gunzip | wc -l
hdfs dfs -cat /data/msd/audio/features/msd-rh-v1.0.csv/*.csv.gz | gunzip | wc -l
hdfs dfs -cat /data/msd/audio/features/msd-rp-v1.0.csv/*.csv.gz | gunzip | wc -l
hdfs dfs -cat /data/msd/audio/features/msd-ssd-v1.0.csv/*.csv.gz | gunzip | wc -l
hdfs dfs -cat /data/msd/audio/features/msd-trh-v1.0.csv/*.csv.gz | gunzip | wc -l
hdfs dfs -cat /data/msd/audio/features/msd-tssd-v1.0.csv/*.csv.gz | gunzip | wc -l
hdfs dfs -cat /data/msd/audio/statistics/*.csv.gz | gunzip | wc -l
hdfs dfs -ls /data/msd/genre
hdfs dfs -cat  /data/msd/genre/msd-topMAGD-genreAssignment.tsv | head
# Dfining schema for each dataset

import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()

attributes = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    #.schema(schema_daily)
    .load("hdfs:///data/msd/audio/attributes")
    .limit(1000)   
)
attributes.printSchema()
schema_attributes = StructType([
    StructField("ID", StringType(), True),
    StructField("DATE", StringType(), True),
    StructField("ELEMENT", StringType(), True),
    StructField("VALUE", FloatType(), True),
    StructField("MEASUREMENT_FLAG", StringType(), True),
    StructField("QUALITY_FLAG", StringType(), True),
    StructField("SOURCE_FLAG", StringType(), True),
    StructField("OBSERVATION_TIME", StringType(), True),
    ])
attributes.count()
features = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    #.schema(schema_daily)
    .load("hdfs:///data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv")
    .limit(1000)   
)
features.printSchema()
statistics = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    #.schema(schema_daily)
    .load("hdfs:///data/msd/audio/statistics/sample_properties.csv.gz")
    .limit(1000)   
)
statistics.printSchema()
genre = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    #.schema(schema_daily)
    .load("hdfs:///data/msd/genre/msd-MAGD-genreAssignment.tsv")
    .limit(1000)   
)
genre.printSchema()
analysis = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    #.schema(schema_daily)
    .load("hdfs:///data/msd/main/summary/analysis.csv.gz")
    .limit(1000)   
)
analysis.printSchema()
metadata = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    #.schema(schema_daily)
    .load("hdfs:///data/msd/main/summary/metadata.csv.gz")
    .limit(1000)   
)
metadata.printSchema()
triplets_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("song_id", StringType(), True),
    StructField("plays", IntegerType(), True)
])
triplets = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", "\t")
    .option("codec", "gzip")
    .schema(triplets_schema)
    .load("hdfs:///data/msd/tasteprofile/triplets.tsv/")
    #.cache()
)
triplets.printSchema()
from pyspark.sql.functions import countDistinct  
triplets.agg(countDistinct('song_id')).show() # Distinct songs
mismatches = (
    spark.read.format("text")
    .option("header", "true")
    .option("inferSchema", "true")
    #.schema(schema_daily)
    .load("hdfs:///data/msd/tasteprofile/mismatches")
    .limit(1000)   
)

# Processing Q2 (a)

mismatches_schema = StructType([
    StructField("song_id", StringType(), True),
    StructField("song_artist", StringType(), True),
    StructField("song_title", StringType(), True),
    StructField("track_id", StringType(), True),
    StructField("track_artist", StringType(), True),
    StructField("track_title", StringType(), True)
])

with open("/scratch-network/courses/2020/DATA420-20S1/data/msd/tasteprofile/mismatches/sid_matches_manually_accepted.txt", "r") as f:
    lines = f.readlines()
    sid_matches_manually_accepted = []
    for line in lines:
        if line.startswith("< ERROR: "):
            a = line[10:28]
            b = line[29:47]
            c, d = line[49:-1].split("  !=  ")
            e, f = c.split("  -  ")
            g, h = d.split("  -  ")
            sid_matches_manually_accepted.append((a, e, f, b, g, h))

matches_manually_accepted = spark.createDataFrame(sc.parallelize(sid_matches_manually_accepted, 8), schema=mismatches_schema)
matches_manually_accepted.cache()
matches_manually_accepted.show(10, 40)

print(matches_manually_accepted.count())  # 488

with open("/scratch-network/courses/2020/DATA420-20S1/data/msd/tasteprofile/mismatches/sid_mismatches.txt", "r") as f:
    lines = f.readlines()
    sid_mismatches = []
    for line in lines:
        if line.startswith("ERROR: "):
            a = line[8:26]
            b = line[27:45]
            c, d = line[47:-1].split("  !=  ")
            e, f = c.split("  -  ")
            g, h = d.split("  -  ")
            sid_mismatches.append((a, e, f, b, g, h))

mismatches = spark.createDataFrame(sc.parallelize(sid_mismatches, 64), schema=mismatches_schema)
mismatches.cache()
mismatches.show(10, 40)

print(mismatches.count())  # 19094

triplets_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("song_id", StringType(), True),
    StructField("plays", IntegerType(), True)
])
triplets = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", "\t")
    .option("codec", "gzip")
    .schema(triplets_schema)
    .load("hdfs:///data/msd/tasteprofile/triplets.tsv/")
    .cache()
)
#triplets.cache()
triplets.show(10, 50)

mismatches = mismatches.join(matches_manually_accepted, on="song_id", how="left_anti")
mismatches.count() #19093
triplets_matched = triplets.join(mismatches, on="song_id", how="left_anti")
triplets_matched.write.parquet('hdfs:///user/ajo139/outputs/msd/triplets_matched.parquet')

print(triplets.count())                 # 48373586
print(triplets_matched.count())  # 45795111

# Processing Q2 (b)

# hdfs dfs -cat "/data/msd/audio/attributes/*" | awk -F',' '{print $2}' | sort | uniq



audio_attribute_type_mapping = {
  "NUMERIC": DoubleType(),
  "real": DoubleType(),
  "string": StringType(),
  "STRING": StringType()
}

audio_dataset_names = [
  "msd-jmir-area-of-moments-all-v1.0",
  "msd-jmir-lpc-all-v1.0",
  "msd-jmir-methods-of-moments-all-v1.0",
  "msd-jmir-mfcc-all-v1.0",
  "msd-jmir-spectral-all-all-v1.0",
  "msd-jmir-spectral-derivatives-all-all-v1.0",
  "msd-marsyas-timbral-v1.0",
  "msd-mvd-v1.0",
  "msd-rh-v1.0",
  "msd-rp-v1.0",
  "msd-ssd-v1.0",
  "msd-trh-v1.0",
  "msd-tssd-v1.0"
]

audio_dataset_schemas = {}
for audio_dataset_name in audio_dataset_names:
  print(audio_dataset_name)

  audio_dataset_path = f"/scratch-network/courses/2020/DATA420-20S1/data/msd/audio/attributes/{audio_dataset_name}.attributes.csv"
  with open(audio_dataset_path, "r") as f:
    rows = [line.strip().split(",") for line in f.readlines()]

  audio_dataset_schemas[audio_dataset_name] = StructType([
    StructField(row[0], audio_attribute_type_mapping[row[1]], True) for row in rows
  ])

audio_dataset_schemas.keys()
type(audio_dataset_schemas)
audio_dataset_name = "msd-jmir-area-of-moments-all-v1.0"
features = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("codec", "gzip")
    .schema(audio_dataset_schemas[audio_dataset_name])
    .load(f"hdfs:///data/msd/audio/features/{audio_dataset_name}.csv/") 
    .cache()
)
features.take(1)

# https://intellipaat.com/community/5161/how-do-i-add-a-new-column-to-a-spark-dataframe-using-pyspark