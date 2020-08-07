# hdfs dfs -ls /user/ajo139/outputs/msd

import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql.functions import *
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
from pyspark.sql.functions import countDistinct
## Song Recommendations Q1 a
df = (
    spark.read.format("parquet")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("hdfs:///user/ajo139/outputs/msd/triplets_matched.parquet") 
)
#count of unique song
df.agg(countDistinct('song_id')).show() #378310
df.agg(countDistinct('user_id')).show() #1019318

#Q1 b
#  Number of unique songs played by the most active user
df.groupby("user_id").agg(sum('plays').alias("Sum")).sort(desc("Sum")).show(1,False)  # Most active user
df.filter('user_id == "093cb74eb3c517c5179ae24caf0ebec51b24d2a2"').agg(countDistinct('song_id')).show() # 195
(195/378310)*100 # 5.15%

# Q1 c
import matplotlib.pyplot as plt
import pandas as pd
song_popularity = df.groupby("song_id").agg(sum("plays").alias("Total_Play_Count")).sort(desc("Total_Play_Count"))
song_popularity.write.mode('overwrite').option('header', 'false').csv("hdfs:///user/ajo139/outputs/msd/song_popularity.csv.gz")
song_popularity.write.option('header', 'true').csv("hdfs:///user/ajo139/outputs/msd/song_popularity1.csv.gz")
# hdfs dfs -copyToLocal /user/ajo139/outputs/msd/song_popularity.csv.gz /users/home/ajo139/output2/data
# cat /users/home/ajo139/output2/data/song_popularity.csv.gz/*.csv.gz | gunzip > song_popularity.csv

user_activity = df.groupby("user_id").agg(sum("plays").alias("Total_Play_Count")).sort(desc("Total_Play_Count"))
user_activity.write.option('header', 'false').csv("hdfs:///user/ajo139/outputs/msd/user_activity.csv.gz")
# hdfs dfs -copyToLocal /user/ajo139/outputs/msd/user_activity.csv.gz /users/home/ajo139/output2/data
# cat /users/home/ajo139/output2/data/user_activity.csv.gz/*.csv.gz | gunzip > user_activity.csv

# Histograms
song_popularity_hist = song_popularity.toPandas()
song_popularity_hist.hist(column="Total_Play_Count")
plt.xlabel('Count of Plays', fontsize=10)
plt.ylabel('Count of Songs', fontsize=10)
plt.title('Song Popularity Distribution')
plt.tight_layout()
plt.savefig("Song_popularity_dist.png", bbox_inches="tight")

user_activity_hist = user_activity.toPandas()
user_activity_hist.hist(column='Total_Play_Count')
plt.xlabel('Count Of Plays', fontsize=10)
plt.ylabel('Count Of Users', fontsize=10)
plt.title('User activity distribution')
plt.tight_layout()
plt.savefig("Active_user_dist.png", bbox_inches="tight")
# Q1 d
song_popularity.count() #378310
song_popularity.sort("Total_Play_Count", ascending=True).show()
song_popularity.show() # maxumum play count is 726888
song_popularity.approxQuantile("Total_Play_Count", [0.40], 0) # 18
song_popularity_filter = song_popularity.filter(F.col("total_play_count") > 18)
user_activity.approxQuantile("Total_Play_Count", [0.20], 0) # 26
user_activity_filter = user_activity.filter(F.col("total_play_count") > 26)

# inner join to exclude the less popular song
df = df.select(['user_id', 'song_id', 'plays']).join(song_popularity_filter.select('song_id'),on='song_id',how='inner')
# Merge with most active users
df = df.select(['user_id', 'song_id', 'plays']).join(user_activity_filter.select('user_id'),on='user_id',how='inner')
df.count() # 42487946

from pyspark.sql.functions import isnan, when, count, col
df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).show() # no null values

#Q1 e
from pyspark.sql.functions import udf
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline

user_id_indexer= StringIndexer(inputCol="user_id", outputCol="user")
song_id_indexer = StringIndexer(inputCol="song_id", outputCol="item")
pipeline = Pipeline(stages=[user_id_indexer, song_id_indexer])
pipelineFit = pipeline.fit(df)
data = pipelineFit.transform(df)

def integer(x):

    return int(x) 

udf_to_interger = udf(integer, IntegerType())
data = data.withColumn("user_int", udf_to_interger("user")).withColumn("song_int", udf_to_interger("item"))
data = data.drop("user","item")
data.show()

train, test = data.randomSplit([0.7, 0.3], seed = 1)

# distinct users in training
df = test.join(train, on="user_id", how="left_anti") # 3 users not in train
tr_users = train.select("user_id").distinct() 
te_users = test.select("user_id").distinct() 
drop_users = te_users.subtract(tr_users) 

test.count() # 12747212
test = test.join(drop_users, on="user_id", how="left_anti")
test.count() # 12747209
train.count() #29740734
train = train.unionAll(df)
train.count() #29740737


# Q2 a
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.mllib.evaluation import RankingMetrics

als = ALS(implicitPrefs=True, userCol="user_int", itemCol="song_int", ratingCol="plays", coldStartStrategy="drop")
alsModel = als.fit(train)
predictions = alsModel.transform(test)
predictions.show()

evaluator = RegressionEvaluator(metricName="rmse", labelCol="plays", predictionCol="prediction")
rmse = evaluator.evaluate(predictions)

print("Root-mean-square error = " + str(rmse)) # 7.22


# Q2 b Select subset of users
users_subset = data.select(als.getUserCol()).filter(data["user_int"].rlike("213[0-5]")).distinct()
users_subset.count() #1572
users_subset.show()

users_subset_recommend = alsModel.recommendForUserSubset(users_subset, 10) # To generate top 10 recommendations for the subset
users_subset_recommend.show(10, False)


def recommend(recommendations):
    items = []
    for item, rating in recommendations:
        items.append(item)
    return items

udf_recommend = F.udf(lambda recommendations: recommend(recommendations), ArrayType(IntegerType()))

#  Comparing recommendations with the actuals
temp = (
    users_subset_recommend.withColumn('recommends', udf_recommend(F.col('recommendations')))
    .select(['user_int', 'recommends'])
    )

test = test.groupBy("user_int").agg(F.collect_list("song_int"))

# Q2 c

test_metric = temp.join(test, on="user_int", how="left")

test_metric = test_metric.drop("user_int")

#filter out null values as there were records as random split was performed
Rdd_test = test_metric.filter(F.col('collect_list(song_int)').isNotNull()).rdd


from pyspark.mllib.evaluation import RankingMetrics

metrics = RankingMetrics(Rdd_test)

print("Precision@5 :" + str(metrics.precisionAt(5)))
print("NDCG@10 :" + str(metrics.ndcgAt(10)))
print("MAP :" + str(metrics.meanAveragePrecision))

