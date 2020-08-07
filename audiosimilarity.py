

import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql.functions import *
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()



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

#Audio similarity Q1 a
features.describe().show # descriptive statistics
features.select('Area_Method_of_Moments_Overall_Standard_Deviation_1',
                'Area_Method_of_Moments_Overall_Standard_Deviation_2',
                'Area_Method_of_Moments_Overall_Standard_Deviation_3').describe().show()
features.select('Area_Method_of_Moments_Overall_Standard_Deviation_4',
                'Area_Method_of_Moments_Overall_Standard_Deviation_5',
                'Area_Method_of_Moments_Overall_Standard_Deviation_6').describe().show()
features.select('Area_Method_of_Moments_Overall_Standard_Deviation_7',
                'Area_Method_of_Moments_Overall_Standard_Deviation_8',
                'Area_Method_of_Moments_Overall_Standard_Deviation_9').describe().show()
features.select('Area_Method_of_Moments_Overall_Standard_Deviation_10',
                'Area_Method_of_Moments_Overall_Average_1',
                'Area_Method_of_Moments_Overall_Average_2',
                'Area_Method_of_Moments_Overall_Average_3').describe().show()
features.select('Area_Method_of_Moments_Overall_Average_8',
                'Area_Method_of_Moments_Overall_Average_9',
                'Area_Method_of_Moments_Overall_Average_10').describe().show()

# statistics using panda dataframe features
import pandas as pd
pd.DataFrame(features.take(5), columns=features.columns).transpose()
numeric_features = [t[0] for t in features.dtypes if t[1] == 'double']
features.select(numeric_features).describe().toPandas().transpose()

## Correlation matrix
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
 # correlation of 2 columns
features.stat.corr('Area_Method_of_Moments_Overall_Standard_Deviation_1','Area_Method_of_Moments_Overall_Standard_Deviation_2')
corr_fe = features.drop('MSD_TRACKID') # drop string column
# convert to vector column first
vector_col = "corr_features"
assembler = VectorAssembler(inputCols=corr_fe.columns, outputCol=vector_col)
features_vector = assembler.transform(corr_fe).select(vector_col).collect()[0][0].toArray()
# get correlation matrix
matrix1 = Correlation.corr(features_vector, vector_col) # Error
#print("Pearson correlation matrix:\n" + str(matrix1[0]))
matrix2 = Correlation.corr(features_vector, vector_col, "spearman")
#print("Spearman correlation matrix:\n" + str(matrix2[0]))

from pyspark.mllib.stat import Statistics
import pandas as pd
import numpy as np
corr_fe = corr_fe.toPandas()
corr_fe.columns = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','0','p','q','r','s','t']
matrix = corr_fe.corr() # correlatin matrix using panda dataframe
print(matrix)
strong_corr = np.where(matrix>0.8)

corr_fe1 = features.drop('MSD_TRACKID')
corr_fe1 = corr_fe1.na.drop()
col_names = corr_fe1.columns
features = corr_fe1.rdd.map(lambda row: row[0:])


fe_corr = Statistics.corr(features, method="pearson")
fe_corr_df = pd.DataFrame(fe_corr)
fe_corr_df.index, fe_corr_df.columns = col_names, col_names



strong_corr_var=np.where(fe_corr>0.8)
strong_corr_var1=[(fe_corr_df.columns[x], fe_corr_df.columns[y]) for x,y in zip(*strong_corr_var) if x!=y and x<y]
dataframe_strong_corr = pd.DataFrame(np.array(strong_corr_var1))

def strong_corr_value(corr_fe1):
	dataframe_sc = []
	for i in range(0, len(corr_fe1)):
		dataframe_sc.append(fe_corr_df[corr_fe1[0][i]][corr_fe1[1][i]]) 
	return dataframe_sc

df_drop_values = pd.DataFrame(strong_corr_value(dataframe_strong_corr))

strong_corr_result= pd.concat([dataframe_strong_corr,df_drop_values], axis = 1)

strong_corr_result.columns = [0,1,2]

dataframe_schema = StructType([StructField('Variable_1',StringType(),True),
					   StructField('Variable_2',StringType(),True),
					   StructField('Value',DoubleType(),True)])

dataframe_schema_strong_corr = sqlContext.createDataFrame(strong_corr_result, dataframe_schema)

dataframe_schema_strong_corr.show(10,False)


#Audio similarity Q1 b

genre_schema = StructType([
    StructField("track_id", StringType(), True),
    StructField("genre", StringType(), True)
])

genre = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", "\t")
    .option("codec", "gzip")
    .schema(genre_schema)
    .load("hdfs:///data/msd/genre/msd-MAGD-genreAssignment.tsv/")
    .cache()
)
genre.show(10)
genre.count()   # 422714

genres_matched = genre.join(mismatches, on="track_id", how="left_anti") # To remove mismatched entries
genres_matched.count()   # 415350

# Visualize the matched song genres
genre_grp = (
     genres_matched
     .groupby("genre")
     .count())
genre_grp.show()
genre_grp.count() # 21
genre_grp.write.mode('overwrite').option('header', 'true').csv("hdfs:///user/ajo139/outputs/msd/genre_matched.csv")
hdfs dfs -copyToLocal /user/ajo139/outputs/msd/genre_matched.csv /users/home/ajo139/output2 # copy to local directory


schema_df = StructType([
    StructField("genre", StringType(), True),
    StructField("count", IntegerType(), True)
    ])	
df = (
    spark.read.format("csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_df)
    .load("hdfs:///user/ajo139/outputs/msd/genre_matched.csv") 
)	  
df.write.option('header', 'true').csv("hdfs:///user/ajo139/outputs/msd/genre_matched2.csv")


def plot_barchart():
    plt.xlabel('Genre', fontsize=16)
    plt.ylabel('Number of songs', fontsize=16)
    plt.title('Popularity of Genres')
    plt.savefig("barplot_genre", bbox_inches='tight')

if __name__ == "__main__":
    plot_barchart()

# Q1 c
features.schema.names
features = features.withColumn('MSD_TRACKID', F.regexp_replace('MSD_TRACKID', "^'|'$", '')) # to remove quotes
genre_audio = (
    features
	.join(genres_matched, features.MSD_TRACKID == genres_matched.track_id, how = 'left')
	.drop(genres_matched.track_id)
)
#genre_audio = genre_audio.selectExpr = ['Area_Method_of_Moments_Overall_Standard_Deviation_1 as asd1',
#'Area_Method_of_Moments_Overall_Standard_Deviation_2 as asd2',
#'Area_Method_of_Moments_Overall_Standard_Deviation_3 as asd3',
#'Area_Method_of_Moments_Overall_Standard_Deviation_4 as asd4',
#'Area_Method_of_Moments_Overall_Standard_Deviation_5 as asd5',
#'Area_Method_of_Moments_Overall_Standard_Deviation_6 as asd6',
#'Area_Method_of_Moments_Overall_Standard_Deviation_7 as asd7',
#'Area_Method_of_Moments_Overall_Standard_Deviation_8 as asd8',
#'Area_Method_of_Moments_Overall_Standard_Deviation_9 as asd9',
#'Area_Method_of_Moments_Overall_Standard_Deviation_10 as asd10',
#'Area_Method_of_Moments_Overall_Average_1 as aa1',
#'Area_Method_of_Moments_Overall_Average_2 as aa2',
#'Area_Method_of_Moments_Overall_Average_3 as aa3',
#'Area_Method_of_Moments_Overall_Average_4 as aa4',
#'Area_Method_of_Moments_Overall_Average_5 as aa5',
#'Area_Method_of_Moments_Overall_Average_6 as aa6',
#'Area_Method_of_Moments_Overall_Average_7 as aa7',
#'Area_Method_of_Moments_Overall_Average_8 as aa8',
#'Area_Method_of_Moments_Overall_Average_9 as aa9',
#'Area_Method_of_Moments_Overall_Average_10 as aa10',
#'MSD_TRACKID as track_id']

genre_audio.count() # 994623

# Audio similarity Q2 a
features.write.parquet('hdfs:///user/ajo139/outputs/msd/features.parquet')
genre_audio.write.parquet('hdfs:///user/ajo139/outputs/msd/genre_audio.parquet')
# data preprcessing
 
genre_audio = genre_audio.where(col("genre").isNotNull()) # To remove null values
df = genre_audio.drop('MSD_TRACKID')
df.count() # 413277
df.na.drop().count() # To check for the missing values # 413277

from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
label_stringIdx = StringIndexer(inputCol = 'new_label', outputCol = 'label')
pipeline = Pipeline(stages=[label_stringIdx])
pipelineFit = pipeline.fit(binary_df)
data = pipelineFit.transform(binary_df)
data = data.drop('new_label')

# Standardization
from pyspark.ml.linalg import DenseVector # Import DenseVector
input_data = df.rdd.map(lambda x: (x[20], DenseVector(x[:19]))) # Define the input_data 
df = spark.createDataFrame(input_data, ["label","features"])

from pyspark.ml.feature import StandardScaler
standardScaler = StandardScaler(inputCol="features", outputCol="features_scaled") # Initialize the standardScaler
scaler = standardScaler.fit(df) # Fit the DataFrame to the scaler
scaled_df = scaler.transform(df) # Transform the data in `df` with the scaler
scaled_df.take(2)


# dimensional reduction
from pyspark.ml.feature import PCA
pca = PCA(k=2, inputCol='features_scaled', outputCol='features_pca')
model = pca.fit(scaled_df)
reduced_df = model.transform(scaled_df).select('label','features_pca')

# Audio similarity Q2 b

from pyspark.sql.functions import udf
def valueToBinary(value):    # user defined function

   if   value == 'Rap': return 'Rap'
   else: return 'not_Rap'

udfValueToBinary = udf(valueToBinary, StringType())

binary_df = reduced_df.withColumn("new_label", udfValueToCategory("label"))

binary_df.show()

# class imbalance
major_df = binary_df.filter(col("new_label") == 'not_Rap')
minor_df = binary_df.filter(col("new_label") == 'Rap')
ratio = int(major_df.count()/minor_df.count())
print("ratio: {}".format(ratio))
percent = int((minor_df.count()/(major_df.count() + minor_df.count()))*100)
print(percent)

# Split the data
(train, test) = data.randomSplit([0.7, 0.3], seed=100) # set seed for reproducibility
major_df = train.filter(col("new_label") == 'not_Rap')
minor_df = train.filter(col("new_label") == 'Rap')
ratio = int(major_df.count()/minor_df.count())
print("ratio: {}".format(ratio))

# undersampling
sampled_majority_df = major_df.sample(False, 1/ratio)
combined_df = sampled_majority_df.unionAll(minor_df)
combined_df.count() # 28850
combined_df.write.parquet('hdfs:///user/ajo139/outputs/msd/train.parquet')
test.write.parquet('hdfs:///user/ajo139/outputs/msd/test.parquet')
major_df = combined_df.filter(col("new_label") == 'not_Rap')
minor_df = combined_df.filter(col("new_label") == 'Rap')
ratio = int(major_df.count()/minor_df.count())
print("ratio: {}".format(ratio))


train = (
    spark.read.format("parquet")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("hdfs:///user/ajo139/outputs/msd/train.parquet") 
)
train = train.withColumnRenamed('features_pca', 'features')
test = (
    spark.read.format("parquet")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("hdfs:///user/ajo139/outputs/msd/test.parquet") 
)
test = test.withColumnRenamed('features_pca', 'features')
## Logistic regression
timestart = datetime.datetime.now()
from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression(featuresCol = 'features', labelCol = 'label', maxIter=10, regParam=0.3, elasticNetParam=0.8)
lrModel = lr.fit(train)

predictions = lrModel.transform(test)
predictions.show(5)

# evaluation
from pyspark.ml.evaluation import BinaryClassificationEvaluator
evaluator1 = BinaryClassificationEvaluator(metricName="areaUnderROC")
evaluator2 = BinaryClassificationEvaluator(metricName="areaUnderPR")
print('Test Area Under ROC', evaluator1.evaluate(predictions))
print('Test Area Under PR', evaluator2.evaluate(predictions))
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
evaluator3 = MulticlassClassificationEvaluator(predictionCol="prediction", metricName="weightedPrecision")
evaluator4 = MulticlassClassificationEvaluator(predictionCol="prediction", metricName="weightedRecall")
evaluator5 = MulticlassClassificationEvaluator(predictionCol="prediction", metricName="accuracy")
print('Precision', evaluator3.evaluate(predictions))
print('Recall', evaluator4.evaluate(predictions))
print('accuracy', evaluator5.evaluate(predictions))


# PRINT ELAPSED TIME    
timeend = datetime.datetime.now()
timedelta = round((timeend-timestart).total_seconds(), 2) 
print "Time taken to execute above cell: " + str(timedelta) + " seconds";

## Gradient boosted tree classifier
from pyspark.ml.classification import GBTClassifier
gbt = GBTClassifier(labelCol="label", featuresCol="features", maxIter=10)
model = gbt.fit(train)
predictions = model.transform(test)
predictions.show(5)
print("Test Area Under ROC: " + str(evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})))
print(model)  # summary only

evaluator3 = MulticlassClassificationEvaluator(predictionCol="prediction", metricName="weightedPrecision")
evaluator4 = MulticlassClassificationEvaluator(predictionCol="prediction", metricName="weightedRecall")
evaluator5 = MulticlassClassificationEvaluator(predictionCol="prediction", metricName="accuracy")
print('Precision', evaluator3.evaluate(predictions))
print('Recall', evaluator4.evaluate(predictions))
print('accuracy', evaluator5.evaluate(predictions))

## Linear support vector machine
from pyspark.ml.classification import LinearSVC
lsvc = LinearSVC(labelCol="label", featuresCol="features", maxIter=10, regParam=0.1)
lsvcModel = lsvc.fit(train)
predictions = lsvcModel.transform(test)
predictions.show(5)

# Print the coefficients and intercept for linear SVC
print("Coefficients: " + str(lsvcModel.coefficients))
print("Intercept: " + str(lsvcModel.intercept))


print("Test Area Under ROC: " + str(evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})))
print(lsvcModel)  # summary only

## Audio Similarity Q3 b
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

lr = LogisticRegression(family="binomial")
paramGrid = (ParamGridBuilder() 
    .addGrid(lr.regParam, [0.08, 0.1, 0.3])  
    .addGrid(lr.elasticNetParam, [0.4, 0.6, 0.8])
    .addGrid(lr.maxIter,[10, 100, 1000])
    .addGrid(lr.aggregationDepth,[2,5,10])
    .addGrid(lr.fitIntercept,[False, True])
    .build())

cv = CrossValidator(
    estimator=lr,
    estimatorParamMaps=paramGrid,
    evaluator= evaluator1,
    numFolds=8,
    parallelism=8
    )

cvModel = cv.fit(train)

# Model summary
bestmodel = cvModel.bestModel
from pyspark.ml.classification import LogisticRegressionSummary
summary = bestmodel.summary
summary.objectiveHistory
accuracy = summary.accuracy
falsePositiveRate = summary.weightedFalsePositiveRate
truePositiveRate = summary.weightedTruePositiveRate
fMeasure = summary.weightedFMeasure()
precision = summary.weightedPrecision
recall = summary.weightedRecall
print("Accuracy: %s\nFPR: %s\nTPR: %s\nF-measure: %s\nPrecision: %s\nRecall: %s"
      % (accuracy, falsePositiveRate, truePositiveRate, fMeasure, precision, recall))


# Evaluation of prediction
predictions = cvModel.transform(test)
evaluator1 = BinaryClassificationEvaluator(metricName="areaUnderROC")
evaluator2 = BinaryClassificationEvaluator(metricName="areaUnderPR")
evaluator3 = MulticlassClassificationEvaluator(predictionCol="prediction", metricName="weightedPrecision")
evaluator4 = MulticlassClassificationEvaluator(predictionCol="prediction", metricName="weightedRecall")
evaluator5 = MulticlassClassificationEvaluator(predictionCol="prediction", metricName="accuracy")
print('Test Area Under ROC', evaluator1.evaluate(predictions))
print('Test Area Under PR', evaluator2.evaluate(predictions))
print('Precision', evaluator3.evaluate(predictions))
print('Recall', evaluator4.evaluate(predictions))
print('accuracy', evaluator5.evaluate(predictions))

# Cross validation results
print('Best Param (regParam): ', bestmodel._java_obj.getRegParam())
print('Best Param (elasticNetParam): ', bestmodel._java_obj.getElasticNetParam())
print('Best Param (aggregationDepth): ', bestmodel._java_obj.getAggregationDepth())
print('Best Param (fitIntercept): ', bestmodel._java_obj.getFitIntercept())
print('Best Param (maxIter): ', bestmodel._java_obj.getMaxIter())

# Audio Similarity Q4 c
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import PCA
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
df = (
    spark.read.format("parquet")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("hdfs:///user/ajo139/outputs/msd/genre_audio.parquet") 
)
df = df.drop('MSD_TRACKID')
# Standardization
from pyspark.ml.linalg import DenseVector # Import DenseVector
input_data = df.rdd.map(lambda x: (x[20], DenseVector(x[:19]))) # Define the input_data 
df = spark.createDataFrame(input_data, ["label0","features0"])
pca = PCA(k=5, inputCol='features0', outputCol='features')
label_stringIdx = StringIndexer(inputCol = 'label0', outputCol = 'label')
pipeline = Pipeline(stages=[pca, label_stringIdx])
pipelineFit = pipeline.fit(df)
data = pipelineFit.transform(df)
data.take(1)
data = data.drop('label0', 'features0')

## Division by stratification
from pyspark.sql.window import Window
temp_data = (
    data
    .withColumn("id", monotonically_increasing_id())
    .withColumn("Random", rand())
    .withColumn(
        "Row",
        row_number()
        .over(
            Window
            .partitionBy("label")
            .orderBy("Random")
        )
    )
)

counts_class = (
    data
    .groupBy("label")
    .count()
    .toPandas()
    .set_index("label")["count"]
    .to_dict()
)
classes = sorted(counts_class.keys())

train = temp_data
for i in classes:
    train = train.where((col("label") != i) | (col("Row") < counts_class[i] * 0.8))

train.cache()

test = temp_data.join(train, on="id", how="left_anti")
test.cache()

train = train.drop("id", "Random", "Row")
test = test.drop("id", "Random", "Row")

# undersampling
from pyspark.sql.functions import countDistinct  
major_df = train.filter(col("label") == 0.0)
minor_df = train.filter(col("label") != 0.0)

def class_balance(data, name):
    N = data.count()
    counts = data.groupBy("label").count().toPandas()
    counts["ratio"] = counts["count"] / N
    print(name)
    print(N)
    print(counts)
    print("")
class_balance(data, "features")

data.agg(countDistinct('label')) # 21
sampled_majority_df = major_df.sample(False, 1/21)
train = sampled_majority_df.unionAll(minor_df)
train.count() # 152989
train.select('label').distinct().show()
train.write.mode('overwrite').parquet('hdfs:///user/ajo139/outputs/msd/train_multiclass.parquet')
test.write.mode('overwrite').parquet('hdfs:///user/ajo139/outputs/msd/test_multiclass.parquet')


# Evaluate the predictions using a Multiclass classifier
evaluator_multiclass = MulticlassClassificationEvaluator(predictionCol="prediction", metricName = "f1")

# Python function to calculate and display the performance metrics for an evaluator
def performance_metrics_multiclass(pred):
  total = pred.count()
  TP = pred.filter((F.col('prediction') == 1) & (F.col('label') == 1)).count()
  FP = pred.filter((F.col('prediction') == 1) & (F.col('label') == 0)).count()
  FN = pred.filter((F.col('prediction') == 0) & (F.col('label') == 1)).count()
  TN = pred.filter((F.col('prediction') == 0) & (F.col('label') == 0)).count()
  f1score = evaluator_multiclass.evaluate(pred, {evaluator_multiclass.metricName: "f1"})
  recall = evaluator_multiclass.evaluate(pred, {evaluator_multiclass.metricName: "weightedRecall"})
  precision = evaluator_multiclass.evaluate(pred, {evaluator_multiclass.metricName: "weightedPrecision"})
  print('Accuracy: {}'.format((TP + TN) / total))
  print('Precision: {}'.format(precision))
  print('Recall: {}'.format(recall))
  print('F1-score: {}'.format((2*precision*recall) / (precision+recall)))
  print("\n")

lr = LogisticRegression(family="multinomial", maxIter=100)
paramGrid = (ParamGridBuilder() 
    .addGrid(lr.regParam, [0.1, 0.3, 0.5])  
    .addGrid(lr.elasticNetParam, [0.6, 0.8, 1.0])
    .addGrid(lr.aggregationDepth,[2,5,10])
    .addGrid(lr.fitIntercept,[False, True])
    .build())

cv = CrossValidator(
    estimator=lr,
    estimatorParamMaps=paramGrid,
    evaluator= evaluator_multiclass,
    numFolds=8,
    parallelism=8
    )

cvModel = cv.fit(train)
predictions = cvModel.transform(test)
performance_metrics_multiclass(predictions)
bestmodel = cvModel.bestModel
from pyspark.ml.classification import LogisticRegressionSummary
summary = bestmodel.summary
print('Best Param (regParam): ', bestmodel._java_obj.getRegParam())
print('Best Param (elasticNetParam): ', bestmodel._java_obj.getElasticNetParam())
print('Best Param (aggregationDepth): ', bestmodel._java_obj.getAggregationDepth())
print('Best Param (fitIntercept): ', bestmodel._java_obj.getFitIntercept())

# https://intellipaat.com/community/5161/how-do-i-add-a-new-column-to-a-spark-dataframe-using-pyspark
# http://www.datasciencemadesimple.com/descriptive-statistics-or-summary-statistics-of-dataframe-in-pyspark/


