from pyspark.mllib.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator

df = sqlContext.read.json("/home/sanchit/project/yelp/data/yelp_dataset_challenge_academic_dataset/reviewDataSet/spark-scripts/Yelp-Challenge/user_features.json")

df_restaurants = df.filter("category = \"Restaurants\"")


assembler = VectorAssembler(
    inputCols=["average_stars", "cat_avg_review_len", "cat_avg_stars", "cat_business_count", "cat_review_count", "months_yelping", "review_count", "votes_cool", "votes_funny", "votes_useful" ],
    outputCol="features")
output = assembler.transform(df_restaurants)

(trainingData, testData) = output.randomSplit([0.7, 0.3])

dt = DecisionTreeRegressor(labelCol = "elite", featuresCol="features")
pipeline = Pipeline(stages=[dt])
model = pipeline.fit(trainingData)
predictions = model.transform(testData)

predictions.select("prediction", "elite", "features").show(5)


evaluator = RegressionEvaluator(
    labelCol="elite", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print "Root Mean Squared Error (RMSE) on test data = %g" % rmse




