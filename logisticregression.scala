import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint


val df = sqlContext.read.json("/home/sanchit/project/yelp/data/yelp_dataset_challenge_academic_dataset/reviewDataSet/spark-scripts/Yelp-Challenge/user_features.json")

val df_category = df.filter("category = \"Restaurants\"").select("average_stars", "cat_avg_review_len", "cat_avg_stars", "cat_business_count", "cat_review_count", "months_yelping", "review_count", "votes_cool", "votes_funny", "votes_useful","elite_cat").withColumnRenamed("elite_cat","label")


val assembler = new VectorAssembler().setInputCols(Array("average_stars", "cat_avg_review_len", "cat_avg_stars", "cat_business_count", "cat_review_count", "months_yelping", "review_count", "votes_cool", "votes_funny", "votes_useful")).setOutputCol("features")
val output = (assembler.transform(df_category)).select("label","features")

val labeled = output.map(row => LabeledPoint(row.getAs[Long](0).doubleValue(), row.getAs(1)))

val splits = labeled.randomSplit(Array(0.7, 0.3), seed = 11L)
val training = splits(0).cache()
val test = splits(1)

// Run training algorithm to build the model
val model = new LogisticRegressionWithLBFGS().setNumClasses(21).run(training)

// Compute raw scores on the test set.
val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
  val prediction = model.predict(features)
  (prediction, label)
}

// Get evaluation metrics.
val metrics = new MulticlassMetrics(predictionAndLabels)

val precision = metrics.precision
println("Precision = " + precision)

// Save and load model
//model.save(sc, "myModelPath")
//val sameModel = LogisticRegressionModel.load(sc, "myModelPath")


