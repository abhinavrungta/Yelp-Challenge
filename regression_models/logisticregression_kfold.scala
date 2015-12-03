import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.Row


val df = sqlContext.read.json("/home/sanchit/project/yelp/data/yelp_dataset_challenge_academic_dataset/reviewDataSet/spark-scripts/Yelp-Challenge/user_features.json")

val df_category = df.filter("category = \"Restaurants\"").select("average_stars", "cat_avg_review_len", "cat_avg_stars", "cat_business_count", "cat_review_count", "months_yelping", "review_count", "votes_cool", "votes_funny", "votes_useful","elite_cat").withColumnRenamed("elite_cat","label")

val assembler = new VectorAssembler().setInputCols(Array("average_stars", "cat_avg_review_len", "cat_avg_stars", "cat_business_count", "cat_review_count", "months_yelping", "review_count", "votes_cool", "votes_funny", "votes_useful")).setOutputCol("features")

val output = (assembler.transform(df_category)).select("label","features")

val labeled = output.map(row => LabeledPoint(row.getAs[Long](0).doubleValue(), row.getAs(1))).toDF()

val splits = labeled.randomSplit(Array(0.7, 0.3), seed = 11L)
val training = splits(0).cache()
val test = splits(1)

val lr = new LogisticRegression().setMaxIter(10)

val pipeline = new Pipeline().setStages(Array(lr))

// We use a ParamGridBuilder to construct a grid of parameters to search over.
// With 3 values for hashingTF.numFeatures and 2 values for lr.regParam,
// this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.
val paramGrid = new ParamGridBuilder().addGrid(lr.regParam, Array(0.1, 0.01)).build()

// We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
// This will allow us to jointly choose parameters for all Pipeline stages.
// A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
// Note that the evaluator here is a BinaryClassificationEvaluator and its default metric
// is areaUnderROC.
val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(new MulticlassClassificationEvaluator).setEstimatorParamMaps(paramGrid).setNumFolds(2) // Use 3+ in practice

// Run cross-validation, and choose the best set of parameters.
val cvModel = cv.fit(training)

// Make predictions on test documents. cvModel uses the best model found (lrModel).
cvModel.transform(test).select("id", "text", "probability", "prediction").collect().foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>println(s"($id, $text) --> prob=$prob, prediction=$prediction")
}
