import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.feature.VectorAssembler


val df = sqlContext.read.json("/home/sanchit/project/yelp/data/yelp_dataset_challenge_academic_dataset/reviewDataSet/spark-scripts/Yelp-Challenge/user_features.json")

val df_category = df.filter("category = \"Shopping\"").select("average_stars", "cat_avg_review_len", "cat_avg_stars", "cat_business_count", "cat_review_count", "months_yelping", "review_count", "votes_cool", "votes_funny", "votes_useful","elite").withColumnRenamed("elite","label")


val assembler = new VectorAssembler().setInputCols(Array("average_stars", "cat_avg_review_len", "cat_avg_stars", "cat_business_count", "cat_review_count", "months_yelping", "review_count", "votes_cool", "votes_funny", "votes_useful")).setOutputCol("features")
val output = (assembler.transform(df_category)).select("label","features")

val labeled = output.map(row => LabeledPoint(row.getAs(0), row.getAs(1)))



// Split the data into training and test sets (30% held out for testing)
val splits = labeled.randomSplit(Array(0.7, 0.3))
val (trainingData, testData) = (splits(0), splits(1))


// Train a RandomForest model.
//  Empty categoricalFeaturesInfo indicates all features are continuous.
val numClasses = 21
val categoricalFeaturesInfo = Map[Int, Int]()
val numTrees = 7 // Use more in practice.
val featureSubsetStrategy = "auto" // Let the algorithm choose.
val impurity = "variance"
val maxDepth = 6
val maxBins = 32

val model = RandomForest.trainRegressor(trainingData, categoricalFeaturesInfo,
  numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

// Evaluate model on test instances and compute test error
val labelsAndPredictions = testData.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}
val testMSE = labelsAndPredictions.map{ case(v, p) => math.pow((v - p), 2)}.mean()
println("Test Mean Squared Error = " + testMSE)
//println("Learned regression forest model:\n" + model.toDebugString)
