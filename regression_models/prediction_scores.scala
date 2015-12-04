val df_category = df.filter("category = \"Shopping\"").select("user_id","average_stars", "cat_avg_review_len", "cat_avg_stars", "cat_business_count", "cat_review_count", "months_yelping", "review_count", "votes_cool", "votes_funny", "votes_useful","elite")


val assembler = new VectorAssembler().setInputCols(Array("average_stars", "cat_avg_review_len", "cat_avg_stars", "cat_business_count", "cat_review_count", "months_yelping", "review_count", "votes_cool", "votes_funny", "votes_useful")).setOutputCol("features")
val output = (assembler.transform(df_category)).select("user_id","features")


val predictedRDD = output.map { row =>
  val prediction = model.predict(row.getAs[org.apache.spark.mllib.linalg.SparseVector]("features"))
  (row.getString(0), prediction)
}

val predictedDF = predictedRDD.toDF().withColumnRenamed("_1","user_id").withColumnRenamed("_2", "prediction")
println(predictedDF.count())
predictedDF.repartition(1).save("/home/sanchit/project/yelp/data/yelp_dataset_challenge_academic_dataset/reviewDataSet/spark-scripts/Yelp-Challenge/pred_Shopping.json","json")
