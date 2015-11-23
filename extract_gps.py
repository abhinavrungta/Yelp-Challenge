df_count = df_review.groupBy("user_id").count()

df_count.where("count > 50").select("user_id")

df_businessId = df_review.where("user_id = \"kGgAARL2UmvCcTRfiscjug\"").select("business_id")

df_gps = df_businessId.join(df_business,df_businessId.business_id == df_business.business_id).select("longitude","latitude")


#to save the dataframe in csv format in one file
df_gps.rdd.map(lambda r: ",".join([str(c) for c in r])).repartition(1).saveAsTextFile("/home/sanchit/project/yelp/data/yelp_dataset_challenge_academic_dataset/reviewDataSet/spark-scripts/Yelp-Challenge/gps_coordinats.txt")

