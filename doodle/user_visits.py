df_review = sqlContext.read.json("/home/sanchit/project/yelp/data/yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_review.json")
df_business = sqlContext.read.json("/home/sanchit/project/yelp/data/yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_business.json")

df_join_reviewAndBusiness = (df_review.select(df_review.business_id,df_review.user_id)).join(df_business.select(df_business.business_id,df_business.latitude,df_business.longitude), df_review.business_id == df_business.business_id).select("user_id","latitude","longitude")

#grouping and count on the basis of user-city pair
df_join_reviewAndBusiness = (df_review.select(df_review.business_id,df_review.user_id)).join(df_business.select(df_business.business_id,df_business.latitude,df_business.longitude,df_business.city), df_review.business_id == df_business.business_id).select("user_id","city","latitude","longitude").groupBy("user_id","city").count()


#df_review.cache()
#df_business.cache()
#df_join_reviewAndBusiness.cache()

#to see the count of places visited by each user

#df_join_reviewAndBusiness.groupBy("user_id").count().show()

