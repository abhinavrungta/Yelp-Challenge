from _functools import partial
import os
import sys

from geopy.distance import vincenty
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, FloatType, StringType, ArrayType

os.environ['WORKDIR'] = "/home/ec2-user/Yelp-Challenge/"


def getDistance(x1, y1, x2, y2):
    return vincenty((x1, y1) , (x2, y2)).miles

def isUserlocal(input_row, latitude, longitude):
    user_locations = input_row.cluster_centers
    for center in user_locations:
        if getDistance(latitude, longitude, center.latitude, center.longitude) < 0.5:
            return True
    return False

def isBusinessLocalAndRelevant(input_row, latitude, longitude, sub_categories):
    if getDistance(latitude, longitude, input_row.latitude, input_row.longitude) > 0.5:
        return False
    categories = input_row.categories
    for x in categories:
        if x in sub_categories:
            return True
    return False

class MainApp(object):
    def __init__(self):
        self.category = "Restaurants"
        self.loc_lat = 45.520832
        self.loc_long = -73.57291
        pass
    
    def init(self):
        #os.environ["SPARK_HOME"] = "/Users/abhinavrungta/Desktop/setups/spark-1.5.2"
        # os.environ['AWS_ACCESS_KEY_ID'] = <YOURKEY>
        # os.environ['AWS_SECRET_ACCESS_KEY'] = <YOURKEY>
        #conf = SparkConf()
        #conf.setMaster("local[10]")
        #conf.setAppName("PySparkShell")
        #conf.set("spark.executor.memory", "2g")
        #conf.set("spark.driver.memory", "1g")
        #self.sc = SparkContext(conf=conf)
        #self.sqlContext = SQLContext(self.sc)
        self.sqlContext = sqlContext
        self.sc = sc
	    
    def loadData(self):
        self.category_list = self.sc.textFile(os.environ['WORKDIR']+"yelp_dataset_challenge_academic_dataset/cat_subcat.csv").map(lambda line: (line.split(',')[0], line.split(',')))
        category_schema = StructType([
            StructField("category", StringType(), True),
            StructField("sub_category", ArrayType(StringType()), True)
        ])
        # self.category_list.registerTempTable("categories_list")
        # subcat = self.sqlContext.sql("SELECT sub_category FROM categories_list WHERE category = \"{0}\" LIMIT 1".format(self.category))
        self.category_list = self.sqlContext.createDataFrame(self.category_list, category_schema)
        subcat = self.category_list.where(self.category_list.category == self.category).first().sub_category
        
        self.df_business = self.sqlContext.read.json(os.environ['WORKDIR']+"/yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_business.json")
        # self.df_business = self.sqlContext.read.json("s3n://ds-emr-spark/data/yelp_academic_dataset_business.json").cache()
        self.df_business = self.df_business.select("business_id", "latitude", "longitude", "categories")

        filter_business = partial(isBusinessLocalAndRelevant, latitude = self.loc_lat, longitude = self.loc_long, sub_categories = subcat)
        self.df_business = self.df_business.rdd.filter(filter_business)
        self.df_business = self.sqlContext.createDataFrame(self.df_business)
        self.df_business = self.df_business.select(self.df_business.business_id)

        schema_2 = StructType([
            StructField("latitude", FloatType(), True),
            StructField("longitude", FloatType(), True)
        ])
        
        schema = StructType([
            StructField("cluster_centers", ArrayType(schema_2), True),
            StructField("user_id", StringType(), True)
        ])

        self.user_locations = self.sqlContext.read.json(os.environ['WORKDIR']+"clustering_models/center_gmm.json/gmm", schema)
        filter_users = partial(isUserlocal, latitude = self.loc_lat, longitude = self.loc_long)
        self.user_locations = self.user_locations.rdd.filter(filter_users)
        self.user_locations = self.sqlContext.createDataFrame(self.user_locations)
        self.user_locations = self.user_locations.select(self.user_locations.user_id)

        self.df_review = self.sqlContext.read.json(os.environ['WORKDIR']+"yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_review.json")
        # self.joined = self.df_review.join(self.user_locations)
        # self.joined.registerTempTable("reviews")
        # self.joined = self.sqlContext.sql("SELECT user_id, business_id, AVG(stars) AS avg_rating FROM reviews GROUP BY user_id")
        # print(self.joined.take(2))
    
    def createCheckInDataPerUser(self):
        pass

from pyspark.sql.types import *
from pyspark.sql import functions as func
import matplotlib.pyplot as plt
import pandas as pd
import pylab
import os

app = MainApp()
app.init()
app.loadData()
app.createCheckInDataPerUser()

df_userLocs = app.user_locations
df_businessLocs =  app.df_business

df_userFeatures = sqlContext.read.json(os.environ['WORKDIR'] + "user_features.json")
df_reviews  = sqlContext.read.json(os.environ['WORKDIR'] + "yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_review.json")
df_business = sqlContext.read.json(os.environ['WORKDIR'] + "yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_business.json")

df_finalBusiness = df_business.join(df_businessLocs,df_business.business_id == df_businessLocs.business_id).select(df_business.business_id,df_business.stars)


df_joinBusinessLocsAndReviews = df_businessLocs.join(df_reviews,df_businessLocs.business_id == df_reviews.business_id).select(df_reviews.user_id,df_reviews.business_id,df_reviews.stars)


df_finalUsersBusinessRating =  df_joinBusinessLocsAndReviews.join(df_userLocs,df_userLocs.user_id == df_joinBusinessLocsAndReviews.user_id).select(df_joinBusinessLocsAndReviews.business_id,df_joinBusinessLocsAndReviews.stars).groupBy("business_id").agg(func.avg("stars").alias('avg_rating'))

df = df_finalUsersBusinessRating.join(df_finalBusiness, df_finalUsersBusinessRating.business_id == df_finalBusiness.business_id).select(df_finalUsersBusinessRating.business_id, "stars","avg_rating")

pdf = df.toPandas()

pdf.plot(x='business_id',y='avg_rating',color='y',label='avg_rating_by_users')
pdf.plot(x='business_id',y='stars',color='r',label='business_rating')
plt.legend(loc='lower left', fontsize=20)
pylab.show()
