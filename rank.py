import os
import sys
os.environ['WORKDIR'] = "/home/ec2-user/Yelp-Challenge/"
os.environ["SPARK_HOME"] = "/home/ec2-user/spark-1.5.2/"
sys.path.append(os.environ["SPARK_HOME"] + "python")
sys.path.append(os.environ["SPARK_HOME"] + "python/lib/py4j-0.8.2.1-src.zip")        

from _functools import partial

from geopy.distance import vincenty
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.types import StructType, StructField, FloatType, StringType, \
    ArrayType
from pyspark.sql import functions as F
from os.path import isfile, join
import re


def getDistance(x1, y1, x2, y2):
    return vincenty((x1, y1) , (x2, y2)).miles

def isUserlocal(input_row, latitude, longitude):
    user_locations = input_row.cluster_centers
    for center in user_locations:
        if getDistance(latitude, longitude, center.latitude, center.longitude) < 5.0:
            return True
    return False

def isBusinessLocalAndRelevant(input_row, latitude, longitude, sub_categories):
    if getDistance(latitude, longitude, input_row.latitude, input_row.longitude) > 5.0:
        return False
    categories = input_row.categories
    for x in categories:
        if x in sub_categories:
            return True
    return False

class MainApp(object):
    def __init__(self, cat, lat, longt):
        self.category = cat
        self.loc_lat = lat
        self.loc_long = longt
        print(longt)
        print(lat)
        pass
    
    def init(self):
        # os.environ['AWS_ACCESS_KEY_ID'] = <YOURKEY>
        # os.environ['AWS_SECRET_ACCESS_KEY'] = <YOURKEY>
        conf = SparkConf()
        conf.setMaster("local[10]")
        conf.setAppName("PySparkShell")
        #conf.set("spark.executor.memory", "2g")
        #conf.set("spark.driver.memory", "1g")
        #self.sc = sc
        #self.sqlContext = sqlContext
        self.sc = SparkContext(conf=conf)
        self.sqlContext = SQLContext(self.sc)
        
    def loadData(self):
        category_list = self.sc.textFile(os.environ['WORKDIR'] + "yelp_dataset_challenge_academic_dataset/cat_subcat.csv").map(lambda line: (line.split(',')[0], line.split(',')))
        category_schema = StructType([
            StructField("category", StringType(), True),
            StructField("sub_category", ArrayType(StringType()), True)
        ])
        # self.category_list.registerTempTable("categories_list")
        # subcat = self.sqlContext.sql("SELECT sub_category FROM categories_list WHERE category = \"{0}\" LIMIT 1".format(self.category))
        category_list = self.sqlContext.createDataFrame(category_list, category_schema)
        subcat = category_list.where(category_list.category == self.category).first().sub_category

        self.df_business = self.sqlContext.read.json(os.environ['WORKDIR'] + "yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_business.json")
        # self.df_business = self.sqlContext.read.json("s3n://ds-emr-spark/data/yelp_academic_dataset_business.json").cache()
        self.df_business = self.df_business.select("business_id", "name", "stars", "latitude", "longitude", "categories")

        filter_business = partial(isBusinessLocalAndRelevant, latitude = self.loc_lat, longitude = self.loc_long, sub_categories = subcat)
        self.df_business = self.df_business.rdd.filter(filter_business)
        self.df_business = self.sqlContext.createDataFrame(self.df_business)
        self.df_business = self.df_business.select("business_id", "name", "stars", "latitude", "longitude")
        self.df_business.registerTempTable("business")

        schema_2 = StructType([
            StructField("latitude", FloatType(), True),
            StructField("longitude", FloatType(), True)
        ])
        
        schema = StructType([
            StructField("cluster_centers", ArrayType(schema_2), True),
            StructField("sl_score", FloatType(), True),
            StructField("user_id", StringType(), True)
        ])

        self.df_user_locations = self.sqlContext.read.json(os.environ['WORKDIR'] + "clustering_models/center.json/dbscan", schema)
        filter_users = partial(isUserlocal, latitude = self.loc_lat, longitude = self.loc_long)
        self.df_user_locations = self.df_user_locations.rdd.filter(filter_users)
        self.df_user_locations = self.sqlContext.createDataFrame(self.df_user_locations)
        self.df_user_locations = self.df_user_locations.select("user_id")
        self.df_user_locations.registerTempTable("user")
        #print "user locations: ", self.self.df_user_locations.count()

        self.df_review = self.sqlContext.read.json(os.environ['WORKDIR'] + "yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_review.json")
        self.df_review = self.df_review.select("business_id", "user_id", "stars")
        self.df_review.registerTempTable("review")
        #print "reviews: ", self.self.df_review.count()

        self.df_joined = self.sqlContext.sql("SELECT r.user_id AS user_id, r.business_id AS business_id, first(b.name) AS business_name, first(b.stars) AS business_stars, first(b.latitude) AS latitude, first(b.longitude) AS longitude, avg(r.stars) AS avg_rev_stars FROM review r, business b, user u WHERE r.business_id = b.business_id AND r.user_id = u.user_id GROUP BY r.user_id, r.business_id")
        self.df_joined.registerTempTable("joined")
        
        self.df_business.unpersist()
        self.df_user_locations.unpersist()
        self.df_review.unpersist()

        self.df_category_pred = self.loadEliteScorePredictionsForCategory()
        self.df_category_pred.registerTempTable("prediction")
        
        self.df_joined = self.sqlContext.sql("SELECT j.*, p.prediction AS elite_score, (j.avg_rev_stars*p.prediction) AS w_score FROM joined j, prediction p WHERE j.user_id = p.user_id") 
        #print "joined: ", self.self.df_joined.count()
        #self.self.df_joined.show()

        self.df_category_pred.unpersist()

        df_grouped = self.df_joined.groupBy("business_id", "business_name", "business_stars", "latitude", "longitude").agg(F.avg("w_score").alias("rank"))
        df_grouped = df_grouped.sort("rank", ascending=False)
        print df_grouped.count()
        df_grouped.show()

        # self.df_joined.unpersist()

        return df_grouped.take(10)

    def loadEliteScorePredictionsForCategory(self):
        fileloc = "regression_models/"
        filename = "pred_" + re.sub(" ", "_", self.category.lower()) + ".json" 

        category_file = os.environ['WORKDIR'] + join(fileloc, filename)
        #print category_file
        if isfile(category_file):
            df_category_pred = self.sqlContext.read.json(category_file)
            #print self.df_category_pred.count()
            #self.df_category_pred.show()
            return df_category_pred


    def createCheckInDataPerUser(self):
        pass

if __name__ == "__main__":
    app = MainApp("Restaurants", 35.222406, -80.79221)
    app.init()
    app.loadData()

