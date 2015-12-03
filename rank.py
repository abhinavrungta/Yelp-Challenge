from _functools import partial
import os
import sys

from geopy.distance import vincenty
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.types import StructType, StructField, FloatType, StringType, \
    ArrayType


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
    def __init__(self):
        self.category = "Restaurants"
        self.loc_lat = 45.520832
        self.loc_long = -73.57291
        pass
    
    def init(self):
        #os.environ["SPARK_HOME"] = "/Users/abhinavrungta/Desktop/setups/spark-1.5.2"
        # os.environ['AWS_ACCESS_KEY_ID'] = <YOURKEY>
        # os.environ['AWS_SECRET_ACCESS_KEY'] = <YOURKEY>
        conf = SparkConf()
        #conf.setMaster("local[10]")
        #conf.setAppName("PySparkShell")
        #conf.set("spark.executor.memory", "2g")
        #conf.set("spark.driver.memory", "1g")
        self.sc = sc
        self.sqlContext = sqlContext
        # self.sc = SparkContext(conf=conf)
        # self.sqlContext = SQLContext(self.sc)
        
    def loadData(self):
        self.category_list = self.sc.textFile(os.environ['WORKDIR'] + "yelp_dataset_challenge_academic_dataset/cat_subcat.csv").map(lambda line: (line.split(',')[0], line.split(',')))
        category_schema = StructType([
            StructField("category", StringType(), True),
            StructField("sub_category", ArrayType(StringType()), True)
        ])
        # self.category_list.registerTempTable("categories_list")
        # subcat = self.sqlContext.sql("SELECT sub_category FROM categories_list WHERE category = \"{0}\" LIMIT 1".format(self.category))
        self.category_list = self.sqlContext.createDataFrame(self.category_list, category_schema)
        subcat = self.category_list.where(self.category_list.category == self.category).first().sub_category
        
        self.df_business = self.sqlContext.read.json(os.environ['WORKDIR'] + "yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_business.json")
        # self.df_business = self.sqlContext.read.json("s3n://ds-emr-spark/data/yelp_academic_dataset_business.json").cache()
        self.df_business = self.df_business.select("business_id", "latitude", "longitude", "categories")

        filter_business = partial(isBusinessLocalAndRelevant, latitude = self.loc_lat, longitude = self.loc_long, sub_categories = subcat)
        self.df_business = self.df_business.rdd.filter(filter_business)
        self.df_business = self.sqlContext.createDataFrame(self.df_business)
        self.df_business = self.df_business.select("business_id")
        self.df_business.registerTempTable("business")
        #print "business: ", self.df_business.count()

        schema_2 = StructType([
            StructField("latitude", FloatType(), True),
            StructField("longitude", FloatType(), True)
        ])
        
        schema = StructType([
            StructField("cluster_centers", ArrayType(schema_2), True),
            StructField("user_id", StringType(), True)
        ])

        self.df_user_locations = self.sqlContext.read.json(os.environ['WORKDIR'] + "clustering_models/center_gmm.json/gmm", schema)
        filter_users = partial(isUserlocal, latitude = self.loc_lat, longitude = self.loc_long)
        self.df_user_locations = self.df_user_locations.rdd.filter(filter_users)
        self.df_user_locations = self.sqlContext.createDataFrame(self.df_user_locations)
        self.df_user_locations = self.df_user_locations.select("user_id")
        self.df_user_locations.registerTempTable("user")
        #print "user locations: ", self.df_user_locations.count()

        self.df_review = self.sqlContext.read.json(os.environ['WORKDIR'] + "yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_review.json")
        self.df_review = self.df_review.select("business_id", "user_id", "stars")
        self.df_review.registerTempTable("review")
        #print "reviews: ", self.df_review.count()

        self.df_joined = self.sqlContext.sql("SELECT r.user_id AS user_id, r.business_id AS business_id, avg(r.stars) AS avg_stars FROM review r, business b, user u WHERE r.business_id = b.business_id AND r.user_id = u.user_id GROUP BY r.user_id, r.business_id")
        #print "joined: ", joinedDF.count()
        #df_joined.show()
    
    def createCheckInDataPerUser(self):
        pass


app = MainApp()
app.init()
app.loadData()
app.createCheckInDataPerUser()
