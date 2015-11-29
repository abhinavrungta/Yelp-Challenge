from _functools import partial
import json
import numpy
import os
from pylab import *
import sys

from geopy.distance import vincenty
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, ArrayType, FloatType, StructField, Row
from sklearn.cluster import DBSCAN


def getDistance(x1, y1, x2, y2):
    return vincenty((x1, y1) , (x2, y2)).miles
    
def getLocationsOfUser(userId, business_db):
    locations = business_db[business_db["user_id"] == userId]
    list = []
    for index, row in locations.iterrows():
        list.append(Row(latitude=row['latitude'], longitude=row['longitude']))
    return list        
    
def getCentersOfUser(locations):
    size = len(locations)
    distance_matrix = numpy.zeros((size, size))
    for x in range(0, size):
        for y in range(x + 1, size):
            pointA = locations[x]
            pointB = locations[y]
            dist = getDistance(pointA.latitude, pointA.longitude, pointB.latitude, pointB.longitude)
            distance_matrix[x][y] = dist
            distance_matrix[y][x] = dist
    
    db = DBSCAN(eps=3, min_samples=10, metric='precomputed').fit(distance_matrix)
    
    unique_labels = set(db.labels_)
    cluster_centers = []
    for k in unique_labels:
        if k != -1:
            cluster_points = []    
            for i in range(0, len(db.labels_)):
                if(db.labels_[i] == k):
                    cluster_points.append(locations[i])
            cluster_centers.append(calculateCenter(cluster_points))
    return cluster_centers

def calculateCenter(cluster_points):
    size = len(cluster_points)
    sum_x = 0.0
    sum_y = 0.0
    sum_z = 0.0
    for point in cluster_points:
        lat = point.latitude * math.pi / 180
        longt = point.longitude * math.pi / 180
        
        sum_x = math.cos(lat) * math.cos(longt)
        sum_y = math.cos(lat) * math.sin(longt)
        sum_z = math.sin(lat)
    sum_x /= size
    sum_y /= size
    sum_z /= size
    final_long = math.atan2(sum_y, sum_x)
    final_hyp = math.sqrt(sum_y * sum_y + sum_x * sum_x)
    final_lat = math.atan2(sum_z, final_hyp)
    
    final_lat = final_lat * 180 / math.pi
    final_long = final_long * 180 / math.pi
    
    return Row(latitude=final_lat, longitude=final_long)

class MainApp(object):
    def __init__(self):
        pass
    
    def init(self):
        # os.environ["SPARK_HOME"] = "/Users/abhinavrungta/Desktop/setups/spark-1.5.2"
        # os.environ['AWS_ACCESS_KEY_ID'] = <YOURKEY>
        # os.environ['AWS_SECRET_ACCESS_KEY'] = <YOURKEY>
        conf = SparkConf()
        conf.setMaster("local")
        conf.setAppName("PySparkShell")
        conf.set("spark.executor.memory", "2g")
        # conf.set("spark.driver.memory", "1g")
        self.sc = SparkContext(conf=conf)
        self.sqlContext = SQLContext(self.sc)
        
    def loadData(self):
        self.df_review = self.sqlContext.read.json("../yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_review.json").cache()
        # self.df_review = self.sqlContext.read.json("s3n://ds-emr-spark/data/yelp_academic_dataset_review.json").cache()
        self.df_business = self.sqlContext.read.json("../yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_business.json").cache()
        # self.df_business = self.sqlContext.read.json("s3n://ds-emr-spark/data/yelp_academic_dataset_business.json").cache()
        self.df_review.registerTempTable("reviews")
        self.df_business.registerTempTable("business")
        
    def createCheckInDataPerUser(self):
        review_user = self.sqlContext.sql("SELECT business_id, user_id FROM reviews")
        business_loc = self.sqlContext.sql("SELECT business_id, latitude, longitude FROM business")
        review_user.registerTempTable("reviews_user")
        business_loc.registerTempTable("business_loc")
        
        self.df_join_reviewAndBusiness = self.sqlContext.sql("SELECT r.user_id, b.latitude, b.longitude FROM reviews_user r JOIN business_loc b ON r.business_id = b.business_id").cache()
        self.df_join_reviewAndBusiness.registerTempTable("userBusiness")
        
        self.df_unique_users = self.sqlContext.sql("SELECT DISTINCT user_id FROM userBusiness")
        self.df_unique_users.registerTempTable("users")
        
        pd = self.df_join_reviewAndBusiness.toPandas()
        global_db = self.sc.broadcast(pd)
        
        schema = StructType([
            StructField("latitude", FloatType()),
            StructField("longitude", FloatType())
        ])
        partialFunc = partial(getLocationsOfUser, business_db=global_db.value)
        
        self.get_locations = udf(partialFunc, ArrayType(schema))
        self.get_centers = udf(getCentersOfUser, ArrayType(schema))
    
        self.df_unique_users = self.df_unique_users.withColumn("user_locations", self.get_locations(self.df_unique_users["user_id"]))
        self.df_unique_users.registerTempTable("users")
        
        self.df_unique_users.repartition(1).write.save("user.json", "json", "overwrite")

        self.df_unique_users = self.df_unique_users.withColumn("user_centers", self.get_centers(self.df_unique_users["user_locations"]))
        self.df_unique_users.registerTempTable("users")
        
        self.df_unique_users.repartition(1).write.save("center.json", "json", "overwrite")
        self.df_unique_users.show()
        
    def distanceCalc(self):
        self.df_unique_users = self.sqlContext.read.json("user.json/part-r-00000-23a1b514-f5fe-4f61-9a64-01ebbc88c146").cache()
        getCentersOfUser(self.df_unique_users.toPandas().iloc[0]["user_locations"])

def main():
        app = MainApp()
        app.init()
        app.loadData()
        app.createCheckInDataPerUser()
        # app.distanceCalc()
        

if __name__ == "__main__":  # Entry Point for program.
    sys.exit(main())
