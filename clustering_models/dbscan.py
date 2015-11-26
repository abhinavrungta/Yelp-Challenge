import json
import numpy
import os
from pylab import *
import sys

from geopy.distance import vincenty
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import StructType, ArrayType, FloatType, StructField, Row
from sklearn.cluster import DBSCAN

def getDistance(x1, y1, x2, y2):
    return vincenty((x1, y1) , (x2, y2)).miles
    
def getLocationsOfUser(business_db, userId):
    locations = business_db[business_db["user_id"] == userId]
    list = []
    for index, row in locations.iterrows():
        list.append(Row(latitude=row['latitude'], longitude=row['longitude']))
    return list
    # self.sqlContext.sql("SELECT latitude, longitude FROM userBusiness where user_id = \"{0}\"".format(userId)).collect()        
    
def getCentersOfUser(self, locations):
    size = locations.length
    distance_matrix = numpy.zeros((size, size))
    for x in range(0, size - 1):
        for y in range(x + 1, size - 1):
            pointA = locations[x]
            pointB = locations[y]
            distance_matrix[x][y] = getDistance(pointA.latitude, pointA.longitude, pointB.latitude, pointB.longitude)

    # list.append(Row(latitude=row['latitude'], longitude=row['longitude']))            
    db = DBSCAN(eps=3, min_samples=10, metric='precomputed').fit(distance_matrix)
    return db.components_

class MainApp(object):
    def __init__(self):
        pass
    
    def init(self):
        os.environ["SPARK_HOME"] = "/Users/abhinavrungta/Desktop/setups/spark-1.5.2"
        # os.environ['AWS_ACCESS_KEY_ID'] = <YOURKEY>
        # os.environ['AWS_SECRET_ACCESS_KEY'] = <YOURKEY>
        conf = SparkConf()
        conf.setMaster("local")
        conf.setAppName("PySparkShell")
        conf.set("spark.executor.memory", "3g")
        self.sc = SparkContext(conf=conf)
        self.sqlContext = SQLContext(self.sc)
        
    def init2(self):
        schema = StructType([
            StructField("latitude", FloatType()),
            StructField("longitude", FloatType())
    ])
        self.get_locations = UserDefinedFunction(getLocationsOfUser, ArrayType(schema))
        self.get_centers = UserDefinedFunction(getCentersOfUser, ArrayType(schema))
        
    def loadData(self):
        self.df_review = self.sqlContext.read.json("../yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_review.json").cache()
        self.df_business = self.sqlContext.read.json("../yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_business.json").cache()
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
    
        self.df_unique_users.withColumn("user_locations", self.get_locations(global_db.value, self.df_unique_users["user_id"]))
        self.df_unique_users.registerTempTable("users")
        
        self.df_unique_users.repartition(1).save("user.json", "json")

        self.df_unique_users.withColumn("user_centers", self.get_centers(self.df_unique_users["user_locations"]))
        self.df_unique_users.registerTempTable("users")
        
        self.df_unique_users.repartition(1).save("center.json", "json")
        self.df_unique_users.show()

def main():
        app = MainApp()
        app.init()
        app.init2()
        app.loadData()
        app.createCheckInDataPerUser()
        # app.createFeatures()

if __name__ == "__main__":  # Entry Point for program.
    sys.exit(main())
