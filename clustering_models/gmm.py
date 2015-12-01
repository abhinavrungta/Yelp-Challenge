import math
import os
import sys

from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.types import Row
from pyspark.storagelevel import StorageLevel
from sklearn import mixture

import numpy as np        
    
def getCentersOfUser(data):
    userId = data[0]
    locations_row = list(data[1])
    size = len(locations_row)
    cluster_centers = []
    if(size < 5):
        return (userId, cluster_centers)
    locations = np.empty([size, 2])
    for x in range(0, size):
        point = locations_row[x]
        locations[x][0] = point.latitude
        locations[x][1] = point.longitude
    
    lowest_bic = np.infty
    bic = []
    n_components_range = range(1, 5)
    cv_types = ['spherical', 'tied', 'diag', 'full']
    for n_components in n_components_range:
        # Fit a mixture of Gaussians with EM
        gmm = mixture.GMM(n_components=n_components, covariance_type='spherical')
        gmm.fit(locations)
        bic.append(gmm.bic(locations))
        if bic[-1] < lowest_bic:
            lowest_bic = bic[-1]
            best_gmm = gmm
    
    centers = best_gmm.means_
    size = len(centers)
    for x in range(0, size):
        cluster_centers.append(Row(latitude=centers[x][0], longitude=centers[x][1]))
    
    return (userId, cluster_centers)

class MainApp(object):
    def __init__(self):
        pass
    
    def init(self):
        os.environ["SPARK_HOME"] = "/Users/abhinavrungta/Desktop/setups/spark-1.5.2"
        # os.environ['AWS_ACCESS_KEY_ID'] = <YOURKEY>
        # os.environ['AWS_SECRET_ACCESS_KEY'] = <YOURKEY>
        conf = SparkConf()
        conf.setMaster("local[10]")
        conf.setAppName("PySparkShell")
        conf.set("spark.executor.memory", "2g")
        conf.set("spark.driver.memory", "1g")
        self.sc = SparkContext(conf=conf)
        self.sqlContext = SQLContext(self.sc)
        
    def loadData(self):
        self.df_review = self.sqlContext.read.json("../yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_review.json")
        # self.df_review = self.sqlContext.read.json("s3n://ds-emr-spark/data/yelp_academic_dataset_review.json").cache()
        self.df_business = self.sqlContext.read.json("../yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_business.json")
        # self.df_business = self.sqlContext.read.json("s3n://ds-emr-spark/data/yelp_academic_dataset_business.json").cache()
        self.df_review.registerTempTable("reviews")
        self.df_business.registerTempTable("business")
        
    def createCheckInDataPerUser(self):
        review_user = self.sqlContext.sql("SELECT business_id, user_id FROM reviews")
        business_loc = self.sqlContext.sql("SELECT business_id, latitude, longitude FROM business")
        review_user.registerTempTable("reviews_user")
        business_loc.registerTempTable("business_loc")
        
        self.df_join_reviewAndBusiness = self.sqlContext.sql("SELECT r.user_id, b.latitude, b.longitude FROM reviews_user r JOIN business_loc b ON r.business_id = b.business_id").rdd.groupBy(lambda x: x.user_id).persist(StorageLevel(True, True, False, True, 1))
        # self.df_join_reviewAndBusiness.repartition(1).saveAsTextFile("user.json")
        self.user_centers = self.df_join_reviewAndBusiness.map(getCentersOfUser, preservesPartitioning = True)
        self.user_centers.repartition(1).saveAsTextFile("center_gmm.json")

def main():
        app = MainApp()
        app.init()
        app.loadData()
        app.createCheckInDataPerUser()

if __name__ == "__main__":  # Entry Point for program.
    sys.exit(main())
