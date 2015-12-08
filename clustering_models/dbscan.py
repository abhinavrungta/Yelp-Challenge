import os
os.environ['WORKDIR'] = "/home/ec2-user/Yelp-Challenge/"
import math

from geopy.distance import vincenty
from pyspark.sql.types import StructType, StructField, FloatType, StringType, \
    ArrayType, Row
from pyspark.storagelevel import StorageLevel
from sklearn.cluster import DBSCAN
from sklearn import metrics


def getDistance(x1, y1, x2, y2):
    return vincenty((x1, y1) , (x2, y2)).miles        
    
def getCentersOfUser(data):
    userId = data[0]
    locations = list(data[1])
    size = len(locations)
    distance_matrix = [[0.0 for x in range(size)] for x in range(size)]
    for x in range(0, size):
        for y in range(x + 1, size):
            pointA = locations[x]
            pointB = locations[y]
            dist = getDistance(pointA.latitude, pointA.longitude, pointB.latitude, pointB.longitude)
            distance_matrix[x][y] = dist
            distance_matrix[y][x] = dist
    
    db = DBSCAN(eps=3, min_samples=5, metric='precomputed')
    y = db.fit_predict(distance_matrix)
    sl_score = 0.0
    if len(set(y)) >= 2:
        sl_score = metrics.silhouette_score(distance_matrix, y, metric="precomputed")
        
    unique_labels = set(db.labels_)
    cluster_centers = []
    for k in unique_labels:
        if k != -1:
            cluster_points = []    
            for i in range(0, len(db.labels_)):
                if(db.labels_[i] == k):
                    cluster_points.append(locations[i])
            cluster_centers.append(calculateCenter(cluster_points))        
    return (cluster_centers, sl_score, str(userId))

def calculateCenter(cluster_points):
    size = len(cluster_points)
    sum_x = 0.0
    sum_y = 0.0
    sum_z = 0.0
    for point in cluster_points:
        lat = point.latitude * math.pi / 180
        longt = point.longitude * math.pi / 180
        
        sum_x += math.cos(lat) * math.cos(longt)
        sum_y += math.cos(lat) * math.sin(longt)
        sum_z += math.sin(lat)
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
        # conf = SparkConf()
        # conf.setMaster("local[10]")
        # conf.setAppName("PySparkShell")
        # conf.set("spark.executor.memory", "2g")
        # conf.set("spark.driver.memory", "1g")
        # self.sc = SparkContext(conf=conf)
        self.sc = sc
        # self.sqlContext = SQLContext(self.sc)
        sqlf.sqlContext = sqlContext
        
    def loadData(self):
        self.df_review = self.sqlContext.read.json(os.environ['WORKDIR'] + "yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_review.json")
        # self.df_review = self.sqlContext.read.json("s3n://ds-emr-spark/data/yelp_academic_dataset_review.json").cache()
        self.df_business = self.sqlContext.read.json(os.environ['WORKDIR'] + "yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_business.json")
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
        self.user_centers = self.df_join_reviewAndBusiness.map(getCentersOfUser, preservesPartitioning=True)
        
        schema_2 = StructType([
            StructField("latitude", FloatType(), True),
            StructField("longitude", FloatType(), True)
        ])
        
        schema = StructType([
            StructField("cluster_centers", ArrayType(schema_2), True),
            StructField("sl_score", FloatType(), True),
            StructField("user_id", StringType(), True)
        ])
        df = self.sqlContext.createDataFrame(self.user_centers.repartition(1), schema)
        df.save("center.json", "json")
        score = df.mean('sl_score')
        print(score)

if __name__ == "__main__":
    app = MainApp()
    app.init()
    app.loadData()
    app.createCheckInDataPerUser()
