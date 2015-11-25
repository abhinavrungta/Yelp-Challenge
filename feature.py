import datetime
import json
import os
from pylab import *
import sys

from geopy.distance import vincenty
from pyspark import SparkConf, SparkContext
from pyspark.sql.context import SQLContext
import requests

import pandas as pd

def getNoOfMonths(yelpingSince):
    year_month = yelpingSince.split('-')
    year = int(year_month[0])
    month = int(year_month[1])
    NoOfMonths = year * 12 + month
    
    currentMonth = datetime.datetime.now().month
    currentYear = datetime.datetime.now().year
    currentNoOfMonths = currentYear * 12 + currentMonth
    
    return currentNoOfMonths - NoOfMonths

def getEliteScore(elite):
    currentYear = datetime.datetime.now().year + 1
    sum = 0
    for x in elite:
        sum += x / (currentYear - x)

    return sum

def mapUsers(user):
    result = {}
    result['user_id'] = user[0]
    result['userName'] = user[1].encode('utf-8')
    result['totalReviews'] = user[2]
    votes = user[3]
    result['funny'] = votes[1]
    result['cool'] = votes[0]
    result['useful'] = votes[2]
    result['fans'] = user[4]
    result['yelpingSince'] = getNoOfMonths(user[5])
    result['elite'] = getEliteScore(user[6])
    result['k'] = 0
    return result

class MainApp(object):
    def __init__(self):
        os.environ["SPARK_HOME"] = "/Users/abhinavrungta/Desktop/setups/spark-1.5.2"
        # os.environ['AWS_ACCESS_KEY_ID'] = <YOURKEY>
        # os.environ['AWS_SECRET_ACCESS_KEY'] = <YOURKEY>
        conf = SparkConf()
        conf.setMaster("local")
        conf.setAppName("My application")
        conf.set("spark.executor.memory", "2g")
        self.sc = SparkContext(conf=conf)
        self.sqlContext = SQLContext(self.sc)
        self.df_user = self.sqlContext.read.json("dataset/user.json").cache()
        self.df_review = self.sqlContext.read.json("dataset/review.json").cache()
        self.df_business = self.sqlContext.read.json("dataset/business.json").cache()
        self.df_user.registerTempTable("user")
    
    def getS3File(self, s3FilePath, destinationPathOnLocal):
        r = requests.get(s3FilePath)
        fileOb = open(destinationPathOnLocal, 'w')
        fileOb.write(r.text)
        fileOb.close()
        
    def writeToS3File(self, s3FilePath, sourcePathOnLocal):
        fileOb = open(sourcePathOnLocal, 'r')
        payload = fileOb.read();
        fileOb.close()
        
        headers = {"x-amz-acl" : "public-read-write"}
        return requests.put(s3FilePath, headers=headers, data=payload)
    
    def reads3spark(self, path):
        # path = "s3n://b-datasets/flight_data/*"
        x = self.sc.textFile(path)  # we can just specify all the files.
        return x

    def writes3spark(self, x, path):
        x.saveAsTextFile(path)
        
    def createFeatures(self):
        userData = self.sqlContext.sql("SELECT user_id, name, review_count, votes, fans, yelping_since, elite FROM user")
        userData = userData.map(mapUsers).coalesce(1)
        res = self.sqlContext.createDataFrame(userData)
        
        review_user = self.df_review.select(self.df_review.business_id, self.df_review.user_id)
        business_loc = self.df_business.select(self.df_business.business_id, self.df_business.city, self.df_business.state)
        df_join_reviewAndBusiness = review_user.join(business_loc, review_user.business_id == business_loc.business_id).select("user_id", "city", "state")
        df_grouped = df_join_reviewAndBusiness.groupBy(["user_id", "city", "state"]).count()
        df_panda = res.toPandas()
        for name, group in df_grouped:
            if(group['city'] > 10):
                user_id = df_grouped.get_group(name)[0]['user_id']
                df_panda[user_id]['k'] = df_panda[user_id]['k'] + 1 
        
        res = self.sqlContext.createDataFrame(df_panda)    
        res.toJSON().saveAsTextFile('user_features.json')
        

def main():
        app = MainApp()
        # app.getS3File("http://ds-emr-spark.s3.amazonaws.com/data/yelp_academic_dataset_checkin.json", "user.json")
        # app.writeToS3File("http://ds-yelp-dataset.s3.amazonaws.com/yelp_academic_dataset_checkin.json", "user.json")
        app.createFeatures()

if __name__ == "__main__":  # Entry Point for program.
    sys.exit(main())
