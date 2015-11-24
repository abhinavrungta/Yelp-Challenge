import datetime
import math
import os
import sys

from pyspark import *
from pyspark.sql import SQLContext
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import IntegerType, DoubleType

import pandas as pd


def getEliteScore(elite):
    currentYear = datetime.datetime.now().year + 1.0
    sum = 0.0
    base = 0.0
    for x in elite:
        base += x
        sum += x / (currentYear - x)
    if base == 0:
        return 0.0
    return sum / base

def getEliteCategory(elite):
    return int(math.floor(elite / 0.05))

class MainApp(object):
    def __init__(self):
        # os.environ["SPARK_HOME"] = "/Users/abhinavrungta/Desktop/setups/spark-1.5.2"
        config = SparkConf()
        # self.awsAccessKeyId="<awsAccessKeyId>"
        # self.awsSecretAccessKey="<awsSecretAccessKey>"
        # config.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        # config.set("fs.s3n.awsAccessKeyId", self.awsAccessKeyId)
        # config.set("fs.s3n.awsSecretAccessKey", self.awsSecretAccessKey)

        self.sc = SparkContext(conf=config)
        self.sqlContext = SQLContext(self.sc)
        self.elite_score = UserDefinedFunction(getEliteScore, DoubleType())
        self.elite_cat = UserDefinedFunction(getEliteCategory, IntegerType())

        self.bucketName = 'ds-emr-spark'
    

    def createFeatures(self):
        self.loadBusinessData()
        self.loadReviewData()
        self.loadUserData()

        self.joinData()
        

    def loadJsonDataAsPandasDF(self, filename):
        with open(filename, 'rb') as f:
            data = f.readlines()

        # remove the trailing "\n" from each line
        data = map(lambda x: x.rstrip(), data)

        # each element of 'data' is an individual JSON object.
        # i want to convert it into an *array* of JSON objects
        # which, in and of itself, is one large JSON objectfr
        # basically... add square brackets to the beginning
        # and end, and have all the individual business JSON objects
        # separated by a comma
        data_json_str = "[" + ','.join(data) + "]"

        # now, load it into pandas dataframe and return it
        return pd.read_json(data_json_str)


    def loadJsonDataAsSparkDF(self, filename):
        # df = sqlContext.read.json("s3n://"+self.awsAccessKeyId+":"+self.awsSecretAccessKey+"@"+self.bucketName+"/"+fileName)
        df = self.sqlContext.read.json(filename)
        return df


    def loadCategories(self, filename):
        catSubCatDict = {}
        subCatCatDict = {}
        for line in open(filename):
            key, value = line.rstrip().split(":")
            valueList = []
            for v in value.split(","):
                subCatCatDict[v] = key
                valueList.extend(v)
            catSubCatDict[key] = valueList

        return catSubCatDict, subCatCatDict


    ''' 
    df = dataframe to split,
    subCatCatDict = dictionary containing sub-category to category mapping
    target_column = the column containing the values to split
    new_column = the name of column which will contain value after splitting target_column

    returns: a dataframe with each entry for the target column separated, with each element moved into a new row. 
    The values in the other columns are duplicated across the newly divided rows.
    '''
    def splitDataFrameList(self, df, subCatCatDict, target_column, new_column):
        
        def splitListToRows(row, row_accumulator, subCatCatDict, target_column, new_column):
            split_row = row[target_column]
            for s in split_row:
                if(s in subCatCatDict): 
                    new_row = row.to_dict()
                    new_row[new_column] = subCatCatDict.get(s)
                    row_accumulator.append(new_row)
        
        new_rows = []
        df.apply(splitListToRows, axis=1, args=(new_rows, subCatCatDict, target_column, new_column))
        new_df = pd.DataFrame(new_rows)
        return new_df


    def loadBusinessData(self):
        catSubCatDict, subCatCatDict = self.loadCategories("yelp_dataset_challenge_academic_dataset/cat_subcat.csv")
        # print catSubCatDict
        # print subCatList
        
        self.business = self.loadJsonDataAsPandasDF("yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_business.json")
        self.business = self.splitDataFrameList(self.business[['business_id', 'categories']], subCatCatDict, 'categories', 'category')
        # print business.head()
        
        self.business = self.sqlContext.createDataFrame(self.business)
        self.business.registerTempTable("business")
        # print business.count()

        # business = sqlContext.sql("SELECT DISTINCT business_id, categories FROM business WHERE category IN (" + str(subCatList).strip("[]") + ")")
        self.busines = self.sqlContext.sql("SELECT DISTINCT business_id, category FROM business")
        # print "number of businesses: ", business.count()


    def loadReviewData(self):
        self.review = self.loadJsonDataAsSparkDF("yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_review.json")
        self.review.registerTempTable("review")
        # print "number of reivews: ", review.count()
            
        self.review = self.sqlContext.sql("SELECT r.*, length(r.text) AS review_len FROM review AS r")
        self.review.registerTempTable("review")
        # review.printSchema()


    def loadUserData(self):
        self.user = self.loadJsonDataAsSparkDF("yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_user.json")
        self.user.registerTempTable("user")
        
        self.user = self.sqlContext.sql("SELECT u.*, floor(months_between(current_date(), to_date(u.yelping_since))) \
        AS months_yelping FROM user u")
        self.user.registerTempTable("user")

        self.user = self.user.withColumn("elite", self.elite_score(self.user["elite"]))
        self.user.registerTempTable("user")
        self.user = self.user.withColumn("elite_cat", self.elite_cat(self.user["elite"]))
        self.user.registerTempTable("user")
        # d.printSchema()
        # print "number of users: ", user.count()
        
        
    def joinData(self):
        businessReview = self.sqlContext.sql("SELECT r.*, b.category FROM business AS b, review AS r \
        WHERE b.business_id = r.business_id")
        businessReview.registerTempTable("business_review")
        # print businessReview.count()
        # print businessReview.printSchema()
        # print businessReview.show()
        
        businessReviewAgg = self.sqlContext.sql("SELECT br.user_id, br.category, COUNT(DISTINCT br.business_id) AS cat_business_count, \
        COUNT(*) AS cat_review_count, AVG(br.review_len) AS cat_avg_review_len, AVG(br.stars) AS cat_avg_stars \
        FROM business_review AS br GROUP BY br.user_id, br.category")
        businessReviewAgg.registerTempTable("business_review_agg")
        
        # print businessReviewAgg.count()
        # print businessReviewAgg.show()
        
        self.userAgg = self.sqlContext.sql("SELECT u.user_id, u.name, u.review_count, u.average_stars, \
            u.votes.cool AS votes_cool, u.votes.funny AS votes_funny, u.votes.useful AS votes_useful, \
            u.months_yelping, u.elite, u.elite_cat, br.category, br.cat_business_count, br.cat_review_count, \
            br.cat_avg_review_len, br.cat_avg_stars \
            FROM business_review_agg AS br, user AS u WHERE br.user_id = u.user_id")
        # print userAgg.count()
        # print userAgg.show()


    def showSchema(self):
        self.userAgg.printSchema()
        self.userAgg.show()


    def writeToFile(self):    
        self.userAgg.repartition(1).save("user_features.json","json")


def main():
    app = MainApp()
    app.createFeatures()
    app.showSchema()
    app.writeToFile()

if __name__ == "__main__":  # Entry Point for program.
    sys.exit(main())
