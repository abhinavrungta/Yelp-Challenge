# run this code in zeppelin

%pyspark
checkin_file="/home/yogesh/datascience/project/yelp/yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_checkin.json"
checkin_df = sqlContext.read.json(checkin_file)
checkin_df.registerTempTable("checkin")


%pyspark
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import IntegerType, DoubleType

#df.printSchema()
def totalCheckIns(checkins):
    sum = 0
    for v in checkins:
        if v != None:
            sum += int(v)
    return sum
    
totalCheckIns = UserDefinedFunction(totalCheckIns, IntegerType())

checkin_df = checkin_df.withColumn("tc", totalCheckIns(df["checkin_info"]))
checkin_df.registerTempTable("checkin")

df.show()


%pyspark
business_file="/home/yogesh/datascience/project/yelp/yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_business.json"
business_df = sqlContext.read.json(business_file)
business_df.registerTempTable("business")


%sql
select b.business_id, b.stars as avg_rating, c.tc as num_checkins from business as b, checkin as c where b.business_id = c.business_id
# refer to figure business_avg_rating_vs_avg_checkins.png