%pyspark 
from pyzipcode import ZipCodeDatabase
zcdb = ZipCodeDatabase()
zip = z["zipcode"]
zipcode = zcdb[zip]
cat = z["category"]
