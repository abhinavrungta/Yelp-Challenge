from pylab import *
import pandas as pd

import json

categories = set([])
with open('yelp_academic_dataset_business.json') as f:
	for line in f:
		business = json.loads(line)
		cat = business['categories']
		categories = categories.union(set(cat))
		
file = open('categories.txt', 'w')
for item in categories:
	file.write("%s\n" % item)

file.close()
