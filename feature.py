import sys
from pylab import *
import pandas as pd
import json
from geopy.distance import vincenty
import math
from datetime import datetime

class MainApp(object):
    def __init__(self):
        pass
    
    def createFeatures(self):
        user_features = open('user_features.txt', 'w')
        with open('yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_user.json') as f:
            for line in f:
                user = json.loads(line)
                user_id = user['user_id']
                user_name = user['name'].encode('utf-8')
                total_reviews = user['review_count']
                votes = user['votes']
                votes_funny = votes['funny'] if 'funny' in votes.keys() else 0
                votes_cool = votes['cool'] if 'cool' in votes.keys() else 0
                votes_useful = votes['useful'] if 'useful' in votes.keys() else 0
                fans = user['fans']
                yelping_since = self.getNoOfMonths(user['yelping_since'])
                elite = self.getEliteScore(user['elite'])
                user_features.write("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}\n".format(user_id, user_name, total_reviews, elite, votes_cool, votes_funny, votes_useful, fans, yelping_since))
                
        user_features.close()

    def getNoOfMonths(self, yelpingSince):
        year_month = yelpingSince.split('-')
        year = int(year_month[0])
        month = int(year_month[1])
        NoOfMonths = year * 12 + month
        
        currentMonth = datetime.now().month
        currentYear = datetime.now().year
        currentNoOfMonths = currentYear * 12 + currentMonth
        
        return currentNoOfMonths - NoOfMonths

    def getEliteScore(self, elite):
        currentYear = datetime.now().year + 1
        sum = 0
        for x in elite:
            sum += x / (currentYear - x)
    
        return sum

def main():
        app = MainApp()
        app.createFeatures()

if __name__ == "__main__":  # Entry Point for program.
    sys.exit(main())
