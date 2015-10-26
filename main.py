import sys
from pylab import *
import pandas as pd
import json
from geopy.distance import vincenty
import math

class MainApp(object):
    def __init__(self):
        self.cities_data = pd.read_csv("worldcitiespop.txt", names=["Country", "City", "AccentCity", "Region", "Population", "Latitude", "Longitude"], na_values=['-', ''], low_memory=False)
        self.cities_data["City"] = self.cities_data["City"].apply(lambda x:x.lower())
        self.cities_data["Region"] = self.cities_data["Region"].apply(lambda x:str(x).lower())
        self.city_loc = {}
        self.noMatch = 0;

    def getCityLocation(self, city, state):
        location = {}
        city_state = self.cities_data[(self.cities_data["City"] == city.lower()) & (self.cities_data["Region"] == state.lower())]
        if not city_state.empty:
            location['latitude'] = float(city_state.iloc[0]["Latitude"])
            location['longitude'] = float(city_state.iloc[0]["Longitude"])

        return location
    
    def classify(self):
        file = open('region1.txt', 'w')
        with open('yelp_academic_dataset_business.json') as f:
            for line in f:
                business = json.loads(line)
                city = business['city']
                state = business['state']
                city_state = city + "--" + state
                if city_state not in self.city_loc:
                    self.city_loc[city_state] = self.getCityLocation(city, state)

                if len(self.city_loc[city_state]) != 0:
                    centralPoint = (self.city_loc[city_state]['latitude'], self.city_loc[city_state]['longitude'])
                    businessPoint = (business['latitude'], business['longitude'])
                    miles = vincenty(centralPoint, businessPoint).miles
                    dy = businessPoint[1] - centralPoint[1]
                    dx = businessPoint[0] - centralPoint[0]
                    degree = math.degrees(math.atan2(dy, dx))
                    region = "central"
                    if miles <= 3:
                        region = "central"
                    else:
                        if -45 < degree <= 45:
                            region = "east"
                        elif 45 < degree <= 135:
                            region = "north"
                        elif 135 < degree <= 180 or -180 <= degree <= -135:
                            region = "west"
                        elif -135 < degree <= -45:
                            region = "south"
                    
                    file.write("{0}\t{1}\t{2}\t{3}\t{4}\n".format(business['business_id'], business['stars'], region, miles, degree))
                else:
                    self.noMatch += 1
        
        file.close()
        print(self.noMatch)

def main():
        app = MainApp()
        app.classify()

if __name__ == "__main__":  # Entry Point for program.
    sys.exit(main())
