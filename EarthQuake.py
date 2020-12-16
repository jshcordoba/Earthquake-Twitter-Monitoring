"""
This class fetches the earthquakes that occured within the United States
within the past 24 hours. Creates a report of the quakes that have
happened. Report is retrievable through 'get_report'. Also, returns
the amount of quakes that have happened through 'get_count'.
"""

import requests
from datetime import datetime, timedelta
import csv
import time
import pandas as pd
from math import cos, radians


class EarthQuake():
    def __init__(self, sequence: int):
        self._today = datetime.today().date()
        self._yesterday = self._today - timedelta(days=1)
        self._data_file = open(f'data_file{sequence}.csv', 'w')
        self._data_file_path = f'data_file{sequence}.csv'
        self._csv_writer = csv.writer(self._data_file)
        self._quake_data = self.fetch_quake()
        self._count_quake = self._quake_data['metadata']['count']

    def fetch_quake(self):
        """Fetches earthquake data from USGS api."""
        r = requests.get(f"https://earthquake.usgs.gov/fdsnws/event/1/query?"
                         f"format=geojson&starttime={self._yesterday}&"
                         f"endtime={self._today}&"
                         f"minlatitude=21&minlongitude=-165&maxlatitude=70&"
                         f"maxlongitude=-65&minmagnitude=3&maxdepth=70")
        return r.json()

    def fill_in_csv(self):
        """Fills in CSV of earthquake data"""
        data = self.fetch_quake()
        count = 0
        for row in data['features']:
            if count == 0:
                headers = (list(row['properties'].keys()) +
                           list(row['geometry'].keys()))
                headers.append('Square Coordinates')
                self._csv_writer.writerow(headers)
                count += 1
            else:
                values = list(row['properties'].values()) + \
                         list(row['geometry'].values())
                values.append(self.square_coordinate(
                    row['geometry']['coordinates'][:2]))
                self._csv_writer.writerow(values)

        self._data_file.close()

    def square_coordinate(self, coordinate):
        """
        coordinate: Latitidude & Longitude coordinate of Earthquake

        Returns square coordinate points that are a 10 mile radius
        from the initial coordinate point that is passed through.
        """
        y, x = tuple(coordinate)
        search_radius = 10  # in miles
        earth_radius = 3958.8
        dY = 360*search_radius/earth_radius
        dX = dY*cos(radians(y))
        upper_x = x - dX
        upper_y = y - dY
        lower_x = x + dX
        lower_y = y + dY
        coordinate = [upper_y, lower_x, lower_y, upper_x]
        return coordinate

    @property
    def get_count(self):
        return self._count_quake

    @property
    def get_report(self):
        return self._data_file_path