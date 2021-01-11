"""
Earthquake monitoring portion. Compares the records of two earthquake
objects and checks to see if there are any discrepancies. If a
discrepancy is found, that means that there is a new earthquake. The
new earthquake data is uploaded as a json file to the S3 bucket.
"""

import requests
from datetime import datetime, timedelta
import csv
import time
import pandas as pd
from math import cos, radians
import boto3
from EarthQuake import EarthQuake
import ast
import json


def new_summary_info(greater_report, less_report):
    """
      greater_report: Dataframe - Dataframe that has the new quake
      less_report: Dataframe - Dataframe
      Extracts the new earthquake data and returns it as a dictionary
      """
    new_quake = greater_report[~greater_report.isin(less_report)]
    new_quake.drop(['time', 'updated'], axis=1, inplace=True)
    new_quake.dropna(how='all', inplace=True)
    new_quake_coordinate = new_quake['Square Coordinates'].iloc[0]
    new_depth = ast.literal_eval(new_quake['coordinates'].iloc[0])[-1]
    new_city = new_quake['place'].iloc[0]
    new_mag = new_quake['mag'].iloc[0]
    summary_info = {'coordinate': new_quake_coordinate, 'city': new_city,
                    'magnitude': new_mag, 'depth': new_depth}
    return summary_info


def report_quake():
    """
    Sends new quake info to s3 bucket as a json file.
    :return:
    """
    call_one_report = pd.read_csv(call_one.get_report)
    call_two_report = pd.read_csv(call_two.get_report)
    if call_two.get_count > call_one.get_count:
        summary_data = new_summary_info(call_two_report,
                                        call_one_report)
        cli.put_object(Body=json.dumps(summary_data), Bucket='xxx',
                       Key='xxx.json')
    else:
        summary_data = new_summary_info(call_one_report,
                                                call_two_report)
        cli.put_object(Body=json.dumps(summary_data), Bucket='xxx',
                       Key='xxx.json')


if __name__ == '__main__':
    cli = boto3.client('s3', aws_access_key_id='xxx',
                       aws_secret_access_key='xxx'
                                             'xxx'
                       )
    call_two = EarthQuake(2)
    while True:
        call_one = EarthQuake(1)
        call_one.fill_in_csv()
        if call_one.get_count == 0 or call_two.get_count == 0:
            time.sleep(240)
            continue
        if call_two.get_count != call_one.get_count:
            report_quake()
        time.sleep(60)
        call_two = EarthQuake(2)
        call_two.fill_in_csv()
        if call_two.get_count == 0 or call_one.get_count == 0:
            time.sleep(240)
            continue
        if call_two.get_count != call_one.get_count:
            report_quake()
