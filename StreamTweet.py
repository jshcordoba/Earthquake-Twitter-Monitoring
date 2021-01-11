"""
Creates tweet stream using the geo coordinates of the latest
earthquake and uploads them to S3 bucket. If the latest earthquake
occured in Alaska, 'Not Available' is uploaded to the S3 bucket.
 """

import ast
import csv
import time
import boto3
import tweepy
import random
import json


class MyStreamListener(tweepy.StreamListener):
    """Modifies tweepy and the tweet stream functionality. Creates
     csv file and saves the tweets to the file. Also, has a time limit
     feature that closes the stream after time has elapsed."""
    def __init__(self, time_limit: int):
        """
        :param time_limit: int, seconds
        """
        self.start_time = time.time()
        self.limit = time_limit
        self.saveFile = open('yourpath.csv', 'w', encoding='utf-8')
        self._csv_writer = csv.writer(self.saveFile)
        self._csv_writer.writerow(['Tweets', 'Coordinates'])
        super(MyStreamListener, self).__init__()

    def on_status(self, status):
        print(status.text)
        self._csv_writer.writerow([status.text, status.coordinates])
        if self.check_time() is False:
            return False

    def check_time(self):
        if time.time() > (self.start_time + self.limit):
            return False

    @property
    def get_tweet_file(self):
        return self.saveFile

    def file_close(self):
        self.saveFile.close()


def fetch_coordinates():
    """Fetches earthquake coordinates from s3 bucket. Coordinates
    are returned as a list."""
    cli = boto3.client('s3')
    coordinate_object = cli.get_object(Bucket='xxx',
                                       Key='xxx.json')
    filedata = coordinate_object['Body'].read()
    filedata = json.loads(filedata)
    coordinates = ast.literal_eval(filedata['coordinate'])
    return coordinates


def tweets_to_s3():
    """Uploads the tweet.csv file to the S3 Bucket"""
    cli = boto3.client('s3')
    cli.upload_file('yourpath.csv', Bucket='xxx',
                    Key='xxx.csv')


def alaska_check():
    """Retrieves json file containing the earthquake data. If Alaska is
    is in 'city' True is returned. False is returned otherwise. """
    cli = boto3.client('s3')
    city_object = cli.get_object(Bucket='xxx',
                                       Key='xxx.json')
    filedata = city_object['Body'].read()
    filedata = json.loads(filedata)
    city = filedata['city']
    if 'Alaska' in city:
        return True
    else:
        return False


def main(event, context):
    if alaska_check() is True:
        saveFile = open('xxx', 'w', encoding='utf-8')
        csv_writer = csv.writer(saveFile)
        csv_writer.writerow(['Tweets'])
        csv_writer.writerow(['Not Available in Area'])
        saveFile.close()
        tweets_to_s3()
    else:
        coordinates = fetch_coordinates()
        auth = tweepy.OAuthHandler(consumer_key='xxx',
                                   consumer_secret='xxx')
        auth.set_access_token('xxx',
                              'xxx')
        api = tweepy.API(auth)
        try:
            api.verify_credentials()
            print('Okay')
        except:
            print("Credentials don't work")
        tweet_stream = MyStreamListener(5)
        myStream = tweepy.Stream(auth=api.auth, listener=tweet_stream)
        print(myStream.filter(locations=coordinates))
        tweet_stream.file_close()
        tweets_to_s3()
        return (print("Done!"))


main('d','d')
