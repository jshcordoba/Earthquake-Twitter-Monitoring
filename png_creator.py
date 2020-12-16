"""
Creates a summary table of the earthquake data as well as a wordcloud
of the tweets that were collected. Saves the wordcloud and table as a
png file and uploads it to the S3 bucket.
"""

import pandas as pd
import io
import boto3
import matplotlib.pyplot as plt
from wordcloud import WordCloud, STOPWORDS
import json


def fetch_csv_s3(bucket: str, filename: str, cli):
    """Fetches CSV file containing tweets from S3 bucket"""
    file_object = cli.get_object(Bucket=bucket,
                                 Key=filename)
    file_dataframe = pd.read_csv(io.BytesIO(file_object['Body'].read()), encoding="ISO-8859-1")
    return file_dataframe


def clean_tweets(data):
    """
    Arg:
        tweets: List of tweets
    Cleans tweets by removing 'https' and everything that follows.
    Returns string of all of the tweets.
    """
    tweets = ''
    for tweet in (data):
        if 'https' in tweet:
            index_url = tweet.index('https')
            tweet = tweet[:index_url]
            tweets += tweet
        else:
            tweets += tweet
    return tweets


def create_wordcloud(data: str):
    """Creates and returns wordcloud"""
    stopwords = STOPWORDS
    cloud = WordCloud(width=800, height=800, background_color='white',
                      stopwords=stopwords, min_font_size=10).generate(data)
    return cloud


def fetch_summary_data(cli):
    """Fetches summary data of Earthquake from S3. Returns dict of the
    data"""
    raw_data = cli.get_object(Bucket='xxx',
                              Key='xxx')
    json_data = raw_data['Body'].read()
    summary_dict = json.loads(json_data)
    return summary_dict


def create_summary_table(tweets, summary_data):
    """Creates summary dataframe"""
    amount_of_tweets = len(tweets)
    magnitude = summary_data['magnitude']
    location = summary_data['city']
    depth = str(summary_data['depth']) + ' km deep'
    summary_dict = {'Amount of Tweets': amount_of_tweets,
                    'Magnitude of Earthquake': magnitude, 'Location': location,
                    'Depth of Earthquake': depth}
    summary_df = pd.DataFrame(data=[summary_dict])
    return summary_df


def main(required, require):
    cli = boto3.client('s3', aws_access_key_id='xxxx',
                       aws_secret_access_key='xxx')
    tweet_data = fetch_csv_s3('xxx', 'xxx.csv', cli)
    tweets = clean_tweets(list(tweet_data.Tweets))
    summary_dict = fetch_summary_data(cli)
    summary_table = create_summary_table(list(tweet_data.Tweets), summary_dict)
    wordcloud = create_wordcloud(tweets)
    plt.figure(figsize=(8, 8), facecolor=None)
    plt.imshow(wordcloud)
    plt.savefig('/tmp/wordcloud.png')
    cli.upload_file('/tmp/wordcloud.png', Bucket='xxx',
                    Key='xxx.png')
    fig, ax = plt.subplots()
    fig.patch.set_visible(False)
    ax.axis('off')
    ax.axis('tight')
    plt.table(cellText=summary_table.values,
              colLabels=summary_table.columns,
              cellLoc='center', loc='center')
    fig.tight_layout()
    plt.savefig('/tmp/summary_table.png')
    cli.upload_file('/tmp/summary_table.png', Bucket='tweetsdatajosh',
                    Key='summary_table.png')


main('d', 'd')
