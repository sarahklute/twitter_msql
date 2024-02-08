"""
J. Rachlin
Demonstration of working with Relational Database with python
"""

import os
from tweets_mysql import TwitterAPI
import pandas as pd
from tweets_objects import Tweet, Follows
import time


# making sure connection is being made
user_name = os.environ.get('TWEET_USER')
print(user_name)
password = os.environ.get('TWEET_PASSWORD')
print(password)

def main():

    # Authenticate
    api = TwitterAPI(os.environ["TWEET_USER"], os.environ["TWEET_PASSWORD"], 'twitter_db')

    # Load data from CSV into FOLLOWS table
    csv_path = 'follows_sample.csv'  # Replace with the actual path to your CSV file
    api.load_data_from_csv('FOLLOWS', csv_path)

    # Record start time
    start_time = time.time()

    # Read  CSV file into a pandas DataFrame
    df_TWEET = pd.read_csv('tweets_sample.csv')

    # Iterate over each row in the TWEETS df
    for index, row in df_TWEET.iterrows():
        twt = Tweet(tweet_id = None, user_id=row[0], tweet_ts = None, tweet_text = row[1])
        api.post_tweet(twt)

    # Calculating time to load 10000000 tweets into table
    end_time = time.time()
    elapsed_time = end_time - start_time
    # Avg tweets per second
    total_tweets = len(df_TWEET)
    average_tweets_per_second = total_tweets / elapsed_time

    print(f"Number of tweets processed: {total_tweets}")
    print(f"Time taken to process tweets: {elapsed_time:.2f} seconds")
    print(f"Average Tweets per Second: {average_tweets_per_second:.2f}")

    # Set the display option to show all columns
    pd.set_option('display.max_columns', None)

    # Random user home timeline
    num_users_to_fetch = 500
    api.calculate_timelines_per_second(num_users_to_fetch) # Returns calculations of home_timeline
    api.home_timeline(2)

if __name__ == '__main__':
    main()