
from dbutils_tweets import DBUtils
from tweets_objects import Tweet, Follows
import pandas as pd
from datetime import datetime
import time
import os

class TwitterAPI:

    def __init__(self, user, password, database, host="localhost"):
        self.dbu = DBUtils(user, password, database, host)

    def load_data_from_csv(self, table_name, csv_path, fields_terminated_by=',', lines_terminated_by='\n',
                           ignore_lines=1):
        """ Load data from a CSV file into a MySQL table using LOAD DATA INFILE """
        # Check if the data already exists in the table
        check_query = f"SELECT COUNT(*) FROM {table_name}"
        existing_rows = self.dbu.execute(check_query).iloc[0, 0]

        if existing_rows == 0:
            # Data doesn't exist, proceed with insertion
            query = f"""
                LOAD DATA LOCAL INFILE '{os.path.abspath(csv_path)}'
                INTO TABLE {table_name}
                FIELDS TERMINATED BY '{fields_terminated_by}'
                LINES TERMINATED BY '{lines_terminated_by}'
                IGNORE {ignore_lines} LINES;
            """
            self.dbu.execute(query, commit=True)
            print("Data inserted successfully.")
        else:
            print("Table already contains data. Skipping insertion.")

    def post_tweet(self, tweet):
        '''posting tweet individually into TWEET table '''
        tweet.tweet_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sql = "INSERT INTO TWEET (tweet_id, user_id, tweet_text, tweet_ts) VALUES (%s, %s, %s, %s)"
        val = (tweet.tweet_id, tweet.user_id, tweet.tweet_text, tweet.tweet_ts)
        self.dbu.insert_one(sql, val)

    def home_timeline(self, user_id):
        '''retrieving home timeline of given user'''
        # Get the users followed by the given user
        follows_query = f"SELECT follows_id FROM FOLLOWS WHERE user_id = {user_id}"
        followed_users = self.dbu.execute(follows_query)["follows_id"].tolist()

        # Get the 10 most recent tweets posted by the followed users
        if followed_users:
            user_ids_str = ", ".join(map(str, followed_users))
            home_timeline_query = f"""
                SELECT tweet_id, user_id, tweet_text, tweet_ts
                FROM TWEET
                WHERE user_id IN ({user_ids_str})
                ORDER BY tweet_ts DESC
                LIMIT 10
            """
            # Return home timeline
            home_timeline = self.dbu.execute(home_timeline_query)
            return home_timeline
        else:
            return pd.DataFrame()
    def get_random_user_ids(self, num_users):
        '''retrieving random user'''
        query = f"SELECT user_id FROM TWEET ORDER BY RAND() LIMIT {num_users}"
        result = self.dbu.execute(query)
        return result["user_id"].tolist()

    def calculate_timelines_per_second(self, num_users):
        '''calculating the timeliens per second given the number of users'''
        # Sample for one random user
        sample_user_id = self.get_random_user_ids(1)[0]
        self.home_timeline(sample_user_id)

        # Measure time for multiple users
        start_time = time.time()

        for _ in range(num_users):
            # Get random user
            random_user_id = self.get_random_user_ids(1)[0]
            # Retrieve home timeline for random user
            self.home_timeline(random_user_id)

        end_time = time.time()
        elapsed_time = end_time - start_time

        # Print the sample timeline for demonstration
        sample_timeline = self.home_timeline(sample_user_id)
        print(f"\nSample Timeline for User {sample_user_id}:\n{sample_timeline}\n")
        print(f"Number of timelines retrieved per second: {num_users / elapsed_time:.2f}")
        print(f"Time taken to retrieve timelines for {num_users} users: {elapsed_time:.2f} seconds")

    def get_followers(self, user_id):
        '''retrieving the followers of a given user'''
        followers_query = f"SELECT user_id FROM FOLLOWS WHERE follows_id = {user_id}"
        followers = self.dbu.execute(followers_query)["user_id"].tolist()
        return followers

    def get_followees(self, user_id):
        '''retrieving the folowees of a given user'''
        followees_query = f"SELECT follows_id FROM FOLLOWS WHERE user_id = {user_id}"
        followees = self.dbu.execute(followees_query)["follows_id"].tolist()
        return followees

    def get_tweets(self, user_id):
        '''retrieving the tweets of a given user'''
        tweets_query = f"SELECT * FROM TWEET WHERE user_id = {user_id} ORDER BY tweet_ts DESC LIMIT 10"
        tweets = self.dbu.execute(tweets_query)
        return tweets

