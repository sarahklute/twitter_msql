'''SQL i wrote in my terminal after setting up twitter_db through MySQL Workbench'''
'''and running command /usr/local/mysql/bin/mysql -u root -p in my terminal'''
USE twitter_db

CREATE TABLE FOLLOWS (
    user_id INT,
    follows_id INT);

CREATE TABLE TWEET (
    tweet_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT,
    tweet_ts DATETIME,
    tweet_text VARCHAR(140));

