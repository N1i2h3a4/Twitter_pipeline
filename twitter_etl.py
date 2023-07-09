import tweepy
import pandas as pd 
import json
from datetime import datetime
import s3fs 

def run_twitter_etl():

    access_key = "1677420938772598784-L47BLvObVTw9W7sQ8CAAEZEJS2xkfn" 
    access_secret = "TjZ1J1BalYqEZ9BzB8J7TyVNMpeulZgwPINHX36gkFLOi" 
    consumer_key = "LQ7EUaRJP7AqqASTtfhJgtCsg"
    consumer_secret = "wSdX8ZGZEk8Rj5BhIne16G2QKdBjdntFJjgcEQhGMF0SF8nY0m"


    # Twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)   
    auth.set_access_token(consumer_key, consumer_secret) 

    # # # Creating an API object 
    api = tweepy.API(auth)
    tweets = api.user_timeline(screen_name='@elonmusk', 
                            # 200 is the maximum allowed count
                            count=200,
                            include_rts = False,
                            # Necessary to keep full_text 
                            # otherwise only the first 140 words are extracted
                            tweet_mode = 'extended'
                            )
    print(tweets)

    tweet_list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                        'text' : text,
                        'favorite_count' : tweet.favorite_count,
                        'retweet_count' : tweet.retweet_count,
                        'created_at' : tweet.created_at}
        
        list.append(refined_tweet)

    df = pd.DataFrame(list)
    df.to_csv("s3://Niharika-airflow-bucket/elonmusk_twitter_data.csv")