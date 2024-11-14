import numpy as np
import praw
from praw import Reddit
import sys
import pandas as pd
from utils.constants import POST_FIELDS

def connect_reddit(client_id, client_secret, user_agent) -> Reddit:
    try:
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
            username='Glad-Speed8358',
            password='!Aawwodtr1964'
        )
        print("Conectado ao Reddit")
        return reddit
    except Exception as e:
        print(e)
        sys.exit(1)

def extract_posts(reddit_instance: Reddit, subreddit: str, time_filter: str, limit=None):
    subreddit = reddit_instance.subreddit(subreddit)
    posts = subreddit.top(time_filter=time_filter, limit=limit)

    posts_list = []

    for post in posts:
        post_dict = vars(post)
        # Mapeando os campos do post do Reddit, usando os campos definidos em POST_FIELDS
        post = {key: post_dict.get(key, None) for key in POST_FIELDS}
        posts_list.append(post)

    return posts_list

def transform_data(post_df: pd.DataFrame):
    post_df['created_utc'] = pd.to_datetime(post_df['created_utc'], unit='s')
    
    if 'over_18' in post_df.columns:
        post_df['over_18'] = np.where(post_df['over_18'] == True, True, False)
    
    if 'author' in post_df.columns:
        post_df['author'] = post_df['author'].astype(str)
    
    if 'edited' in post_df.columns:
        edited_mode = post_df['edited'].mode()[0] if not post_df['edited'].mode().empty else False
        post_df['edited'] = np.where(post_df['edited'].isin([True, False]), post_df['edited'], edited_mode).astype(bool)
    
    if 'num_comments' in post_df.columns:
        post_df['num_comments'] = post_df['num_comments'].fillna(0).astype(int)
    
    if 'score' in post_df.columns:
        post_df['score'] = post_df['score'].fillna(0).astype(int)
    
    if 'upvote_ratio' in post_df.columns:
        post_df['upvote_ratio'] = post_df['upvote_ratio'].fillna(0).astype(float)
    
    if 'selftext' in post_df.columns:
        post_df['selftext'] = post_df['selftext'].fillna('').astype(str)
    
    if 'title' in post_df.columns:
        post_df['title'] = post_df['title'].fillna('').astype(str)

    return post_df

def load_data_to_csv(data: pd.DataFrame, path: str):
    data.to_csv(path, index=False)
