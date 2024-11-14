import praw
from praw import Reddit
import sys

def connect_reddit(client_id, client_secret, user_agent) -> Reddit:
    try:
        reddit = praw.Reddit(client_id=client_id, client_secret=client_secret,user_agent=user_agent, username='Glad-Speed8358', password='!Aawwodtr1964')
        print("Conectado ao Reddit")
        return reddit
    except Exception as e:
        print(e)
        sys.exit(1) 

def extract_posts(reddit_instance: Reddit, subreddit:str, time_filter:str, limit=None):
    subreddit = reddit_instance.subreddit(subreddit)
    posts = subreddit.top(time_filter=time_filter, limit=limit)

    posts_list = []

    for post in posts:
        post_dict = vars(post)
        print()
        post = {key: post_dict[key] for key in POST_FIELDS}
        posts_list.append(post)

    return posts_list

