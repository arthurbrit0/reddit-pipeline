from utils.constants import CLIENT_ID, SECRET
from etls.reddit_etl import connect_reddit, extract_posts

def reddit_pipeline(file_name: str, subreddit: str, time_filter: str, limit=None):
    # conectando no reddit 
    instance = connect_reddit(CLIENT_ID, SECRET, 'Airscholar Agent')
    # extracao
    posts = extract_posts(instance, subreddit, time_filter, limit)
    # transformacao
    # carregando no csv