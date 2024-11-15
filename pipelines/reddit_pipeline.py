import pandas as pd

from utils.constants import CLIENT_ID, SECRET, OUTPUT_PATH
from etls.reddit_etl import connect_reddit, extract_posts, load_data_to_csv, transform_data

def reddit_pipeline(file_name: str, subreddit: str, time_filter: str, limit=None):
    # conectando no reddit 
    instance = connect_reddit(CLIENT_ID, SECRET, 'Airscholar Agent by Glad-Speed8358')
    # extracao
    posts = extract_posts(instance, subreddit, time_filter, limit)
    post_df = pd.DataFrame(posts)
    # transformacao
    post_df = transform_data(post_df)
    # carregando em um csv

    file_path = f'{OUTPUT_PATH}/{file_name}.csv'
    load_data_to_csv(post_df, file_path)    

    return file_path