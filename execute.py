from prefect import flow
import pandas as pd
import yaml

# my etl functions:
from etl import extract_next_chunk, transform_chunk, load_chunk

with open('config/config.yaml', 'r') as file:
    config = yaml.safe_load(file)

@flow(name='my_etl')
def main():
    path = config['raw_path']
    chunk_size = config['chunk_size']
    column_names = ['video_id','trending_date','title','channel_title','category_id','publish_time','tags','views','likes','dislikes','comment_count','thumbnail_link','comments_disabled','ratings_disabled','video_error_or_removed','description']

    # etl process:
    raw_data = extract_next_chunk(path, chunk_size, column_names)
    data = transform_chunk(raw_data)
    load_chunk(data, config['my_database'])

if __name__ == '__main__':
    main()