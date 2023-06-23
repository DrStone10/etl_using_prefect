from prefect import task
import pandas as pd

@task(log_prints=True)
def transform_chunk(dataframe):

    # take only the useful columns:
    raw_df = dataframe[['video_id', 'title', 'channel_title', 'views', 'likes', 'dislikes']]
    df = raw_df.copy()

    # check for duplicates and remove them:
    print(f'{len(df.axes[0])-len(df["video_id"].unique())} duplicates found')
    if len(df.axes[0])-len(df["video_id"].unique()) != 0:
        df.drop_duplicates(subset=['video_id'], inplace=True)
        print('duplicates removed seccessfully!')

    return df
