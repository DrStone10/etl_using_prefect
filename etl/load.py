from sqlalchemy import create_engine
from prefect import task
import pandas as pd

@task(log_prints=True)
def load_chunk(dataframe, database):
    engine = create_engine(database)
    dataframe.to_sql(con=engine, name='yt_videos', if_exists='append', index=False)

