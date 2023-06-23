import pandas as pd
from prefect import task
import os

@task(log_prints=True)
def extract_next_chunk(file_path:str, chunk_size:int, header):
    chunks_loaded_txt = 'chunks_loaded.txt'
    chunks_loaded = set()
    if chunks_loaded_txt in os.listdir():
        with open(chunks_loaded_txt, 'r') as file:
            chunks_loaded = set(file.read().splitlines())
    chunk_num = 1
    while True:
        if str(chunk_num) not in chunks_loaded:
            df = pd.read_csv(file_path, skiprows=(chunk_size*(chunk_num-1)), nrows=chunk_size)
            if df.empty:
                break
            df.columns = header
            df.to_csv(f'data/chunks/chunk{chunk_num}.csv', index=False)
            with open(chunks_loaded_txt, 'a') as file:
                file.write(str(chunk_num) + '\n')
            return df
        chunk_num += 1
    