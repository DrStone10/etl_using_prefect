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
    column_names = config['column_names']

    # etl process:
    raw_data = extract_next_chunk(path, chunk_size, column_names)
    data = transform_chunk(raw_data)
    load_chunk(data, config['my_database'])

if __name__ == '__main__':
    main()