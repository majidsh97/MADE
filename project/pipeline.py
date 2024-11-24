import pandas as pd
from project.config import settings
from os.path import join
import pandas as pd
from project.ETL import Extract,Load,Transform

def Pipeline():

    """
    The main entry point for the pipeline. This function performs the following steps
    in order:
    1. Downloads datasets from the internet
    2. Loads the datasets into a pandas dataframe
    3. Transforms and merges the dataframes into a single table and save to csv
    4. Saves the single table into a SQLite database file
    """
    
    data_path = settings.paths.data_path
    output_table_name= settings.paths.output_table_name

    # Extract
    extract =  Extract(data_path,settings.urls.values())
    extract.download_datasets()
    df = extract.load_csv_files()

    # Transform
    transform = Transform(df)
    transform.transform(join(data_path ,f"{output_table_name}.csv"))

    # Load
    load = Load(join(data_path,f'{output_table_name}.db'))
    load.save_to_sqlite(join(data_path ,f"{output_table_name}.csv"), output_table_name)


if __name__ == '__main__':
    Pipeline()


       
        