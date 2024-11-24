import os
import sys
import pandas as pd
import sqlite3
from pathlib import Path
project_path = Path(__file__).resolve().parent.parent
sys.path.append(str(project_path))
sys.path.append(os.getcwd())
from project.Extract_Transform.extract import DataDownloader, DataProcessor
from project.Extract_Transform.transform import DataTransformer

class Pipeline:
    def __init__(self):
        pass

   
    def extract_data(self):
        kaggle_dataset = 'moazzimalibhatti/co2-emission-by-countries-year-wise-17502022'
        kaggle_data_dir = './data/raw_csv_without_transform/'
        imf_url = 'https://opendata.arcgis.com/datasets/b13b69ee0dde43a99c811f592af4e821_0.csv'
        imf_data_dir = './data/raw_csv_without_transform/'
        kaggle_data_path = './data/raw_csv_without_transform/CO2 emission by countries.csv'
        imf_data_path = './data/raw_csv_without_transform/Climate-related_Disasters_Frequency.csv'
        output_dir = './data/raw_csv_without_transform/'

        downloader = DataDownloader(kaggle_dataset, kaggle_data_dir, imf_url, imf_data_dir)
        #download_kaggle_dataset(dataset_name, kaggle_data_dir)
        downloader.download_kaggle_dataset()
        downloader.download_imf_dataset()
        #dictionary of missing country codes
        missing_country_codes = {
        
        'Russia': 'RU',
        'Saint Helena': 'SH',
        'South Korea': 'KP',
        'Syria': 'SY',
        'Taiwan': 'TW',
        'Tanzania': 'TZ',
        }
        processor = DataProcessor(kaggle_data_path, missing_country_codes, imf_data_path)
        df1 = processor.load_kaggle_data()
        df2 = processor.load_imf_data()
        merged_df = processor.merge_datasets(df1, df2)
        processor.save_merged_data(merged_df, output_dir)

    def transform_data(self):
        data_path = './data/raw_csv_without_transform/outer_joined_data.csv'
        output_path = './data/transformed_climate_data.csv'

        transformer = DataTransformer(data_path)      
        transformer.clean_data()
        transformer.process_density_column()
        transformer.fill_missing_values()
        transformer.rename_columns()
        transformer.filter_data()
        
        #transformer.drop_unnecessary_columns()
        transformer.transform(output_path)
        transformer.save_transformed_data(output_path)
        print("Data transformation complete.")

        
    def csv_to_sqlite(self):    
        df= pd.read_csv('./data/transformed_climate_data.csv')
        df['F2021'] = df['F2021'].astype(str).str.strip()
        df['F2022'] = df['F2022'].astype(str).str.strip()
        columns_to_drop = ['Calling Code', 'ObjectId', 'Country_y', 'ISO3', 'CTS_Full_Descriptor', 'Unit','F2021','F2022']
        df.drop(columns=columns_to_drop, inplace=True)
        conn = sqlite3.connect('./data/ClimateData_revised.sqlite')
        df.to_sql('ClimateData_revised', conn, if_exists='replace', index=False)
        print("Data inserted successfully into the database.")
        conn.close()

    

if __name__ == '__main__':
    Pipeline= Pipeline()
    Pipeline.extract_data()
    Pipeline.transform_data()
    Pipeline.csv_to_sqlite()


       
        