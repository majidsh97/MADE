import os
import pandas as pd
import kagglehub
from sqlalchemy import create_engine

class Extract:
    def __init__(self, data_path, urls):
        self.data_path = data_path
        self.urls = urls

    def download_datasets(self):
        for url in self.urls:
            path = kagglehub.dataset_download(url)
            os.system(f'cp -r {path}/* {self.data_path}')
            print(f"Downloaded dataset from {url} to {self.data_path}")
        print("All datasets downloaded.")

    def load_csv_files(self):
        csv_names = os.listdir(self.data_path)
        datasets = {}
        for name in csv_names:
            if name.endswith('.csv'):
                dataset_name = name.split('.')[0]
                datasets[dataset_name] = pd.read_csv(os.path.join(self.data_path, name))
                print(f"Loaded {dataset_name} into memory.")
        return datasets