from sqlalchemy import create_engine
import pandas as pd

class Load:
    def __init__(self, db_path):
        self.db_path = db_path

    def save_to_sqlite(self, file_path, table_name):
        df = pd.read_csv(file_path)
        engine = create_engine(f"sqlite:///{self.db_path}")
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Data successfully saved to SQLite database at {self.db_path} in table '{table_name}'.")
