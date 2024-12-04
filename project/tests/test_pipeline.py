import os
import sqlite3
import pandas as pd
import pytest
from project.config import settings
from project.pipeline import pipeline
from os.path import join,exists
import sqlite3

@pytest.fixture(scope="session")
def run_pipeline():
    data_path = settings.paths.data_path
    output_table_name= settings.paths.output_table_name
    output_table_path = join(data_path ,f"{output_table_name}.csv")
    output_db_path = join(data_path,f'{output_table_name}.db')
    
    if exists(output_table_path):
        os.remove(output_table_path)
        
    if exists(output_db_path):
        os.remove(output_db_path)
    
    pipeline()
    
    return {
        "data_path": data_path,
        "output_table_name": output_table_name,
        "output_table_path": output_table_path,
        "output_db_path": output_db_path
    }
@pytest.fixture(scope="session")
def sqlite_connection(run_pipeline):
    conn = sqlite3.connect(run_pipeline["output_db_path"])
    yield conn
    conn.close()

def test_table_datasets(run_pipeline):

    assert exists(run_pipeline["output_table_path"]), "csv file does not exist"
    
def test_db_datasets(run_pipeline):

    assert exists(run_pipeline["output_db_path"]), "db file does not exist"
    

def test_sql_connection(sqlite_connection,run_pipeline):
    cursor = sqlite_connection.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()[0]
    if run_pipeline["output_table_name"] in tables:
        assert True
    else:
        assert False
        
def test_sqlite_table(run_pipeline,sqlite_connection):
    cursor = sqlite_connection.cursor()
    cursor.execute(f"SELECT * FROM {run_pipeline['output_table_name']}")
    df = pd.DataFrame(cursor.fetchall(), columns=cursor.description)
    assert len(df) > 0


