import pandas as pd
from pathlib import Path
import sys
import os
sys.path.append(os.getcwd())
#import pandasql as psql
#from project.Extract_Transform import extract

class Transform:
    def __init__(self, data_path):
        self.data_path = data_path
        dtype = {
            'Country': 'str', 
            'Code' : 'str',
            'Year' : 'int',
            'CO2 emission (Tons)' : 'float64',
            'Population(2022)' : 'float64',
            'Area' : 'float64',
            '% of World' : 'str',
            'Density(km2)' : 'str',
            'Disaster Type': 'str',
            'Source': 'str',
            'CTS_Code': 'str',
            'CTS_Name': 'str',
            'F1980': 'float64',
            'F1981': 'float64',
            'F1982': 'float64',
            'F1983': 'float64',
            'F1984': 'float64',
            'F1985': 'float64',
            'F1986': 'float64',
            'F1987': 'float64',
            'F1988': 'float64',
            'F1989': 'float64',
            'F1990': 'float64',
            'F1991': 'float64',
            'F1992': 'float64',
            'F1993': 'float64',
            'F1994': 'float64',
            'F1995': 'float64',
            'F1996': 'float64',
            'F1997': 'float64',
            'F1998': 'float64',
            'F1999': 'float64',
            'F2000': 'float64',
            'F2001': 'float64',
            'F2002': 'float64',
            'F2003': 'float64',
            'F2004': 'float64',
            'F2005': 'float64',
            'F2006': 'float64',
            'F2007': 'float64',
            'F2008': 'float64',
            'F2009': 'float64',
            'F2010': 'float64',
            'F2011': 'float64',
            'F2012': 'float64',
            'F2013': 'float64',
            'F2014': 'float64',
            'F2015': 'float64',
            'F2016': 'float64',
            'F2017': 'float64',
            'F2018': 'float64',
            'F2019': 'float64',
            'F2020': 'float64',
            'F2021': 'float64',
            'F2022': 'float64'
        }
        self.df = pd.read_csv(data_path, encoding='latin1', dtype=dtype)

    def clean_data(self):
        self.df.dropna(how='all', inplace=True)
        self.df.drop_duplicates(inplace=True)
    
    def process_density_column(self):
        #Convert the column to string if it's not already
        self.df['Density(km2)'] = self.df['Density(km2)'].astype(str)
        
        self.df['Density(km2)'] = (
            self.df['Density(km2)']
            .str.extract('(\d+)')   # Extract numeric part (if any)
            .fillna(0)              # Replace NaN with 0 (or a suitable value)
            .astype(int)            # Convert to integer
        )
    
    def fill_missing_values(self):
        columns_to_fill = ['F1980', 'F1981', 'F1982', 'F1983', 'F1984', 'F1985', 'F1986', 'F1987', 'F1988', 'F1989', 'F1990', 
                           'F1991', 'F1992', 'F1993', 'F1994', 'F1995', 'F1996', 'F1997', 'F1998', 'F1999', 'F2000', 'F2001', 
                           'F2002', 'F2003', 'F2004', 'F2005', 'F2006', 'F2007', 'F2008', 'F2009', 'F2010', 'F2011', 'F2012', 
                           'F2013', 'F2014', 'F2015', 'F2016', 'F2017', 'F2018', 'F2019', 'F2020', 'F2021', 'F2022']
        self.df[columns_to_fill] = self.df[columns_to_fill].fillna(0)
    


    def rename_columns(self):
        self.df = self.df.rename(columns={'Country_x': 'Country', 'Ã¯Â»Â¿ObjectId': 'ObjectId', 'Indicator': 'Disaster Type'})
        self.df['Disaster Type'] = self.df['Disaster Type'].astype(str).str.replace(
            'Climate related disasters frequency, Number of Disasters:', '', regex=False)
        self.df = self.df.rename(columns={'CO2 emission (Tons)': 'CO2_Emission'})
        
    
    def filter_data(self):
        self.df.dropna(subset=['Year'], inplace=True)
        self.df['Year'] = self.df['Year'].astype(int)
        self.df = self.df.sort_values(by='Year', ascending=False)
        self.df = self.df[self.df['Year'] >= 1980]
        #self.df.drop(self.df[self.df['Disaster_Type'] == 'TOTAL'].index,inplace=True)
    
    
    
    def save_transformed_data(self, output_path):
        self.df.to_csv(output_path, index=False)
    
    def transform(self, output_path):
        self.clean_data()
        self.process_density_column()
        self.fill_missing_values()
        self.rename_columns()
        #self.drop_unnecessary_columns()
        self.filter_data()
        
        self.save_transformed_data(output_path)
        print("Data transformation complete.")