import pandas as pd
from project.config import settings

class Transform:
    def __init__(self,df):
        
        self.Internation_students_canada = df['Internation_students_Canada']
        self.Internation_students_us = df['origin']


        
    def transform(self, output_path):
        """
        This function takes in the raw dataframes and transforms them into a single clean dataframe.
        
        The steps are as follows:
        1. Group the USA data by country and year, and sum the students.
        2. Rename the columns of the Canada data.
        3. Merge the USA and Canada dataframes on the country column.
        4. Cast the year column to string and the students column to float.
        5. Filter the dataframe to only include the years 2015-2022.
        6. Drop any rows with null values.
        7. Fill any remaining null values with 0.
        8. Save the dataframe to a csv file.
        
        Returns:
            The transformed dataframe.
        
        """
        # groupby and rename

        self.Internation_students_us =self.Internation_students_us.groupby(['origin','year']).agg({'students': 'sum'}).reset_index()
        self.Internation_students_us['year'] = self.Internation_students_us['year'].str.split('/').str[0]
        self.Internation_students_us = self.Internation_students_us.pivot(index='origin', columns='year', values='students').fillna(0)

        #rename_columns
        self.Internation_students_canada.rename(columns={'Country of Citizenship':'origin'}, inplace=True)
        
        #merge
        merged_table = pd.merge(self.Internation_students_us, self.Internation_students_canada, on='origin', how='inner', suffixes=('_US', '_Canada'))
        
        #dtype
        merged_table.iloc[:, 0] = merged_table.iloc[:, 0].astype('str')
        merged_table.iloc[:, 1:] = merged_table.iloc[:, 1:].astype('float64')
        
        #filter_data
        merged_table = merged_table[['origin',
                                    '2015_US','2016_US', '2017_US', '2018_US', '2019_US', '2020_US', '2021_US', '2022_US',
                                    '2015_Canada','2016_Canada', '2017_Canada', '2018_Canada', '2019_Canada','2020_Canada', '2021_Canada', '2022_Canada']]

        #drop if nan
        if  merged_table.isna().any().any():
            merged_table.dropna(inplace=True)
            pass
        
        #fill null
        if merged_table.isnull().any().any():
           merged_table = merged_table.fillna(0)
        
        #save
        merged_table.to_csv(output_path)
        print("Data transformation complete.")
        
        return merged_table