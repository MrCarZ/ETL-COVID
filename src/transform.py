import os
from utils import transformations
from .extract import Extract
from .load import MySQL
from dotenv import load_dotenv

load_dotenv()

class Transform:
    def __init__(self, api_path):
        DATABASE_USER = os.getenv("DATABASE_USER")
        DATABASE_HOST = os.getenv("DATABASE_HOST")
        DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD")
        DATABASE_NAME = os.getenv("DATABASE_NAME")
        
        self.api_key =  api_path
        self.raw_data = Extract(self.api_key).get_data_from_api('cases')
        self.database = MySQL(DATABASE_HOST, DATABASE_USER, DATABASE_PASSWORD, DATABASE_NAME)
        getattr(self, 'transform_dataset')()

    def transform_dataset(self):
        for country in self.raw_data:
            country_raw_data = self.raw_data[country]
            evaluated_country_data = transformations.evaluate_json(country_raw_data, ['All'])

            if(len(evaluated_country_data) != 0):
                country_dataframe = self.generate_dataframe(country_raw_data)    
                country_name = transformations.sanitize_table_name(country)
                self.load_into_db(country_name, country_dataframe)

    def generate_dataframe(self, country_data):
        dataframe = transformations.json_to_dataframe(country_data, reset_index=True, transpose=True)
        dataframe = transformations.rename_column(dataframe, {'index':'location', 'long':'lon'})
      
        ## TODO: Comentar funções novas do transformations.py
        
        type_conversion = {'lat': 'float64',
                            'lon': 'float64',
                            'confirmed':'int64',
                            'deaths':'int64',
                            'updated':'datetime'}

        dataframe = transformations.convert_dataframe(dataframe, type_conversion)
        return dataframe

    def load_into_db(self, table_name, table_dataframe):
        self.database.create_table(table_name)
        self.database.insert_to_db(table_dataframe, table_name) 

    
#Transform().parseDataset()