import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import yaml
import json
import sqlalchemy
from sqlalchemy import text
local_path = "C:/Users/elshu/OneDrive/Documents/Data/AiCore_Projects/pinterest-data-pipeline708/db_creds.yaml"


random.seed(100)


class AWSDBConnector:

    def __init__(self):
        if __name__ == '__main__':
            print('init')
    
    def read_db_creds(self, local_path):
        '''
        This method reads a local yaml file from a local filepath
        '''
        with open(local_path) as creds:
            cred_dict = yaml.full_load(creds)
        return cred_dict
        
    def create_db_connector(self):
        cred_dict = self.read_db_creds(local_path)
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{cred_dict['USER']}:{cred_dict['PASSWORD']}@{cred_dict['HOST']}:{cred_dict['PORT']}/{cred_dict['DATABASE']}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
            
            print(pin_result)
            print(geo_result)
            print(user_result)


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


