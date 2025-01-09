import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import yaml
import json
import sqlalchemy
from sqlalchemy import text

local_path = "db_creds.yaml"
invoke_url = "https://jnh2wjrl1j.execute-api.us-east-1.amazonaws.com/test/streams/Kinesis-Prod-Stream/record"


random.seed(100)


class AWSDBConnector:

    def __init__(self):
        if __name__ == "__main__":
            print("init")

    def read_db_creds(self, local_path):
        """
        This method reads a local yaml file from a local filepath
        """
        with open(local_path) as creds:
            cred_dict = yaml.full_load(creds)
        return cred_dict

    def create_db_connector(self):
        cred_dict = self.read_db_creds(local_path)
        engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{cred_dict['USER']}:{cred_dict['PASSWORD']}@{cred_dict['HOST']}:{cred_dict['PORT']}/{cred_dict['DATABASE']}?charset=utf8mb4"
        )
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

            # print(pin_result)
            # print(geo_result)
            # print(user_result)

            # Payload for pin information from Pinterest
            pin_payload = json.dumps(
                {
                    "StreamName": "Kinesis-Prod-Stream",
                    "Data": {
                        "index": pin_result["index"],
                        "unique_id": pin_result["unique_id"],
                        "title": pin_result["title"],
                        "description": pin_result["description"],
                        "poster_name": pin_result["poster_name"],
                        "follower_count": pin_result["follower_count"],
                        "tag_list": pin_result["tag_list"],
                        "is_image_or_video": pin_result["is_image_or_video"],
                        "image_src": pin_result["image_src"],
                        "downloaded": pin_result["downloaded"],
                        "save_location": pin_result["save_location"],
                        "category": pin_result["category"],
                    },
                    "PartitionKey": "1254dc635ec5-pin",
                },
                default=str,
            )

            # Payload for the geographical information from Pinterest
            geo_payload = json.dumps(
                {
                    "StreamName": "Kinesis-Prod-Stream",
                    "Data": {
                        "ind": geo_result["ind"],
                        "timestamp": geo_result["timestamp"],
                        "latitude": geo_result["latitude"],
                        "longitude": geo_result["longitude"],
                        "country": geo_result["country"],
                    },
                    "PartitionKey": "1254dc635ec5-geo",
                },
                default=str,
            )

            # Payload for the user information from Pinterest
            user_payload = json.dumps(
                {
                    "StreamName": "Kinesis-Prod-Stream",
                    "Data": {
                        "ind": user_result["ind"],
                        "first_name": user_result["first_name"],
                        "last_name": user_result["last_name"],
                        "age": user_result["age"],
                        "date_joined": user_result["date_joined"],
                    },
                    "PartitionKey": "1254dc635ec5-user",
                },
                default=str,
            )

            headers = {"Content-Type": "application/json"}

            pin_response = requests.request(
                "PUT", invoke_url, headers=headers, data=pin_payload
            )
            geo_response = requests.request(
                "PUT", invoke_url, headers=headers, data=geo_payload
            )
            user_response = requests.request(
                "PUT", invoke_url, headers=headers, data=user_payload
            )

            print(pin_response.status_code)
            print(geo_response.status_code)
            print(user_response.status_code)


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print("Working")