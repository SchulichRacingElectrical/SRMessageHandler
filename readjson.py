import redis
import json
import csv
import pandas as pd
from io import StringIO


class Runner:
    host = "hilmi.ddns.net"
    port = 20114
    db = 0
    password = "schulichracing14"
    channel = "main-channel"

    def __init__(self):
        self.r = redis.Redis(
            host=self.host,
            port=self.port,
            password=self.password)
        self.p = self.r.pubsub()
        self.p.subscribe(self.channel)

    def get_json(self):
        message = self.p.get_message()  # Checks for message
        return message

    def print(self):
        while True:
            message = self.get_json()
            if message and message['data'] != 1:
                print(message)
                data = message['data'].decode('utf-8')
                data_json = json.loads(data)
                # #df = pd.DataFrame.from_dict(data.items(), orient='index', columns=data.keys())
                #
                # # #normalized_json = json_normalize(message)
                # #
                # #df = pd.read_json(data)
                # df = pd.DataFrame(data_json, index=[0])
                # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
                #     print(df)


if __name__ == '__main__':
    r = Runner()
    r.get_json()
    r.print()
