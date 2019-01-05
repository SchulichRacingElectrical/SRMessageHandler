import redis
import json
import csv


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
            if message:
                print(message['data'])


if __name__ == '__main__':
    r = Runner()
    r.print()
