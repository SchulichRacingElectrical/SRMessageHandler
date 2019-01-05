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

    def send_json(self, message):
        self.r.publish("main-channel", message)

    def parse_csv(self, filename):
        with open(filename) as f:
            for row in csv.DictReader(f):
                self.send_json(json.dumps(row))


if __name__ == '__main__':
    r = Runner()
    r.parse_csv("data/test.csv")
