import redis
import json
import csv
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import time


class Runner:

    host = "localhost"
    port = 6379
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
        cluster = Cluster([self.host])
        self.session = cluster.connect('competition')
        self.session.row_factory = dict_factory

    def send_json(self, message):
        self.r.publish("competition-channel", message)

    def process_table(self):

        query = f"SELECT * FROM sessions WHERE session=1 ORDER BY interval"
        stmt = self.session.prepare(query)
        results = self.session.execute(stmt)

        for row in results:
            row_json = json.dumps(row)
            self.send_json(row_json)

    # For OG runner
    def parse_csv(self, filename):
        with open(filename) as f:
            for row in csv.DictReader(f):
                self.send_json(json.dumps(row))


if __name__ == '__main__':
    r = Runner()
    while True:
        print("Transfer started...")
        r.process_table()
        print("Finished reading from competition.sessions table.")
        time.sleep(2)
