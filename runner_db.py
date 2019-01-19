import redis
import json
import csv
from cassandra.cluster import Cluster


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

    def send_json(self, message):
        self.r.publish("main-channel", message)

    def get_table(self):
        query = f"SELECT * FROM endurance"
        stmt = self.session.prepare(query)
        results = self.session.execute(stmt)
        #print(json.dumps(dict(results)))
        for row in results:
            row_json = dict(json.dumps(row))
            print(row_json)

    def parse_csv(self, filename):
        with open(filename) as f:
            for row in csv.DictReader(f):
                self.send_json(json.dumps(row))


if __name__ == '__main__':
    r = Runner()
    r.get_table()
