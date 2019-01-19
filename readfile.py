import json
import csv
from time import sleep
from databasehelper import DatabaseHelper


class ReadFile:
    filename = ""
    db_helper = DatabaseHelper()

    def __init__(self):
        self.filename = "data/rc_1LukeDriving.csv"

    def parse_csv(self):
        with open(self.filename) as f:
            for row in csv.DictReader(f):
                #print(row)
                self.db_helper.write_to_database(json.dumps(row))


if __name__ == '__main__':
    rf = ReadFile()
    print("Transfer data...")
    rf.parse_csv()
    print("Finished Transfer.")
