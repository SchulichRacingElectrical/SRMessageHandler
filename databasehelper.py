from cassandra.cluster import Cluster
from cassandra.query import UNSET_VALUE
import redis
import json
import time

class DatabaseHelper:
    host = "localhost"
    port = 6379
    db = 0
    password = "schulichracing14"
    channel = "endurance-channel"

    def __init__(self):
        cluster = Cluster([self.host])
        self.session = cluster.connect('competition')

    def get_json(self):
        message = self.p.get_message()  # Checks for message
        return message

    def write_to_database(self, message):
        #data_json = message.decode('utf-8')
        data = json.loads(message)
        #time_as_time = datetime.utcfromtimestamp(float(data["Utc"])/1000)
        lat = 0
        int_keys = ["Interval", "Yaw", "Pitch", "Roll", "RPM", "IAT", "EngineTemp", "OilTemp", "FuelTemp", "GPSSats", "GPSQual", "GPSDOP", "LapCount", "LapTime", "Sector", "SectorTime", "PredTime", "ElapsedTime", "CurrentLap", "MAP", "Baro"]
        float_keys = ["RearRight", "RearLeft", "FrontLeft", "FrontRight", "Battery", "AccelX", "AccelY", "AccelZ", "InjectorPW", "AFR", "OilPressure", "Latitude", "Longitude", "Speed", "Distance", "Altitude", "TPS"]
        date = 0
        for key, value in data.items():
            if key in int_keys:
                try:
                    value = int(value)
                except ValueError:
                    value = UNSET_VALUE
            elif key in float_keys:
                try:
                    value = float(value)
                except ValueError:
                    value = UNSET_VALUE
            elif value is '':
                value = UNSET_VALUE
            data[key] = value
        # print(data)
        insert_string = f"""INSERT INTO sessions (session, interval, utc, rearright, rearleft,frontleft, frontright, battery, column_8, accelx, accely, accelz, yaw, pitch,roll, rpm, tps, injectorpw, baro, map, afr,iat, enginetemp, oilpressure, oiltemp,fueltemp, latitude, longitude, speed, distance, altitude, gpssats, gpsqual, gpsdop, lapcount, laptime, sector, sectortime, predtime, elapsedtime, currentlap) VALUES (?, ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
        stmt = self.session.prepare(insert_string)
        results = self.session.execute(stmt, (1, data["Interval"],time.time(),data["RearRight"],data["RearLeft"],data["FrontLeft"],data["FrontRight"],data["Battery"],data["Column_8"],data["AccelX"],data["AccelY"],data["AccelZ"],data["Yaw"],data["Pitch"],data["Roll"],data["RPM"],data["TPS"],data["InjectorPW"],data["Baro"],data["MAP"],data["AFR"],data["IAT"],data["EngineTemp"],data["OilPressure"],data["OilTemp"],data["FuelTemp"],data["Latitude"],data["Longitude"],data["Speed"],data["Distance"],data["Altitude"],data["GPSSats"],data["GPSQual"],data["GPSDOP"],data["LapCount"],data["LapTime"],data["Sector"],data["SectorTime"],data["PredTime"],data["ElapsedTime"],data["CurrentLap"]))

