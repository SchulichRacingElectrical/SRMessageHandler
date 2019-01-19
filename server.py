import redis
import time

host = "hilmi.ddns.net"
port = 20114
db = 0
password = "schulichracing14"
queue = redis.Redis(
    host=host,
    port=port,
    password=password)
channel = queue.pubsub()
counter = 0
while True:
    queue.publish("main-channel", counter)
    counter = counter + 1
    time.sleep(0.2)
