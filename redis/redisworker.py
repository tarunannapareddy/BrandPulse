import redis
import random
from datetime import datetime
import time

redis_host = '10.183.205.163'
redis_port = 6379
r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

if __name__ == '__main__':
    key =0
    while(True):
        key = key+1
        time = datetime.now()
        print(f"inserting data key {key} and value {time}")
        r.set(key, time, px= 90000)

        print("printing all data")
        r.set('')
        keys = r.keys()
        for key in keys:
           print(f'key {key} value {r.get(key)}')
        time.sleep(30)
        

