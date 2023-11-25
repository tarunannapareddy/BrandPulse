import redis
import random
from datetime import datetime
import time

redis_host = '10.183.205.163'
redis_port = 6379
r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

if __name__ == '__main__':
    key = 0
    while(True):
        key +=1
        value = datetime.now()
        print("inserting data key {} and value {}".format(key, value))
        r.set(str(key), str(value), px= 9000)

        print("printing all data")
        keys = r.keys()
        for k in keys:
           print(f'key {k} value {r.get(k)}')
        time.sleep(3)