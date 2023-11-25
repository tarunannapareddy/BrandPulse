import redis
import json

redis_host = '10.188.248.75'
redis_port = 6379

r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
r.set('foo', 'bar')
print(r.get('foo'))