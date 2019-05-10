from event import Event
from random import randint
import requests
import time

url = 'http://104.45.169.178'
station_ids = [3000, 3005, 3011, 3031, 3075, 3081, 3082,
               4132, 4227, 4216, 4217, 4300, 4336, 4353, 4380, 4385]

if __name__ == '__main__':
    for i in range(100):
        dummy_take = Event(randint(1, 1000), time.time(),
                           'TAKE', station_ids[randint(0, len(station_ids) - 1)])
        requests.post(url, dummy_take.to_message_json())
        print('Sending event ' + dummy_take.to_message_json())
        dummy_return = Event(randint(1, 1000), time.time(),
                             'RETURN', station_ids[randint(0, len(station_ids) - 1)])
        requests.post(url, dummy_return.to_message_json())
        print('Sending event ' + dummy_return.to_message_json())
