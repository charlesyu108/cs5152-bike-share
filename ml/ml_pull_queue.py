import hashlib
import json
import numpy as np
import os
import pandas as pd
import pickle
import requests
import threading
import time

from azure.storage.queue import QueueService
from datetime import datetime, timedelta
from dotenv import load_dotenv, find_dotenv
from sklearn.ensemble import RandomForestClassifier

load_dotenv(find_dotenv())
ACCOUNT_KEY = os.getenv('ACCOUNT_KEY')
ACCOUNT_NAME = os.getenv('ACCOUNT_NAME')
MAX_MESSAGE_LIMIT = 32
PULL_INTERVAL = 600  # in seconds
QUEUE_NAME = 'batchqueue'

DEBUG = True

infile = open('station_cols', 'rb')
columns = pickle.load(infile)
infile = open('model', 'rb')
model = pickle.load(infile)
next_call = time.time()


queue_service = QueueService(
    account_name=ACCOUNT_NAME, account_key=ACCOUNT_KEY)


class Event:
    def __init__(self, event_type, station, time):
        self.event_type = event_type
        self.station = station
        self.time = time


def add_dummy_task():
    task_str = '{"info": {"station": 4177, "type": "RETURN", "user": "charles.yu", "time": 1555776040}, "type": "RETURN"}'
    queue_service.put_message(QUEUE_NAME, task_str)


def parse_result(message):
    msg = json.loads(message)
    info = msg['info']
    return Event(info['type'], info['station'], info['time'])


def get_queue_data():
    metadata = queue_service.get_queue_metadata(QUEUE_NAME)
    count = metadata.approximate_message_count
    messages_read = 0

    while messages_read < count:
        num_messages = count - messages_read if count - messages_read < 32 else 32
        if DEBUG:
            messages = queue_service.peek_messages(
                QUEUE_NAME, num_messages=num_messages)
        else:
            messages = queue_service.get_messages(
                QUEUE_NAME, num_messages=num_messages)
        deltas = {station: 0 for station in columns}
        for message in messages:
            event = parse_result(message.content)
            if event.event_type == 'TAKE':
                deltas[event.station] -= 1
            else:
                deltas[event.station] += 1
            if not DEBUG:
                queue_service.delete_message(
                    QUEUE_NAME, message.id, message.pop_receipt)
        messages_read += len(messages)
    last_msg = parse_result(messages[-1].content)
    deltas['time'] = last_msg.time
    deltas = {k: [v] for k, v in deltas.items()}
    df = pd.DataFrame.from_dict(deltas)
    return model.predict(df)[0]


def run_loop():
    global next_call
    print(get_queue_data())
    next_call = next_call + PULL_INTERVAL
    threading.Timer(next_call - time.time(), run_loop).start()


if __name__ == '__main__':
    run_loop()
