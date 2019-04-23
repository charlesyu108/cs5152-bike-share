import json, os, pickle, requests, threading, time, logging, base64
import numpy as np
import pandas as pd

from azure.storage.queue import QueueService
from datetime import datetime, timedelta
from dotenv import load_dotenv, find_dotenv
from sklearn.ensemble import RandomForestClassifier

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

infile = open('station_cols', 'rb')
columns = pickle.load(infile)
infile = open('model', 'rb')
model = pickle.load(infile)
next_call = time.time()

class Event:
    def __init__(self, event_type, station, time):
        self.event_type = event_type
        self.station = station
        self.time = time

#def add_dummy_task():
#   task_str = '{"info": {"station": 4177, "type": "RETURN", "user": "charles.yu", "time": 1555776040}, "type": "RETURN"}'
#    queue_service.put_message(QUEUE_NAME, task_str)

def parse_result(message):
    # Messages are B64 encoded by default
    decoded_bytes = base64.b64decode(message)
    decoded_string = decoded_bytes.decode('utf-8')
    print(decoded_string)
    msg = json.loads(decoded_string)
    if 'info' in msg:
        msg = msg['info']
    return Event(msg['type'], msg['station'], int(msg['time']))

global PREDICTION
PREDICTION = None

def get_queue_data():
    metadata = queue_service.get_queue_metadata(QUEUE_NAME)
    count = metadata.approximate_message_count
    messages_read = 0
    messages = None

    while messages_read < count:
        num_messages = count - messages_read if count - messages_read < 32 else 32

        if DEBUG:
            messages = queue_service.peek_messages(QUEUE_NAME, num_messages=num_messages)
        else:
            messages = queue_service.get_messages(QUEUE_NAME, num_messages=num_messages)

        deltas = {station: 0 for station in columns}

        for message in messages:
            event = parse_result(message.content)
            if event.station in deltas:

                if event.event_type == 'TAKE':
                    deltas[event.station] -= 1
                else:
                    deltas[event.station] += 1

            if not DEBUG:
                queue_service.delete_message(QUEUE_NAME, message.id, message.pop_receipt)

        messages_read += len(messages)
    
    # There were messages successfuly fetched:
    if messages:
        last_msg = parse_result(messages[-1].content)
        deltas['time'] = last_msg.time
        deltas = {k: [v] for k, v in deltas.items()}
        df = pd.DataFrame.from_dict(deltas)
        prediction = model.predict(df)[0]
        
        global PREDICTION
        PREDICTION = prediction

    # Always return PREDICTION -- Either the last if no messages or most recent.
    return PREDICTION

def run_loop():
    global next_call
    
    logging.info("Making new prediction...")

    # Make prediction
    prediction = get_queue_data()
    logging.info("Prediction is : " + str(prediction))

    # Upload prediction to server
    requests.post(PREDICTION_SERVER_URL + "/model", json={"prediction":str(prediction)})

    next_call = next_call + PULL_INTERVAL
    threading.Timer(next_call - time.time(), run_loop).start()

if __name__ == '__main__':
    load_dotenv(find_dotenv())
    PREDICTION_SERVER_URL = os.getenv("PREDICTION_SERVER_URL", "http://20.42.27.246")
    ACCOUNT_KEY = os.getenv('ACCOUNT_KEY')
    ACCOUNT_NAME = os.getenv('ACCOUNT_NAME')
    logging.info("Key//Name:" + ACCOUNT_KEY + "//" + ACCOUNT_NAME)
    MAX_MESSAGE_LIMIT = 32
    PULL_INTERVAL = int(os.getenv('PULL_INTERVAL', 600))  # in seconds
    QUEUE_NAME = 'batchqueue'
    DEBUG = os.getenv('DEBUG', False)
    queue_service = QueueService(account_name=ACCOUNT_NAME, account_key=ACCOUNT_KEY)
    run_loop()
