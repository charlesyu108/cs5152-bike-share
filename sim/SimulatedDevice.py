# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for full license information.

import random
import time
import sys
import csv, datetime

# Using the Python Device SDK for IoT Hub:
#   https://github.com/Azure/azure-iot-sdk-python
# The sample connects to a device-specific MQTT endpoint on your IoT Hub.
import iothub_client
# pylint: disable=E0611
from iothub_client import IoTHubClient, IoTHubClientError, IoTHubTransportProvider, IoTHubClientResult
from iothub_client import IoTHubMessage, IoTHubMessageDispositionResult, IoTHubError, DeviceMethodReturnValue

# The device connection string to authenticate the device with your IoT hub.
# Using the Azure CLI:
# az iot hub device-identity show-connection-string --hub-name {YourIoTHubName} --device-id MyNodeDevice --output table
#CONNECTION_STRING = "HostName=testing-hub-5412.azure-devices.net;DeviceId=bikerack-1;SharedAccessKey=PzFHM66n/VNvITp/AW6iYklEVPen/IWNEKVXsg4S+m4="
CONNECTION_STRING = "HostName=bike-tester-hub.azure-devices.net;DeviceId=testerdevice;SharedAccessKey=ln1gV5fGxEhDlycfooCDEXv9eyrZzp6huzcF1raSxvg="

# Using the MQTT protocol.
PROTOCOL = IoTHubTransportProvider.MQTT
MESSAGE_TIMEOUT = 10000

# Define the JSON message to send to IoT Hub.
TIMESTAMP = ""
RACK_NUMBER = ""
BIKE_NUMBER = ""
# taken (0) or returned (1)
EVENT_TYPE = 1
MSG_TXT = "{\"timestamp\": %s,\"rack no.\": %s,\"bike no.\": %s,\"event type\": %d}"

def send_confirmation_callback(message, result, user_context):
    print ( "IoT Hub responded to message with status: %s" % (result) )

def iothub_client_init():
    # Create an IoT Hub client
    client = IoTHubClient(CONNECTION_STRING, PROTOCOL)
    return client

def iothub_client_telemetry_sample_run(events, bike_ids, rack_ids):

    try:
        client = iothub_client_init()
        print ( "IoT Hub device sending periodic messages, press Ctrl-C to exit" )

        #while True:
        for event in events:
            # Build the message with simulated telemetry values.
            #temperature = TEMPERATURE + (random.random() * 15)
            #humidity = HUMIDITY + (random.random() * 20)
            #msg_txt_formatted = MSG_TXT % (temperature, humidity)
            # message = IoTHubMessage(msg_txt_formatted)

            # # Add a custom application property to the message.
            # # An IoT hub can filter on these properties without access to the message body.
            # prop_map = message.properties()
            # if temperature > 30:
            #   prop_map.add("temperatureAlert", "true")
            # else:
            #   prop_map.add("temperatureAlert", "false")

            # # Send the message.
            # print( "Sending message: %s" % message.get_string() )
            # client.send_event_async(message, send_confirmation_callback, None)
            # time.sleep(1)

            timestamp = event.time.strftime("%y-%m-%d %H:%M:%S")
            if event.type == Event.EventType.TAKE:
                rack_number = event.src
            else:
                rack_number = event.dest
            bike_number = event.bike
            msg_txt_formatted = MSG_TXT % (timestamp, rack_number, bike_number, event.type)
            message = IoTHubMessage(msg_txt_formatted)

            # Add a custom application property to the message.
            # An IoT hub can filter on these properties without access to the message body.
            # prop_map = message.properties()
            # if temperature > 30:
            #   prop_map.add("temperatureAlert", "true")
            # else:
            #   prop_map.add("temperatureAlert", "false")

            # Send the message.
            print( "Sending message: %s" % message.get_string() )
            client.send_event_async(message, send_confirmation_callback, None)
            time.sleep(1)

    except IoTHubError as iothub_error:
        print ( "Unexpected error %s from IoTHub" % iothub_error )
        return
    except KeyboardInterrupt:
        print ( "IoTHubClient sample stopped" )

"""Generates params for the simulation from a Metro Bike Share CSV"""
def generate(csv_file):

    with open(csv_file, "r") as f:
        reader = csv.DictReader(f)
        trips = [row for row in reader]

    events = []
    bike_ids = set()
    hub_ids = set()

    timestamp_format = '%Y-%m-%d %H:%M:%S'

    for trip in trips:
        try:
            bike_id = trip["bike_id"]
            src = trip["start_station"]
            dest = trip["end_station"]
            start_time = datetime.datetime.strptime(trip["start_time"], timestamp_format)
            end_time = datetime.datetime.strptime(trip["end_time"], timestamp_format)

            if not bike_id or not src or not dest or not start_time or not end_time:
                # Bad parse, don't want to record this
                raise Exception()

            start_event = Event(bike_id, start_time, Event.EventType.TAKE, src, dest)
            end_event = Event(bike_id, end_time, Event.EventType.RETURN, src, dest)

            events.append(start_event)
            events.append(end_event)
            bike_ids.add(bike_id)
            hub_ids.add(src)
            hub_ids.add(dest)
        except:
            print("Corrupt data?")
            print("\t", trip)

    sorted_events = sorted(events, key = lambda e: e.time)
    return sorted_events, bike_ids, hub_ids

class Event:
    """
    Represents a bike check-in or check-out event.
    """
    class EventType:
        """
        Enum for the type of bike event.
        """
        TAKE, RETURN = 0, 1

    def __init__(self, bike_id, time, event_type, src_id, dest_id):
        self.bike = bike_id
        self.time = time
        self.src = src_id
        self.dest = dest_id
        self.type = event_type

if __name__ == '__main__':
    print ( "IoT Hub Quickstart #1 - Simulated device" )
    print ( "Press Ctrl-C to exit" )
    params = generate("metro-bike-share-trips-2017-q4-v2.csv")
    iothub_client_telemetry_sample_run(params[0], params[1], params[2])
