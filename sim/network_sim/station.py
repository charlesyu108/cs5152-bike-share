#! /usr/bin/env python
# Python 2.7
# Azure IOT Hub Client
from messaging import Messaging, Message

import sys, threading

import iothub_client
from iothub_client import IoTHubClient, IoTHubClientError, IoTHubTransportProvider, IoTHubClientResult
from iothub_client import IoTHubMessage, IoTHubMessageDispositionResult, IoTHubError, DeviceMethodReturnValue

CONNECTION_STRING = "HostName=bike-tester-hub.azure-devices.net;DeviceId=testerdevice;SharedAccessKey=ln1gV5fGxEhDlycfooCDEXv9eyrZzp6huzcF1raSxvg="

# Using the MQTT protocol.
PROTOCOL = IoTHubTransportProvider.MQTT
MESSAGE_TIMEOUT = 10000


class Station:
    """
    Models a Bike Station.
    """
    def __init__(self, id, name, listening_port=8080):
        self.id = id
        self.port = listening_port
        self.name = name
        self.messaging = Messaging(port=listening_port)
        self.is_running = False
        self.iothub_client = IoTHubClient(CONNECTION_STRING, PROTOCOL)

    def notify_iothub(self, msg):
        message = IoTHubMessage(msg.to_json())
        self.iothub_client.send_event_async(message, self.iothub_callback, None)

    def iothub_callback(self, message, result, user_context):
        print ( "IoT Hub responded to message with status: %s" % (result) )

    def run(self):
        self.is_running = True
        print("Station {} is now running! Listening on port {}".format(self.name, self.port))

        msg_thread = threading.Thread(target=self.messaging.start_listening)
        msg_thread.daemon = True
        msg_thread.start()

        while self.is_running:

            msg = self.messaging.get_message()

            if msg.type == Message.MessageType.BIKE_TAKE:
                self.notify_iothub(msg)
            elif msg.type == Message.MessageType.BIKE_RETURN:
                self.notify_iothub(msg)
            elif msg.type == Message.MessageType.SHUTDOWN:
                self.is_running = False
                pass
            else:
                #TODO bad message
                pass
        print("Station {} shutdown.".format(self.name))

if __name__ == "__main__":
    # parse argv - get host ports and parents
    station_id, port = sys.argv[1], sys.argv[2]
    station = Station(station_id, "stn " + station_id, int(port))
    station.run()