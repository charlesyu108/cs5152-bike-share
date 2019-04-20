#! /usr/bin/env python
# Python 2.7

from messaging import Messaging, Message
import sys, threading, logging
import iothub_client
from iothub_client import IoTHubClient, IoTHubClientError, IoTHubTransportProvider, IoTHubClientResult
from iothub_client import IoTHubMessage, IoTHubMessageDispositionResult, IoTHubError, DeviceMethodReturnValue

logging.getLogger().setLevel(logging.INFO)

# Using the MQTT protocol.
PROTOCOL = IoTHubTransportProvider.MQTT
MESSAGE_TIMEOUT = 10000

class Station:
    """
    Models a Bike Station.
    """
    def __init__(self, id, name, conn_string, listening_port=8080):
        self.id = id
        self.port = listening_port
        self.name = name
        self.messaging = Messaging(port=listening_port)
        self.is_running = False
        self.iothub_client = IoTHubClient(conn_string, PROTOCOL)
        logging.info("Station was created successfully.")

    def notify_iothub(self, msg):
        json_msg = msg.to_json()
        iot_hub_msg = IoTHubMessage(json_msg)
        self.iothub_client.send_event_async(iot_hub_msg, self.iothub_callback, None)
        logging.info("Station notified hub")

    def iothub_callback(self, message, result, user_context):
        logging.info ( "IoT Hub responded to message with status: %s" % (result) )

    def run(self):
        self.is_running = True
        logging.info("Station {} is now running! Listening on port {}".format(self.name, self.port))

        msg_thread = threading.Thread(target=self.messaging.start_listening)
        msg_thread.daemon = True
        msg_thread.start()

        while self.is_running:

            msg = self.messaging.get_message()
            logging.info("Processing new message...")

            if msg.type == Message.MessageType.BIKE_TAKE:
                self.notify_iothub(msg)
            elif msg.type == Message.MessageType.BIKE_RETURN:
                self.notify_iothub(msg)
            elif msg.type == Message.MessageType.SHUTDOWN:
                self.is_running = False
                pass
            else:
                #TODO bad message
                logging.warn("Encountered unknown message type :" + msg.type)
                pass
        logging.info("Station {} shutdown.".format(self.name))

if __name__ == "__main__":
    station_id, port, conn_string = sys.argv[1], sys.argv[2], sys.argv[3]
    station = Station(station_id, "stn " + station_id, conn_string, int(port))
    station.run()
