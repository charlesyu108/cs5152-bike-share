#! /usr/bin/env python
# Python 2.7
# Models an event
from messaging import Message

class Event:
    """
    Represents a bike check-in or check-out event.
    """
    class EventType: BIKE_TAKE, BIKE_RETURN = "TAKE", "RETURN"

    def __init__(self, user_id, time, event_type, station_id):
        self.user = user_id
        self.time = time
        self.station = station_id
        self.type = event_type

    def to_message_json(self):
        message = Message(self.type, self.__dict__)
        return message.to_json()