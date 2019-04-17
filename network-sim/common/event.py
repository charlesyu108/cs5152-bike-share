#! /usr/bin/env python
# Python 2.7
# Models an event
from messaging import Message

class Event:
    """
    Represents a bike check-in or check-out event.
    """
    class EventType: BIKE_TAKE, BIKE_RETURN = 0, 1

    def __init__(self, bike_id, time, event_type, src_id, dest_id):
        self.bike = bike_id
        self.time = time
        self.src = src_id
        self.dest = dest_id
        self.type = event_type

    def to_message_json(self):
        message = Message(self.type, self.__dict__)
        return message.to_json()