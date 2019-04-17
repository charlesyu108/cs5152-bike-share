#! /usr/bin/env python
# Python 2.7
# Feeds data to stations
from event import Event
from messaging import Message
from parameters import SimulationParamGenerator
import requests, time

class Simulation:

    @classmethod
    def init_from_csv(cls, csv_file):
        print("Init from {}", csv_file, "...")
        events, _, _ = SimulationParamGenerator.generate(csv_file)
        print("TODO: You still must provide mapping of stations to addresses!")
        return cls({}, events)

    def __init__(self, stations, events):
        self.stations = stations
        self.events = events

    def send_event(self, event):
        if event.type == Event.EventType.BIKE_TAKE:
            send_to = event.src
        else:
            send_to = event.dest

        if send_to in self.stations:
            addr = self.stations[send_to]
            requests.post(addr, data=event.to_message_json())
            return True

        return False

    def kill_station(self, id):
        kill_message = Message(Message.MessageType.SHUTDOWN, {})
        addr = self.stations[id]
        resp = requests.post(addr, data = kill_message.to_json())
        if resp.status_code == 200:
            print("Success in shutdown of ", id)

    def run(self):
        for event in self.events:
            is_rel = self.send_event(event)
            if is_rel:
                time.sleep(1)

        for station in self.stations:
            print("Killing station ", station, "...")
            self.kill_station(station)