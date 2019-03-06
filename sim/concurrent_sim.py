import csv, datetime, time, random, queue
from multiprocessing import Queue, Process
from threading import Thread, Lock
from collections import defaultdict

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

class Messaging:
    """
    Messaging abstraction used by Hubs that maintains two queues for inter-process communication.

    Events to be processed get sent into `inbox_queue` and receipt of processing gets sent back
    to the parent via the `parent_queue.`
    """
    class Ack:
        """ A "receipt" that an event has been processed. """
        def __init__(self, event):
            self.bike = event.bike
            self.type = event.type

    def __init__(self, parent_queue=None):
        self.parent_queue = parent_queue
        self.inbox_queue = Queue()

    def ack_event(self, event):
        """(<- Out) Acknowledge to PARENT queue that an event was processed."""
        self.parent_queue.put(Messaging.Ack(event))

    def get_event(self):
        """Pop off the top element from inbox_queue."""
        return self.inbox_queue.get()

    def send_in(self, obj):
        """(-> In) Send an event INTO this Messaging object's inbox_queue."""
        self.inbox_queue.put(obj)

class Hub:
    """
    Models a Bike Hub.
    """
    def __init__(self, hub_id, name, parent_queue):
        self.id = hub_id
        self.name = name
        self.messaging = Messaging(parent_queue=parent_queue)

    def take_bike(self, bike_id):
        # TODO: Implement notif to cloud
        print("{} : Bike {} was taken".format(self.name, bike_id))

    def return_bike(self, bike_id):
        # TODO: Implement notif to cloud
        print("{} : Bike {} was returned".format(self.name, bike_id))

    def process_event(self, event):
        """ Processes a given event"""
        print("Timestamp: {}".format(event.time))
        if event.type == Event.EventType.TAKE:
            self.take_bike(event.bike)
        elif event.type == Event.EventType.RETURN:
            self.return_bike(event.bike)
        else:
            raise RuntimeError
        print()
        # Let simulation parent know message has been published
        self.messaging.ack_event(event)

    def run(self):
        """
        "Forever"-event loop.
        Tries to get an event, process it, sleep for 0-1 sec and then repeat.
        """
        while True:
            event = self.messaging.get_event()
            self.process_event(event)
            sleep_t = random.randint(0, 10) * 0.01
            time.sleep(sleep_t)

class Simulation:
    """
    Main simulation class.

    Instantiates everything necessary for a simulation based on some input parameters (event, bikes, hub_ids).
    Has a `run` method that starts child processes and feeds events out to the children.
    """

    def __init__(self, events, bikes, hub_ids):
        self.queue = Queue()
        self.hubs = { hub_id :  Hub(hub_id, "Hub no. {}".format(hub_id), self.queue) for hub_id in hub_ids }
        # self.bikes = bikes # Prob unnecessary
        self.event_feed = events

        self.racked_bikes = set()
        self.out_bikes = set()

        self.waiting_events_lock = Lock()
        self.waiting_events = defaultdict(list)

    def process_event(self, event, is_waiting_event_retry=False):
        """
        Decides what to do with an event.
        The flag `is_waiting_event_retry` is for retrying an event in the waiting pool
        """
        with self.waiting_events_lock:
            if event.type == Event.EventType.TAKE:
                target = event.src
                # This means that we have never before seen this bike
                if event.bike not in self.out_bikes and event.bike not in self.racked_bikes:
                    self.racked_bikes.add(event.bike)
                # Process the bike check-out event
                if event.bike in self.racked_bikes:
                    self.hubs[target].messaging.send_in(event)
                else:
                    if not is_waiting_event_retry:
                        self.waiting_events[event.bike] += [event]
            else: # Event.EventType.RETURN
                target = event.dest
                # Process the bike return event
                if event.bike in self.out_bikes:
                    self.hubs[target].messaging.send_in(event)
                else:
                    if not is_waiting_event_retry:
                        self.waiting_events[event.bike] += [event]

    def feed_events(self):
        """
        This is the method that feeds new events into the imulation. 
        Creates child processes for the hubs and feeds events to them. 
        """
        # Map all hub_ids to hub processes
        hub_processes = { hub_id : Process(target = hub.run) for hub_id, hub in self.hubs.items() }

        # Start all hub processes
        for hub_proc in hub_processes.values(): hub_proc.start()
        
        # Process all events in the event feed
        for event in self.event_feed:
            self.process_event(event)
            time.sleep(random.randint(0, 1) * 0.1)

        # Join all hub processes before exiting 
        # TODO ... hmm might be janky since these technically don't finish as is...
        for hub_proc in hub_processes.values():
            hub_proc.join()

    def process_ack_messages(self):
        """Event-loop that processes the ack's (receipts) from child hubs that
        certain events have been published."""
        while True:
            try:
                ack = self.queue.get_nowait()
                if ack.type == Event.EventType.TAKE:
                    self.racked_bikes.remove(ack.bike)
                    self.out_bikes.add(ack.bike)
                else: # Event.EventType.RETURN:
                    self.out_bikes.remove(ack.bike)
                    self.racked_bikes.add(ack.bike)
                # Retry first event that depends on this bike
                if len(self.waiting_events[ack.bike]):
                    event = self.waiting_events[ack.bike][0]
                    self.process_event(event, True) # Will leave event in place if fail
            except queue.Empty:
                pass
            time.sleep(0.001)

    def run(self):
        sim_thread = Thread(target=self.feed_events)
        msg_thread = Thread(target=self.process_ack_messages)

        sim_thread.start()
        msg_thread.start()

        sim_thread.join()
        msg_thread.join()

class SimulationParamGenerator:
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

"""
Sample usage:

from concurrent_sim import *
# Generate params from file. Returns ( events: List of events, bikes : set of bike ids, hubs : set of hub ids )
params = SimulationParamGenerator.generate("metro-bike-share-trips-2017-q4-v2.csv")
# Init simulation
sim = Simulation(params[0], params[1], params[2])
# Run the sim
sim.run()
"""