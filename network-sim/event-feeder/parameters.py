from event import Event
import csv, datetime

class SimulationParamGenerator:    
    @staticmethod
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

                start_event = Event(bike_id, start_time, Event.EventType.BIKE_TAKE, src, dest)
                end_event = Event(bike_id, end_time, Event.EventType.BIKE_RETURN, src, dest)

                events.append(start_event)
                events.append(end_event)
                bike_ids.add(bike_id)
                hub_ids.add(src)
                hub_ids.add(dest)
            except Exception as exc:
                print("Corrupt data?", exc)
                print("\t", trip)

        sorted_events = sorted(events, key = lambda e: e.time)
        return sorted_events, bike_ids, hub_ids