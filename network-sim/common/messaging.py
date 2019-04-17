#! /usr/bin/env python
# Python 2.7
import SocketServer, json, time
from BaseHTTPServer import BaseHTTPRequestHandler
from multiprocessing import Queue
from threading import Thread

class Message:
    def __init__(self, type_, info):
        self.type = type_
        self.info = info

    def to_json(self):
        return json.dumps(self.__dict__, default=str)

    @staticmethod
    def from_json(json_string):
        converted_dict = json.loads(json_string)
        return Message(converted_dict["type"], converted_dict["info"])

    class MessageType:
        # Enum for the type of message
        BIKE_TAKE, BIKE_RETURN, SHUTDOWN, RECEIPT = 0, 1, 2, 3

class Messaging:
    def __init__(self,port=8080):
        self.q = Queue()
        handler = MakeHandlerClass.make(self.q)
        self.httpd = SocketServer.TCPServer(("", port), handler)
        self.httpd.q = self.q
        self.is_listening = False

    def get_message(self):
        return self.q.get()

    def get_message_nowait(self):
        return self.q.get_nowait()
    
    def start_listening(self):
        try:
            self.is_listening = True
            server_thread = Thread(target=self.httpd.serve_forever)
            server_thread.daemon = True
            server_thread.start()
            server_thread.join()
        except KeyboardInterrupt:
            self.shutdown()

    def send_message(self, to, msg):
        # TODO: Implement!
        pass

    def shutdown(self):
        self.is_listening = False
        self.httpd.shutdown()

class MakeHandlerClass:
    @staticmethod
    def make(msg_queue):
        class RequestHandler(BaseHTTPRequestHandler):
            q = msg_queue
            def __init__(self, *args, **kwargs):
                BaseHTTPRequestHandler.__init__(self, *args, **kwargs)

            def add_message_to_q(self, msg):
                if RequestHandler.q:
                    RequestHandler.q.put(Message.from_json(msg))
                
            def do_POST(self):
                # Read payload
                content_len = int(self.headers.getheader('content-length', 0))
                payload = self.rfile.read(content_len)
                # Convert & Add to message queue
                self.add_message_to_q(payload)
                # OK everything
                self.send_response(200)
        return RequestHandler