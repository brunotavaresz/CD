"""Message Broker"""
import enum
from typing import Dict, List, Any, Tuple
import socket
import selectors

from src.protocol import PubSub


class Serializer(enum.Enum):
    """Possible message serializers."""

    JSON = 0
    XML = 1
    PICKLE = 2


class Broker:
    """Implementation of a PubSub Message Broker."""
    
    def __init__(self):
        """Initialize broker."""
        self.canceled = False
        self._host = "localhost"
        self._port = 5000
        self.broker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
        self.broker.bind((self._host, self._port))
        self.selector = selectors.DefaultSelector()
        self.broker.listen()
        self.msg = {}          
        self.TypeSerial = {}       
        self.subs = {}    
        self.selector.register(self.broker, selectors.EVENT_READ, self.accept)

    def accept(self, broker, mask):
        """Accept a connection and store it's serialization type."""
        conn, addr = broker.accept()  
        self.selector.register(conn, selectors.EVENT_READ, self.read)

    def read(self, conn, mask):
        """Handle further operations"""

        date = PubSub.recv_msg(conn)
        if date:
            if date.type == "Sub":
                self.subscribe(date.topic, conn, self.getSerial(conn))

            elif date.type == "Pub":
                self.put_topic(date.topic, date.value)

                if date.topic in self.subs:
                    subscribers = self.list_subscriptions(date.topic)
                    for conn, serialType in subscribers:
                        PubSub.sendMsg(conn, self.getSerial(conn), date)

                else:
                    self.subs[date.topic] = []

            elif date.type == "CancelSub":
                self.unsubscribe(date.topic, conn)

            elif date.type == "ReqList":
                PubSub.sendMsg(conn, self.serialType, PubSub.topicListRep(self.list_topics()))

            elif date.type == "Ack" or date["type"] == "Ack":
                self.acknowledge(conn, date.lan)

        else:
            self.unsubscribe("", conn)
            self.selector.unregister(conn)
            conn.close()

    def list_topics(self) -> List[str]:
        """Returns a list of strings containing all topics containing values."""

        return [topic for topic in self.msg.keys() if self.msg[topic] is not None]


    def get_topic(self, topic):
        """Returns the currently stored value in topic."""

        return self.msg[topic] if topic in self.msg else None


    def put_topic(self, topic, value):
        """Store in topic the value."""

        if topic not in self.msg.keys():
            self.createTopic(topic)

        self.msg[topic] = value

    def list_subscriptions(self, topic: str) -> List[Tuple[socket.socket, Serializer]]:
        """Provide list of subscribers to a given topic."""

        return [(conn, self.TypeSerial[conn]) for conn in self.subs[topic]]


    def subscribe(self, topic: str, address: socket.socket, _format: Serializer = None):
        """Subscribe to topic by client in address."""
        codeSerial = _format

        if address not in self.TypeSerial:
            self.acknowledge(address, codeSerial)

        if topic in self.msg.keys():               
            if topic not in self.subs:
                self.createTopic(topic)

            if address not in self.subs[topic]:
                self.subs[topic].append(address)

            if topic in self.msg and self.msg[topic] is not None:
                PubSub.sendMsg(address, self.getSerial(address), PubSub.pub(topic, self.msg[topic]))
            return
        

        else:
            self.put_topic(topic, None)
            self.subscribe(topic, address, codeSerial)


    def unsubscribe(self, topic, address):
        """Unsubscribe to topic by client in address."""

        if topic != "":
            subtopics = [t for t in self.msg.keys() if t.startswith(topic)]
            for t in subtopics:
                self.subs[t] = [c for c in self.subs[t] if c != address]


        else:
            for t in self.msg.keys():
                if address in self.subs[t]:
                    self.subs[t] = [c for c in self.subs[t] if c != address]


    def acknowledge(self, conn, codeSerial):
        """Acknowledge new connection and its serialization type."""
        def set_json():
            self.TypeSerial[conn] = Serializer.JSON

        def set_xml():
            self.TypeSerial[conn] = Serializer.XML

        def set_pickle():
            self.TypeSerial[conn] = Serializer.PICKLE

        switch = {
            0: set_json,
            Serializer.JSON: set_json,
            None: set_json,
            1: set_xml,
            Serializer.XML: set_xml,
            2: set_pickle,
            Serializer.PICKLE: set_pickle,
        }
        switch.get(codeSerial, set_json)()

    def createTopic(self, topic):
        self.msg[topic] = None  
        self.subs[topic] = []

        i = 0
        while i < len(self.msg.keys()):
            t = list(self.msg.keys())[i]
            if topic.startswith(t):
                for consumer in self.subs[t]:
                    if consumer not in self.subs[topic]:
                        self.subs[topic].append(consumer)
            i += 1

    def getSerial(self, conn):
        return self.TypeSerial[conn] if conn in self.TypeSerial else None
            
    def run(self):
        """Run until canceled."""
        while not self.canceled:
            try:
                events = self.selector.select()
                for key, mask in events:
                    callback = key.data
                    callback(key.fileobj, mask)
            except KeyboardInterrupt:
                print("Caught keyboard interrupt, exiting")
                self.sock.close()
                        
           