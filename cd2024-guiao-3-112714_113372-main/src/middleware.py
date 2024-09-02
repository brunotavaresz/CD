"""Middleware to communicate with PubSub Message Broker."""
from collections.abc import Callable
from enum import Enum
import socket
from typing import Any

from src.protocol import PubSub


class MiddlewareType(Enum):
    """Middleware Type."""

    CONSUMER = 1
    PRODUCER = 2


class Queue:
    """Representation of Queue interface for both Consumers and Producers."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        """Create Queue."""
        self.topic = topic
        self.type = _type
        self.port = 5000
        self.host = "localhost"
        self.codeSerial = 0
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))

    def push(self, value):
        """Sends data to broker."""
        if self.type.value == 2:
            PubSub.sendMsg(self.socket, self.codeSerial, PubSub.pub(self.topic, value))

    def pull(self) -> (str, Any):
        """Receives (topic, data) from broker.
        Should BLOCK the consumer!"""
        date = PubSub.recv_msg(self.socket)
        if date and date.value:
            if date.type == "ReplyList":
                return date.list
            else:
                return (date.topic, int(date.value))
        else:
            return None

    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        PubSub.sendMsg(self.socket, self.codeSerial, PubSub.topicListReq())

    def cancel(self):
        """Cancel subscription."""
        PubSub.sendMsg(self.socket, self.codeSerial, PubSub.cancelSubs(self.topic))

    

class JSONQueue(Queue):
    """Queue implementation with JSON based serialization."""

    def __init__(self, topic, _type = MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        self.codeSerial = 0
        if _type == MiddlewareType.CONSUMER:
            PubSub.sendMsg(self.socket, 0, PubSub.ack(self.codeSerial))
            PubSub.sendMsg(self.socket, self.codeSerial, PubSub.sub(self.topic))

class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""
    def __init__(self, topic, _type = MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        self.codeSerial = 1
        if _type == MiddlewareType.CONSUMER:
            PubSub.sendMsg(self.socket, 0, PubSub.ack(self.codeSerial))
            PubSub.sendMsg(self.socket, self.codeSerial, PubSub.sub(self.topic))

class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""
    def __init__(self, topic, _type = MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        self.codeSerial = 2
        if _type == MiddlewareType.CONSUMER:
            PubSub.sendMsg(self.socket, 0, PubSub.ack(self.codeSerial))
            PubSub.sendMsg(self.socket, self.codeSerial, PubSub.sub(self.topic))