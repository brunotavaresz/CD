"""Protocol for chat server - Computação Distribuida Assignment 1."""
import json
from datetime import datetime
from socket import socket


class Message:
    """Message Type."""
    def __init__(self, command):
        self.command = command
    
class JoinMessage(Message):
    """Message to join a chat channel."""
    def __init__(self,command,channel):
        super().__init__("join")
        self.channel = channel

    def __str__(self):
        return f'{{"command": "{self.command}", "channel": "{self.channel}"}}'

class RegisterMessage(Message):
    """Message to register username in the server."""
    def __init__(self,command, user):
        super().__init__("register")
        self.user = user

    def __str__(self):
        return f'{{"command": "{self.command}", "user": "{self.user}"}}'

    
class TextMessage(Message):
    """Message to chat with other clients."""
    def __init__(self,command, message,channel = None):
        super(TextMessage, self).__init__(command)
        self.message = message
        self.channel = channel

    def __str__(self):
        if self.channel == None:
            return f'{{"command": "{self.command}", "message": "{self.message}", "ts": {int(datetime.utcnow().timestamp())}}}'
        else:
            return f'{{"command": "{self.command}", "message": "{self.message}", "channel": "{self.channel}", "ts": {int(datetime.utcnow().timestamp())}}}'


class CDProto:
    """Computação Distribuida Protocol."""

    @classmethod
    def register(cls, username: str) -> RegisterMessage:
        """Creates a RegisterMessage object."""
        return RegisterMessage("register", username)


    @classmethod
    def join(cls, channel: str) -> JoinMessage:
        """Creates a JoinMessage object."""
        return JoinMessage("join", channel)

    @classmethod
    def message(cls, message: str, channel: str = None) -> TextMessage:
        """Creates a TextMessage object."""
        return TextMessage("message", message, channel)

    @classmethod
    def send_msg(cls, connection: socket, msg: Message):
        """Sends through a connection a Message object."""
        if type(msg) is RegisterMessage:
            data = json.dumps({"command": "register", "user": msg.user})
        elif type(msg) is JoinMessage:
            data = json.dumps({"command": "join", "channel": msg.channel})
        elif type(msg) is TextMessage:
            if msg.channel == None:
                data = json.dumps({"command": "message", "message": msg.message, "ts": int(datetime.utcnow().timestamp())})
            else:
                data = json.dumps({"command": "message", "message": msg.message, "channel": msg.channel, "ts": int(datetime.utcnow().timestamp())})
        
        header = len(data).to_bytes(2, "big")
        data = data.encode("utf-8")
        connection.sendall(header + data)

    @classmethod
    def recv_msg(cls, connection: socket) -> Message:
        """Receives through a connection a Message object."""
        header = connection.recv(2)
        if header:
            header = int.from_bytes(header, "big")
            msg = connection.recv(header).decode("utf-8")
            
            try:
                msg = json.loads(msg)
            except :
                raise CDProtoBadFormat(msg)

            if msg["command"] == "register":
                return RegisterMessage("register",msg["user"])
            elif msg["command"] == "join":
                return JoinMessage("join",msg["channel"])
            elif msg["command"] == "message":
                if "channel" in msg:
                    return TextMessage("message",msg["message"], msg["channel"])
                else:
                    return TextMessage("message",msg["message"])
            else:
                raise CDProtoBadFormat(msg)
        else:
            return None
                      
class CDProtoBadFormat(Exception):
    """Exception when source message is not CDProto."""

    def __init__(self, original_msg: bytes=None) :
        """Store original message that triggered exception."""
        self._original = original_msg

    @property
    def original_msg(self) -> str:
        """Retrieve original message as a string."""
        return self._original.decode("utf-8")