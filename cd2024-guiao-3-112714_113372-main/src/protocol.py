import json
import pickle
import xml.etree.ElementTree as ET
import enum
import socket

class Serializer(enum.Enum):
    """Serializers."""

    JSON = 0
    XML = 1
    PICKLE = 2

class Message:
    """Message."""

    def __init__(self, type):
        self.type = type

class Sub(Message):
    """Message subscribe a topic"""

    def __init__(self, topic):
        super().__init__("Sub")
        self.topic = topic

    def __repr__(self):
        return f'{{"type": "{self.type}", "topic": "{self.topic}"}}'

    def toXML(self):
        return "<?xml version=\"1.0\"?><data type=\"{}\" topic=\"{}\"></data>".format( self.type, self.topic )

    def toPickle(self):
        return {"type": self.type, "topic": self.topic}

class Pub(Message):
    """Message publish topic"""

    def __init__(self, topic, value):
        super().__init__("Pub")
        self.topic = topic
        self.value = value

    def __repr__(self):
        return f'{{"type": "{self.type}", "topic": "{self.topic}", "value": "{self.value}"}}'

    def toXML(self):
        return "<?xml version=\"1.0\"?><data type=\"{}\" topic=\"{}\" value=\"{}\"></data>".format( self.type, self.topic, self.value )

    def toPickle(self):
        return {"type": self.type, "topic": self.topic, "value": self.value}

class ReqList(Message):
    """Message request topic list"""

    def __init__(self):
        super().__init__("ReqList")

    def __repr__(self):
        return f'{{"type": "{self.type}"}}'

    def toXML(self):
        return "<?xml version=\"1.0\"?><data type=\"{}\"></data>".format( self.type )

    def toPickle(self):
        return {"type": self.type}

class RepList(Message):
    """Message reply topic list"""
    
    def __init__(self, list):
        super().__init__("RepList")
        self.list = list

    def __repr__(self):
        return f'{{"type": "{self.type}", "list": "{self.list}"}}'

    def toXML(self):
        return "<?xml version=\"1.0\"?><data type=\"{}\"></data>".format( self.type )


    def toPickle(self):
        return {"type": self.type, "list": self.list}

class CancelSub(Message):
    """Message cancel topic"""

    def __init__(self, topic):
        super().__init__("CancelSub")
        self.topic = topic

    def __repr__(self):
        return f'{{"type": "{self.type}", "topic": "{self.topic}"}}'

    def toXML(self):
        return "<?xml version=\"1.0\"?><data type=\"{}\" topic=\"{}\"></data>".format( self.type, self.topic )

    def toPickle(self):
        return {"type": self.type, "topic": self.topic}

class Ack(Message):
    """Message to inform broker of your language"""

    def __init__(self, lan):
        super().__init__("Ack")
        self.lan = lan

    def __repr__(self):
        return f'{{"type": "{self.type}", "lan": "{self.lan}"}}'

    def toXML(self):
        return "<?xml version=\"1.0\"?><data type=\"{}\" topic=\"{}\"></data>".format( self.type, self.topic )

    def toPickle(self):
        return {"type": self.type, "lan": self.lan}

class PubSub:
    @classmethod
    def sub(cls, topic) -> Sub:
        return Sub(topic)

    @classmethod
    def pub(cls, topic, value) -> Pub:
        return Pub(topic, value) 

    @classmethod
    def ReqList(cls) -> ReqList:
        return ReqList()

    @classmethod
    def RepList(cls, list) -> RepList:
        return RepList(list)

    @classmethod
    def cancelSub(cls, topic) -> CancelSub:
        return CancelSub(topic)

    @classmethod
    def ack(cls, lan) -> Ack:
        return Ack(lan)

    @classmethod
    def sendMsg(cls, conn: socket, SerCode, msg: Message):
        """Send msg"""

        if SerCode == None: 
            SerCode = 0
        if type(SerCode) == str: 
            SerCode = int(SerCode)
        if isinstance(SerCode, enum.Enum): 
            SerCode = SerCode.value

        conn.send(SerCode.to_bytes(1, 'big'))
        if SerCode == 2 or SerCode == Serializer.PICKLE:
            mPick = pickle.dumps(msg.toPickle())
            hPick = len(mPick).to_bytes(2, 'big')
            msgf = hPick + mPick
            conn.send(msgf)
        elif SerCode == 1 or SerCode == Serializer.XML:
            mXML = msg.toXML().encode('utf-8')
            hXML = len(mXML).to_bytes(2, 'big')
            msgf = hXML + mXML
            conn.send(msgf)
        elif SerCode == 0 or SerCode == Serializer.JSON:
            msgI = json.loads(msg.__repr__())    
            mJSON = json.dumps(msgI).encode('utf-8')
            hJSON = len(mJSON).to_bytes(2, 'big')
            msgf = hJSON + mJSON
            conn.send(msgf)


    @classmethod
    def recv_msg(cls, conn: socket) -> Message:
        """Receive msg"""

        SerCode = int.from_bytes(conn.recv(1), 'big')
        header = int.from_bytes(conn.recv(2), 'big')
        if not header:
            return None

        try:
            if SerCode == Serializer.JSON or SerCode == 0:
                date = conn.recv(header)
                msgenc = date.decode('utf-8')    
                if msgenc == None:
                    return None    
                               
                msg = json.loads(msgenc)
            
            elif SerCode == Serializer.PICKLE or SerCode == 2:
                msgenc = conn.recv(header)
                if msgenc == None:
                    return None
                
                msg = pickle.loads(msgenc)

            elif SerCode == Serializer.XML or SerCode == 1:
                date = conn.recv(header)
                msgenc = date.decode('utf-8') 
                if msgenc == None:
                    return None
                
                msg = {}
                root = ET.fromstring(msgenc)
                for child in root.keys():                               
                    msg[child] = root.get(child)   

        except json.JSONDecodeError as err:
            raise CDProtoBadFormat(msgenc)

        if msg["type"] == "Sub":
            return cls.sub(msg["topic"])
        elif msg["type"] == "Pub":
            return cls.pub(msg["topic"], msg["value"])
        elif msg["type"] == "RepList":
            return cls.RepList(msg["list"])
        elif msg["type"] == "ReqList":
            return cls.ReqList()
        elif msg["type"] == "CancelSub":
            return cls.cancelSub(msg["topic"])
        elif msg["type"] == "Ack":
            return cls.ack(msg["lan"])
        else:
            return None

class CDProtoBadFormat(Exception):

    def __init__(self, original_msg: bytes=None) :
        self._original = original_msg

    @property
    def original_msg(self) -> str:
        return self._original.decode("utf-8")