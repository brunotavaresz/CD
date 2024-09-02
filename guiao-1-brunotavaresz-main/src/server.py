"""CD Chat server program."""
import logging
import selectors
import socket
from src.protocol import CDProto, CDProtoBadFormat

logging.basicConfig(filename="server.log", level=logging.DEBUG)
HOST='localhost'
PORT=5005

class Server:
    PORT
    """Chat Server process."""
    def __init__(self):
        self.selector = selectors.DefaultSelector()
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        self.server.bind(((HOST, PORT)))
        self.server.listen(110)
        self.selector.register(self.server, selectors.EVENT_READ, self.accept)
        self.dic = { None : []}
        print("Server running...") 
    
    def accept(self,sock, mask):
        conn, addr = sock.accept() # Should be ready
        print(f"[NEW CONNECTION] {addr} connected.")
        conn.setblocking(False)
        self.selector.register(conn, selectors.EVENT_READ, self.read)
        self.dic[conn] = [None] # None is the default channel

    def read(self, conn, mask):
        try:
            msg = CDProto.recv_msg(conn)
        except CDProtoBadFormat:
            print("Bad message format")

        if msg is not None:
            if msg.command == "register":
                CDProto.send_msg(conn, msg)
            elif msg.command == "message":
                for key,value in self.dic.items(): 
                        if msg.channel in value: # if the channel is in the list of channels send the message
                            CDProto.send_msg(key, msg)
                        else:
                            print("Channel not found")
            elif msg.command == "join":
                if msg.channel not in self.dic[conn]: # if the channel is not in the list of channels join the channel
                    self.dic[conn].append(msg.channel)
                else: 
                    print("Channel already joined")
                if self.dic[conn] == None:
                    self.dic[conn].remove(None)                
        else:
            print(f"[CONNECTION CLOSED] {conn}")
            self.selector.unregister(conn)
            del self.dic[conn]
            conn.close()
            

    def loop(self):
        """Loop indefinetely."""
        while True:
            events = self.selector.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)