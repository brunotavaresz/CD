"""CD Chat client program"""
import logging
import sys
import socket
import selectors
import fcntl
import os

from .protocol import CDProto

logging.basicConfig(filename=f"{sys.argv[0]}.log", level=logging.DEBUG)
HOST='localhost'
PORT=5005


class Client:
    """Chat Client process."""

    def __init__(self, name: str = "Foo"):
        """Initializes chat client."""
        self.username = name
        self.channel = None
        self.client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # socket do cliente
        self.selector = selectors.DefaultSelector() # seletor de eventos
        self.selector.register(sys.stdin, selectors.EVENT_READ, self.send_msg)
        self.selector.register(self.client_sock, selectors.EVENT_READ, self.receive_msg)
        orig_fl = fcntl.fcntl(sys.stdin, fcntl.F_GETFL)
        fcntl.fcntl(sys.stdin, fcntl.F_SETFL, orig_fl | os.O_NONBLOCK)
        print("Client running...")
        

    def connect(self):
        """Connect to chat server and setup stdin flags."""
        try:
            self.client_sock.connect((HOST, PORT))
            self.client_sock.setblocking(False)
        except Exception as e:
            print(f"Error connecting to server: {e}")
            sys.exit()
        print("Connected to server successfully as", self.username)
    
    def send_msg(self):
        """Send message to server."""
        message = sys.stdin.readline().strip()
        if message.split()[0] == "/join":
            self.channel = message.split()[1]
            msg = CDProto.join(self.channel)
            print(f"{self.username} joined {self.channel}")
        elif message == "exit":
            self.selector.unregister(self.client_sock)
            self.client_sock.close()
            print("{} disconnected".format(self.username))
            sys.exit()
        else:
            msg = CDProto.message(message, self.channel)

        try:
            CDProto.send_msg(self.client_sock, msg)
        except Exception as e:
            logging.error(f"Error sending message: {str(e)}")
            sys.exit()
        
    
    def receive_msg(self):
        """Receive message from server."""
        receive = CDProto.recv_msg(self.client_sock)

        if receive is None:
            print("Server is down")  
            self.selector.unregister(self.client_sock)
            self.client_sock.close()
            sys.exit()
        else:
            print(receive)


    def loop(self):
        """Loop indefinetely."""

        register = CDProto.register(self.username)
        CDProto.send_msg(self.client_sock, register)

        while True:
            events = self.selector.select()
            for key, mask in events:
                callback = key.data
                callback()