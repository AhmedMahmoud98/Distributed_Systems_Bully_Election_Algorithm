import zmq
import time
import enum
import socket
import pickle
import os 
from contextlib import closing

def configure_port(ipPort, portType, connectionType, openTimeOut = False, 
                    Time = 0, subTopic = False, Topics = []):
    context = zmq.Context()
    socket = context.socket(portType)
    if(portType == zmq.SUB and subTopic == True):
        for topic in Topics:
            socket.setsockopt_string(zmq.SUBSCRIBE, topic)
    elif(portType == zmq.SUB and subTopic == False):
        socket.setsockopt_string(zmq.SUBSCRIBE, "")
    if(openTimeOut):
        socket.setsockopt(zmq.RCVTIMEO, Time)
        socket.setsockopt(zmq.LINGER,      0)
        socket.setsockopt(zmq.AFFINITY,    1)
    if(connectionType == "connect"):
        socket.connect("tcp://" + ipPort)
    else:
        socket.bind("tcp://" + ipPort)
    return socket, context

def setTimeOut(socket, Time):
    socket.setsockopt(zmq.RCVTIMEO, Time)
    socket.setsockopt(zmq.LINGER,      0)
    socket.setsockopt(zmq.AFFINITY,    1)

def configure_multiple_ports(IPs, port, portType, openTimeOut = False, 
                                Time = 0, subTopic = False, Topics = []):
    context = zmq.Context()
    socket = context.socket(portType)
    if(portType == zmq.SUB and subTopic == True):
         for topic in Topics:
            socket.setsockopt_string(zmq.SUBSCRIBE, topic)
    elif(portType == zmq.SUB and subTopic == False):
        socket.setsockopt_string(zmq.SUBSCRIBE, "")
    if(openTimeOut):
        socket.setsockopt(zmq.RCVTIMEO, Time)
        socket.setsockopt(zmq.LINGER,      0)
        socket.setsockopt(zmq.AFFINITY,    1)
    for IP in IPs:
        socket.connect("tcp://" + IP + ":" + port)
        time.sleep(1)
    return socket, context


def get_ip():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as s:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]


class MsgDetails(enum.Enum):
    LEADER_MEMBER_ALIVE = 1
    LEADER_AMBIGUITY = 2
    START_ELECITION = 3
    NETWORK_READY = 4
    REAL_LEADER = 5
    NEW_LEADER = 6
    START_MSG = 7
    ELECTION = 8
    PAUSE = 9
    OK = 10

# Constants #
NumOfMachines = 4
SubscribePort = "10000"
AlivePort = "20000"
MachinesIPs = ["192.168.1.13", "192.168.1.14", "192.168.1.15", "192.168.1.16"]
MachinesIDs = [ID for ID in range(NumOfMachines)]

