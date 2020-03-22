import zmq
import time
import enum
import socket
import pickle
import os 
from contextlib import closing
# Functions
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

def get_PID():
    return os.getpid() 
class MsgDetails(enum.Enum):
    LEADER_MEMBER_ALIVE = 1
    START_ELECITION = 2
    ELECTION = 3
    OK = 4
    NOT_LEADER = 5 #msg from members to leader to inform 
    #him that new leader was elected #TODO at i'm alive process.
    START_MSG = 6
    IPs_LIST = 7
    NEW_LEADER = 8
    NETWORK_READY = 9


# Constants #
NumOfMachines = 4
PublishingPort = "10000"
AlivePort = "20000"
StartingMachineIP = "192.168.1.9"
MachinesIPs = ["192.168.1.9", "192.168.1.10", "192.168.1.11", "192.168.1.12"]
MachinesIDs = [ID for ID in range(NumOfMachines)]
