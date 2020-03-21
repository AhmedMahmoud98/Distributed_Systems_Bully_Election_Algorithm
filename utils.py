import zmq
import time
import enum
import socket
import pickle
import os 
from contextlib import closing
# Functions
def configure_port(ipPort, portType, connectionType, openTimeOut=False):
    context = zmq.Context()
    socket = context.socket(portType)
    if(portType == zmq.SUB):
        socket.setsockopt_string(zmq.SUBSCRIBE, "")
    if(openTimeOut):
        socket.setsockopt(zmq.LINGER,      0)
        socket.setsockopt(zmq.AFFINITY,    1)
        socket.setsockopt(zmq.RCVTIMEO, 1000)
    if(connectionType == "connect"):
        socket.connect("tcp://" + ipPort)
    else:
        socket.bind("tcp://" + ipPort)
    return socket, context


def configure_multiple_ports(IPs, ports, portType, openTimeOut=False):
    context = zmq.Context()
    socket = context.socket(portType)
    if(portType == zmq.SUB):
        socket.setsockopt_string(zmq.SUBSCRIBE, "")
    if(openTimeOut):
        socket.setsockopt(zmq.LINGER,      0)
        socket.setsockopt(zmq.AFFINITY,    1)
        socket.setsockopt(zmq.RCVTIMEO,  700)
    if(isinstance(IPs, list) and isinstance(ports, range)):
        for i in range(len(IPs)):
            socket.connect("tcp://" + IPs[i] + ":" + str(ports[i]))

    elif (isinstance(IPs, list)):
        for ip in IPs:
            socket.connect("tcp://" + ip + ":" + ports)
    else:
        tempPorts = ports.copy()
        random.shuffle(tempPorts)
        for port in tempPorts:
           
            socket.connect("tcp://" + IPs + ":" + port)
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
    PID_MSG = 6
    PID_LIST = 7
    NEW_LEADER = 8


class MachineType(enum.Enum):
    Leader = 1
    Member = 2

# Constants #
machinesNumber = 4
machineCommPort = "5556"
MachinesIPs = [get_ip(),get_ip(),get_ip(),get_ip()]
MachinesPID = [get_PID()]


