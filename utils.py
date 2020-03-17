import zmq
import time
import enum
import socket
import pickle
import os 
# Functions
def configure_port(ipPort, portType, connectionType, openTimeOut=False):
    context = zmq.Context()
    socket = context.socket(portType)
    if(portType == zmq.SUB):
        socket.setsockopt_string(zmq.SUBSCRIBE, "")
    if(openTimeOut):
        socket.setsockopt(zmq.LINGER,      0)
        socket.setsockopt(zmq.AFFINITY,    1)
        socket.setsockopt(zmq.RCVTIMEO, 800)
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
        socket.setsockopt(zmq.RCVTIMEO,  800)
    if (isinstance(IPs, list)):
        for ip in IPs:
            socket.connect("tcp://" + ip + ":" + ports)
    else:
        tempPorts = ports.copy()
        random.shuffle(tempPorts)
        for port in tempPorts:
            print(port)
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
    #him that new leader was elected
    PID_MSG = 6
    NEW_LEADER = 7


class MachineType(enum.Enum):
    Leader = 1
    Member = 2

# Constants #
machinesNumber = 3
machineCommPort = "30000"
MachinesIPs = []
MachinesPID = []
LeaderPID = 0 
 
#to be set when leader send alive msg