import pprint
import sys
import time
import zmq
import signal
from contextlib import contextmanager
from utils import *
from datetime import datetime
from dateutil.relativedelta import relativedelta

def Alive_process(isLeader,MachinesIPs):
    # Configure myself as subscriber all machines
    ipPort = myIp + ":" + machineCommPort
    MyPID = get_PID()
    while True:
        #leader send periodically i'm alive msg to others machines
        if(isLeader == True):
            # Terminate Connection
            subsocket.close()
            subcontext.destroy()
            #setup publisher communication
            pubSocket, pubContext = configure_port(ipPort, zmq.PUB, 'bind')
            # I'm Alive Msg that will be sent periodically
            msg = {'id': MsgDetails.LEADER_MEMBER_ALIVE, 'msg': "I'm Alive", 'ip': myIp,'PID'=MyPID,'MachineType'=MachineType.Leader}
            
            # Periodically 1 sec
            pubSocket.send(pickle.dumps(msg))
            time.sleep(1)
        else:#
            # Terminate Connection
            pubsocket.close()
            pubcontext.destroy()
            #setup subscriber communication
            subSocket, subContext = configure_multiple_ports(MachinesIPs,
                                                     machineCommPort, zmq.SUB)
            #receive alive msg from leader
            receivedMessage = pickle.loads(subSocket.recv())
            if(not receivedMessage):
                #no message recieved . so send messages command start election
                for i in range(machinesNumber-1):
                    msg = {'id': MsgDetails.START_ELECITION, 'msg': "start election command", 'ip': myIp}
                    # Periodically 1 sec
                    pubSocket.send(pickle.dumps(msg))
                #TODO if received alive msg with pid not equal to leader pid
                # send to leader that new leader is elected    

