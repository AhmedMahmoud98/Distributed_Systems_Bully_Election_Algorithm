import pprint
import sys
import time
import zmq
import signal
from contextlib import contextmanager
from utils import *
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os

def send_get_PIDs(MachinesPID):
    # Configure myself as publisher  with master
    ipPort = myIp + ":" + machineCommPort
    pubSocket, pubContext = configure_port(get_ip(), zmq.PUB, 'bind')
    # I'm Alive Msg that will be sent periodically
    msg = {'id': MsgDetails.PID_MSG, 'msg': MyPID, 'ip': myIp}

    # Periodically 1 sec
    pubSocket.send(pickle.dumps(msg))
    #setup subscriber communication
    subSocket, subContext = configure_multiple_ports(MachinesIPs,
                                                     machineCommPort, zmq.SUB)
            
    for i in range(machinesNumber-1):
        #receive alive msg from leader    
        receivedMessage = pickle.loads(subSocket.recv())
        #receive msg from other machines and create objects 
        #port in object is pub socket port
        if(receivedMessage['id'] == MsgDetails.PID_MSG):
            PID=MachinesPID.append(receivedMessage['PID'])
            Machines[PID]= Machine(receivedMessage['ip'],PID,PID%20000,True,False) 

def configMachinePorts(Machines,machine_socket):
    ipPort = myIp + ":" + MyPID%20000
    #setup publisher communication
    pubSocket, pubContext = configure_port(ipPort, zmq.PUB, 'bind')
    #connect to all other machines by port number which is PID%20000
    for PID,Mach in Machines.items():
        ip_port = Mach.ip+":"+PID%20000
        machine_socket[PID] = configure_port(ip_port,zmq.SUB,"connect")
    return pubSocket,machine_socket
    
def start_election(MachinesPID,pubsocket,machine_socket):
    index = MachinesPID.index(MyPID)
    count = 0
    for(i in range(1,4)):
        #send to 3 machines that has higher rank
        #and receive ok from them 
        #if at least one Ok msg has received then
        # it job is done 
        # if not then it become leader and send 
        # to all machine that it is the leader  
        msg = {'id': MsgDetails.ELECTION, 'msg': "Election command", 'ip': myIp,'PID'=MyPID}
        pubSocket.send(pickle.dumps(msg))
        receivedMessage = pickle.loads(machine_socket[MachinesPID[index+i]].recv())
        if(receivedMessage['id']==MsgDetails.OK):
            count+=1
    #if count equal 0 then this machine become the leader.
    if(count==0):
        LeaderPID = MyPID
        Machines[MyPID].isLeader=True
        msg = {'id': MsgDetails.NEW_LEADER, 'msg': "i'm the new leader", 'ip': myIp,'PID'=MyPID}
        #send msgs to other machines
        for in range(machinesNumber-1):
            pubSocket.send(pickle.dumps(msg))        



def Machine_process(MachinesPID,MachinesIPs):
    send_get_PIDs(MachinesPID)
    MachinesPID.sort(reverse=False)
    PUB_socket,machine_socket=configMachinePorts(Machines,machine_socket)
    start_election(MachinesPID,PUB_socket,machine_socket)
    
    #run i'm alive process after determined the leader
    p = multiprocessing.Process(target=Alive_process,
                                args=(Machines[MyPID].isLeader, MachinesIPs))
    p.start()  # ...and run!
    
    while(True):
        #msgs received from machine and its behaviour






MyPID=os.getpid()
MyIP=get_ip()
Machines = {}
ports = []
pubSocket, pubContext = none
#PID is key and socket is SUb socket to receive from other machines
machine_socket={}
#key is machine process pid and value is machine object
Machines[MyPID]=Machine(MyIP,MyPID,ports,True,False)
MachinesPID.append(MyPID)