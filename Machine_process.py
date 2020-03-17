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
    #send my pid to other machines
    msg = {'id': MsgDetails.PID_MSG, 'msg': MyPID, 'ip': myIp}

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

def configMachinePorts(Machines):
    ipPort = myIp + ":" + MyPID%20000
    #setup publisher communication
    pubSocket, pubContext = configure_port(ipPort, zmq.PUB, 'bind')
    #connect to all other machines by port number which is PID%20000
    for PID,Mach in Machines.items():
        ip_port = Mach.ip+":"+PID%20000
        subsocket = configure_port(ip_port,zmq.SUB,"connect")
    return pubSocket,subsocket
    
def start_election(MachinesPID,pubsocket,subsocket):
    index = MachinesPID.index(MyPID)
    count = 0
    for(i in range(1,4)):
        #send to 3 machines that has higher rank
        #and receive ok from them 
        #if at least one Ok msg has received then
        # it job is done 
        # if not then it become leader and send 
        # to all machine that it is the leader  
        msg = {'id': MsgDetails.ELECTION, 'msg': "Election command", 'ip': myIp,'PID'=MyPID,'topicfilter' = MachinesPID[index+i]}
        pubSocket.send(pickle.dumps(msg))
        
        #receive ok msg from machine with pid that i have just send command to.
        receivedMessage = pickle.loads(subsocket.recv())
        if(receivedMessage['topicfilter'] == MyPID and receivedMessage['id']==MsgDetails.OK):
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
    PUB_socket,subsocket=configMachinePorts(Machines)
    start_election(MachinesPID,PUB_socket,subsocket)
    
    #run i'm alive process after determined the leader
    p = multiprocessing.Process(target=Alive_process,
                                args=(Machines[MyPID].isLeader, MachinesIPs))
    p.start()  # ...and run!
    
    while(True):
        #msgs received from machine and its behaviour
        receivedMessage = pickle.loads(subsocket.recv())
        if(receivedMessage['id'] == MsgDetails.START_ELECITION):
            #wait second then start to send election msgs to elect new leader
            time.sleep(1)
            start_election(MachinesPID,PUB_socket,subsocket)
        #else if(receivedMessage['id'] == MsgDetails.ELECTION):
                #receiving election msg . then if machine is alive send OK
                # to the machine that send msg to me.
        else if(receivedMessage['id'] == MsgDetails.NEW_LEADER):
                #if the machine was leader and wake up again then it make it self as member
                Machine[MyPID].isLeader=False
                #set the leader PID
                LeaderPID = receivedMessage['PID']
                        
                    




MyPID=os.getpid()
MyIP=get_ip()
Machines = {}
ports = []
pubSocket, pubContext = None
subsocket = None
#PID is key and socket is SUb socket to receive from other machines
machine_socket={}
#key is machine process pid and value is machine object
Machines[MyPID]=Machine(MyIP,MyPID,ports,True,False)
MachinesPID.append(MyPID)

Machine_process(MachinesPID,MachinesIPs)