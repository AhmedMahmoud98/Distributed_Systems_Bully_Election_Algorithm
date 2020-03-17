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
from Machine import *

PIDTransferPort=int(sys.argv[1]) #pub port number 10000
#sub port number will be same number +10000
#then if machine found that this port number is 10000 then it know that it's first machine then it start to communicate with ports 20001,20002,...20000N

MyPID=os.getpid()
MyIP=get_ip()
Machines = {}
ports = []
pubSocket = None
pubContext = None
subsocket = None
#PID is key and socket is SUb socket to receive from other machines
machine_socket={}
#key is machine process pid and value is machine object
Machines[MyPID]=Machine(MyIP,MyPID,ports,True,False)


def send_get_PIDs(myIp,MachinesPID,pubSocket,subSocket):
    #send my pid to other machines
    msg = {'id': MsgDetails.PID_MSG, 'msg': MyPID, 'ip': myIp,'PubPORT':PIDTransferPort}
    print("msg to send to other machines{}".format(msg))
    pubSocket.send(pickle.dumps(msg))
    print("pid is send")

    for i in range(machinesNumber-1):
        print("before receive pid")
        #receive alive msg from leader    
        receivedMessage = pickle.loads(subSocket.recv())
        print("after receive pid msg :{}".format(receivedMessage))
        #receive msg from other machines and create objects 
        #port in object is pub socket port
        if(receivedMessage['id'] == MsgDetails.PID_MSG):
            PID=receivedMessage['PID']
            MachinesPID.append(PID)
            Machines[PID]= Machine(receivedMessage['ip'],PID,receivedMessage['pubPORT'],True,False) 

def configMachinePorts(Machines,myIp):
    ipPort = myIp + ":" + str(PIDTransferPort)
    print(ipPort)
    #setup publisher communication
    pubSocket, pubContext = configure_port(ipPort, zmq.PUB, 'bind')

    #connect to all other machines by port number which is PIDTransferPort+10000+count
    count=0 #to Machines number - 1
    print("iports of sub socket")
    for IP in MachinesIPs:
        #check if this port is mine
        if not (PIDTransferPort == (10000+count)):
            ip_port = IP+":"+str(10000+count)
            print("machine {} ip port: {}".format(count,ip_port))
            subsocket,subcontext = configure_port(ip_port,zmq.SUB,"connect")
        count+=1    
    return pubSocket,subsocket
    
def start_election(MachinesPID,pubsocket,subsocket,myIp):
    index = MachinesPID.index(MyPID)
    count = 0
    for i in range(1,4):
        #send to 3 machines that has higher rank
        #and receive ok from them 
        #if at least one Ok msg has received then
        # it job is done 
        # if not then it become leader and send 
        # to all machine that it is the leader  
        msg = {'id': MsgDetails.ELECTION, 'msg': "Election command", 'ip': myIp,'PID':MyPID,'topicfilter' : MachinesPID[index+i]}
        pubSocket.send(pickle.dumps(msg))
        
        #receive ok msg from machine with pid that i have just send command to.
        receivedMessage = pickle.loads(subsocket.recv())
        if(receivedMessage['topicfilter'] == MyPID and receivedMessage['id']==MsgDetails.OK):
            count+=1
    #if count equal 0 then this machine become the leader.
    if(count==0):
        LeaderPID = MyPID
        Machines[MyPID].isLeader=True
        msg = {'id': MsgDetails.NEW_LEADER, 'msg': "i'm the new leader", 'ip': myIp,'PID':MyPID}
        #send msgs to other machines
        for i in range(machinesNumber-1):
            pubSocket.send(pickle.dumps(msg))        



def Machine_process(MachinesPID,MachinesIPs,myIp):
    PUB_socket,sub_socket=configMachinePorts(Machines,myIp)
    send_get_PIDs(myIp,MachinesPID,PUB_socket,sub_socket)
    MachinesPID.sort(reverse=False)
    start_election(MachinesPID,PUB_socket,sub_socket,myIp)
    
    #run i'm alive process after determined the leader
    p = multiprocessing.Process(target=Alive_process,
                                args=(Machines[MyPID].isLeader, MachinesIPs,PUB_socket))
    p.start()  # ...and run!
    
    while(True):
        #msgs received from machine and its behaviour
        receivedMessage = pickle.loads(sub_socket.recv())
        if(receivedMessage['id'] == MsgDetails.START_ELECITION):
            #wait second then start to send election msgs to elect new leader
            time.sleep(1)
            start_election(MachinesPID,PUB_socket,sub_socket,myIp)
        #else if(receivedMessage['id'] == MsgDetails.ELECTION):
                #receiving election msg . then if machine is alive send OK
                # to the machine that send msg to me.
        elif(receivedMessage['id'] == MsgDetails.NEW_LEADER):
                #if the machine was leader and wake up again then it make it self as member
                Machine[MyPID].isLeader=False
                #set the leader PID
                LeaderPID = receivedMessage['PID']
                        



Machine_process(MachinesPID,MachinesIPs,MyIP)