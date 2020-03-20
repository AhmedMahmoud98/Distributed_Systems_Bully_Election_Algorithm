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
import multiprocessing
from Alive_process import *

PIDTransferPort=int(sys.argv[1]) #pub port number 10000
#sub port number will be same number +10000
#then if machine found that this port number is 10000 then it know that it's first machine then it start to communicate with ports 20001,20002,...20000N

MyPID=os.getpid()
MyIP=get_ip()
Machines = {}
pubSocket = None
pubContext = None
subsocket = None

#shared memory manger
manager = multiprocessing.Manager()

#boolen to know if the distrubted system in election function or not to stop the alive process
checkElection = manager.Value('i',1) #in election ,no leader 1 mean true


#PID is key and socket is SUb socket to receive from other machines
machine_socket={}
#key is machine process pid and value is machine object
Machines[MyPID]=Machine(MyIP,MyPID,PIDTransferPort,True,False)


def send_get_PIDs(myIp,MachinesPID,pubSocket,subSocket):
    if(PIDTransferPort == 10000):
        #first machine then receive whole pids of other machines and store in machinespid
        
        for i in range(2*(machinesNumber-1)):
            print("before receive pid")
            #receive alive msg from leader    
            receivedMessage = pickle.loads(subSocket.recv())
            print("after receive pid msg :{}".format(receivedMessage))
            #receive msg from other machines and create objects 
            #port in object is pub socket port
            if(receivedMessage['id'] == MsgDetails.PID_MSG and receivedMessage['PID'] not in MachinesPID):
                PID=receivedMessage['PID']
                MachinesPID.append(PID)
                Machines[PID]= Machine(receivedMessage['ip'],PID,receivedMessage['PORT'],True,False)

        #start to send whole pids list to others machines
        msg = {'id': MsgDetails.PID_LIST, 'PID': MyPID,'list':MachinesPID ,'ip': myIp,'PORT':PIDTransferPort}
        print("msg to send to other machines{}".format(msg))
        pubSocket.send(pickle.dumps(msg))
        print("pid is send")

    else:
        #not the first machine
        #send my pid 
        time.sleep(2)
        msg = {'id': MsgDetails.PID_MSG, 'PID': MyPID,'ip': myIp,'PORT':PIDTransferPort}
        print("msg to send to other machines{}".format(msg))
        pubSocket.send(pickle.dumps(msg))
        print("pid is send")
        pubSocket.send(pickle.dumps(msg))
        print("pid is send agian")

        print("before receive pid")
        #receive pids list from machine 0
        receievedCorrect = True
        while(receievedCorrect):    
            receivedMessage = pickle.loads(subSocket.recv())
            print("after receive pid msg :{}".format(receivedMessage))
            print(type(receivedMessage['PORT']))
            if receivedMessage['PORT'] == 10000 :
                receievedCorrect = False
            else:
                receievedCorrect = True
        MachinesPID = receivedMessage['list']
    print(MachinesPID)
    return MachinesPID
        

def configMachinePorts(Machines,myIp,block,subTopic):
    ipPort = myIp + ":" + str(PIDTransferPort)
    print(ipPort)
    #setup publisher communication
    pubSocket, pubContext = configure_port(ipPort, zmq.PUB, 'bind')

    #connect to all other machines by port number which is PIDTransferPort+10000+count
    count=0 #to Machines number - 1
    print("iports of sub socket")
    subcontext = zmq.Context()
    subsocket = subcontext.socket(zmq.SUB)
    if subTopic == True:
        subsocket.setsockopt_string(zmq.SUBSCRIBE, str(MyPID))
    else:
        subsocket.setsockopt_string(zmq.SUBSCRIBE, "")
    subsocket.setsockopt_string(zmq.SUBSCRIBE,"Broadcast")
    if(block == True):
        subsocket.setsockopt(zmq.RCVTIMEO, 3000)
        subsocket.setsockopt(zmq.LINGER,      0)
        subsocket.setsockopt(zmq.AFFINITY,    1)
        
    for IP in MachinesIPs:
        #check if this port is mine
        if not (PIDTransferPort == (10000+count)):
            ip_port = IP+":"+str(10000+count)
            print("machine {} ip port: {}".format(count,ip_port))
            subsocket.connect("tcp://" +ip_port)
        count+=1    
    return pubSocket,pubContext,subsocket,subcontext
    
def start_election(MachinesPID,PUBSocket,SUBSocket,myIp,p):
    checkElection.value = 1
    #run i'm alive process after determined the leader
    if( p != None):
        if(p.is_alive()):
            p.Terminate()  # ...terminate!
    
    index = MachinesPID.index(MyPID)
    print("{} of pid : {}".format(index,MyPID))
    isleader = True
    #if my machine is not the first one then i will recv at most 6 msgs 3 OK and 3 ELection 
    #if first machine then 3 ok only at most 
    if(MyPID == MachinesPID[0]):
        #send election at first then receive
        #time.sleep(2)
        for i in range(1,4):
            if(index+i >= len(MachinesPID)):
                break
            msg = {'id': MsgDetails.ELECTION, 'msg': "election command", 'ip': myIp,'PID':MyPID,'topicfilter' : MachinesPID[index+i]}
            print("send msg: {}".format(msg))
            #send election msg
            PUBSocket.send_multipart([str(MachinesPID[index+i]).encode(),pickle.dumps(msg)])
            print("before receive")
            trueMsg = False
            while (not trueMsg):
                print("before receive")
                try:
                    [topic,rmsg] = SUBSocket.recv_multipart()
                    receivedMessage = pickle.loads(rmsg)
                    print("after received:{}".format(receivedMessage))
                    if(receivedMessage['topicfilter'] == MyPID and receivedMessage['id']==MsgDetails.OK):
                        isleader=False
                        trueMsg = True
                    
                    
                except:break        
            
    else:
        #not first machine
        #receive Election at first from the lower machines
        for i in range(index):
            print("before receive")
            trueMsg = False
            while (not trueMsg):
                try:
                    [topic,rmsg] = SUBSocket.recv_multipart()
                    receivedMessage = pickle.loads(rmsg)
                    print("after received:{}".format(receivedMessage))
                    if(receivedMessage['topicfilter'] == MyPID and receivedMessage['id'] == MsgDetails.ELECTION):
                        msg = {'id': MsgDetails.OK, 'msg': "OK", 'ip': myIp,'PID':MyPID,'topicfilter' : receivedMessage['PID']}
                        print("send msg: {}".format(msg))
                        trueMsg = True
                        PUBSocket.send_multipart([str(receivedMessage['PID']).encode(),pickle.dumps(msg)])
                        
                except:
                    break
        #time.sleep(2)
        for i in range(1,4) :
            if(index+i >= len(MachinesPID)):
                break
            msg = {'id': MsgDetails.ELECTION, 'msg': "election command", 'ip': myIp,'PID':MyPID,'topicfilter' : MachinesPID[index+i]}
            print("send msg: {}".format(msg))
            #send election msg
            PUBSocket.send_multipart([str(MachinesPID[index+i]).encode(),pickle.dumps(msg)])
            print("before receive")
            trueMsg = False
            while (not trueMsg):
                try:
                    [topic,rmsg] = SUBSocket.recv_multipart()
                    receivedMessage = pickle.loads(rmsg)
                    print("after received:{}".format(receivedMessage))
                    if(receivedMessage['topicfilter'] == MyPID and receivedMessage['id']==MsgDetails.OK):
                        isleader=False
                        trueMsg = True
                    
                except:break        

    checkElection.value = 0
    #if count equal 0 then this machine become the leader.
    if(isleader == True):
        print("Leader")
        LeaderPID = MyPID
        Machines[MyPID].isLeader=True
        msg = {'id': MsgDetails.NEW_LEADER, 'msg': "i'm the new leader", 'ip': myIp,'PID':MyPID}
        #send msgs to other machines
        time.sleep(2)
        PUBSocket.send_multipart(["Broadcast".encode(),pickle.dumps(msg)])        
        p = multiprocessing.Process(target=Alive_process,
                                args=(checkElection,Machines[MyPID].isLeader, MachinesIPs))
        p.start()  # ...and run!
    else:
        Machines[MyPID].isLeader=False

    
    return isleader
    

def Machine_process(MachinesPID,MachinesIPs,myIp):
    PUB_socket,pubContext,sub_socket,subcontext=configMachinePorts(Machines,myIp,False,False)#with blocking not topic filter
    MachinesPID = send_get_PIDs(myIp,MachinesPID,PUB_socket,sub_socket)
    # Terminate Connection
    sub_socket.close()
    subcontext.destroy()
    PUB_socket.close()
    pubContext.destroy()
    
    PUB_socket,pubContext,sub_socket,subcontext=configMachinePorts(Machines,myIp,True,True)#with no blocking with topic filter on my pid
    MachinesPID.sort(reverse=False)
    print(MachinesPID)
    print("\n\n")
    p = None
    isleader = start_election(MachinesPID,PUB_socket,sub_socket,myIp,p)
    '''
    #run i'm alive process after determined the leader   
    p = multiprocessing.Process(target=Alive_process,
                                args=(checkElection,Machines[MyPID].isLeader, MachinesIPs))
    p.start()  # ...and run!
    '''
    
    while(True):
        #msgs received from machine and its behaviour
        print("in machine process super loop new leader:{}".format(checkElection.value))
        if(checkElection.value == 1):
                #no alive msg received from leader then send msg to all machines to start new election
                msg = {'id': MsgDetails.START_ELECITION, 'msg': "start election command", 'ip': myIp}
                print(msg)
                # not the same socket to send to another sub sockets except alive sub socket
                p.terminate()
                PUB_socket.send_multipart(["Broadcast".encode(),pickle.dumps(msg)])
                time.sleep(2)
                

        try:

            #sub_socket.setsockopt(zmq.RCVTIMEO,  300)
            print("before recv")
            [topic,rmsg] = sub_socket.recv_multipart()
            receivedMessage = pickle.loads(rmsg)
            print("msg recv:",receivedMessage)

            if(receivedMessage['id'] == MsgDetails.START_ELECITION):
                #wait second then start to send election msgs to elect new leader
                checkElection.value = 0
                time.sleep(2)
                start_election(MachinesPID,PUB_socket,sub_socket,myIp,p)

            elif(receivedMessage['id'] == MsgDetails.NEW_LEADER):
                print("in new leader")
                if(p != None):
                    if(p.is_alive()):
                        p.terminate()
                #if the machine was leader and wake up again then it make it self as member
                Machines[MyPID].isLeader=False
                #set the leader PID
                LeaderPID = receivedMessage['PID']
                checkElection.value = 0
                p = multiprocessing.Process(target=Alive_process,
                                args=(checkElection,Machines[MyPID].isLeader, MachinesIPs))
                p.start()  # ...and run!
                print("after start process alive")

        except:
            pass          
            
        
Machine_process(MachinesPID,MachinesIPs,MyIP)