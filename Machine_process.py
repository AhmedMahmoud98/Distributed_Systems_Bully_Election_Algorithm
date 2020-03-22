import os
import sys
import zmq
import time
from utils import *
from Machine import *
from Alive_process import *
from contextlib import contextmanager
from multiprocessing import Process, Value, Lock
from datetime import datetime
from dateutil.relativedelta import relativedelta


MyID = int(sys.argv[1]) 
#MyIP = get_ip()
MyIP = sys.argv[2] #TODO: it will be removed and uncomment the above line (Testing)

# Key is Machine ID and value is Machine object
Machines = {}

pubSocket = None
pubContext = None
subSocket = None
subContext = None

# boolen to know if the distrubted system in election function or not 
# to stop the alive process
ElectionMode = Value('i',1) #in election ,no leader 1 mean true
ElectionModeLock = Lock()

# Check for fake leaders
LeaderAmbiguityMode = Value('i',0)
LeaderAmbiguityModeLock = Lock()

# Fake leader pid is pid of the leader which  wake up after new leader is elected
PauseMode = Value('i',0)
PauseModeLock = Lock()

# Current system leader pid
LeaderID  = Value('i',0)
LeaderIDLock = Lock()

def InitMachines():
    for ID, IP in zip(MachinesIDs, MachinesIPs):
        Machines[ID] = Machine(IP, ID)

def SendStartMsg():
    # Send My ID & IP to the starting Machine
    msg = {'MsgId': MsgDetails.START_MSG, 'ID': MyID}
    pubSocket.send(pickle.dumps(msg))
    print("Send starting MSG")

def RcvStartMsgs():
    # Receive msg from other machines and create object to each one
    for i in range(1 , NumOfMachines):
        receivedMessage = pickle.loads(subSocket.recv())
        if(receivedMessage['MsgId'] == MsgDetails.START_MSG):
            ID = receivedMessage["ID"]
            print("Recieve start MSG from Machine#" + str(ID))


def SendNetworkReady():
    # Send Network Ready to others Machines
    msg = {'id': MsgDetails.NETWORK_READY}
    pubSocket.send(pickle.dumps(msg))
    print("Send Network is ready to all other Machines")

def RcvNetworkReady():
    # Receive Network is Ready
    pickle.loads(subSocket.recv())
    print("Receive Network is ready MSG")

def InitNetwork():
    # the starting machine receive Machines IDs & IPs 
    # then resend this list to all of them
    if(MyID == 0):
        RcvStartMsgs()
        SendNetworkReady()
        
    # Other Machines send ther ID & IP to the starting Machine 
    # Then Receive the Whole List from this machine 
    else:
        SendStartMsg()
        RcvNetworkReady()


def ConfigPubSubSockets(IsBlocked, IsTopic, Time = 0):
    # Setup Subscriber Port
    ipPort = MyIP + ":" + PublishingPort
    global subSocket, subContext, pubSocket, pubContext
    
    Topics = []
    if(IsTopic):
        # The Kind of Messages that I want only to receive
        Topics.append("Broadcast")  # Receive General Msgs
        if(MyID == 0):
            Topics.append("LeaderAmbiguity")

        MachinesIDs = [Machine.ID for Machine in Machines.values()]
        MachinesIDs.sort()
        for ID in MachinesIDs:
            if(ID < MyID):          # Receive Election Msgs from Machines with Smaller ID
                Topics.append(str(ID) + "LeaderElection")
            elif(ID == MyID):  
                # Receive OK Msgs from Machines with Greater ID     
                Topics.append(str(MyID) + "OK") 
                # Receive The Msg that Tell me that I amn't the Leader any more
                Topics.append(str(MyID) + "NotLeader") 
                # Receive Msg To Pause My Machine (For Testing)
                Topics.append(str(MyID) + "Pause")
            else:
                break
        
    subSocket, subContext = configure_port(ipPort, zmq.SUB, 'bind', 
                                            IsBlocked, Time, IsTopic, Topics)
    # Connect to all other Subscribers
    MachinesIPs = [Machine.IP for Machine in Machines.values()]

    # So Remove My IP if existed
    try:
        MachinesIPs.remove(MyIP)
    except:
        pass

    pubSocket, pubContext = configure_multiple_ports(MachinesIPs, PublishingPort, zmq.PUB)

def StartElection():
    # I'm A Leader unless I receive OK Msg from a Machine with Greater ID
    IsLeader = True

    # TODO: Ask How to remove that sleep
    time.sleep((NumOfMachines - 1 - MyID) * 0.01)

    # Send Election Msg to All Machines with Greater ID
    ElectionMsg = {"MsgID": MsgDetails.ELECTION, "ID": MyID}
    Topic = (str(MyID) + "LeaderElection").encode()
    pubSocket.send_multipart([Topic ,pickle.dumps(ElectionMsg)])

    try:
        while(True):
            setTimeOut(subSocket, 2000)
            Topic, receivedMessage = subSocket.recv_multipart()
            receivedMessage = pickle.loads(receivedMessage)
            if(receivedMessage["MsgID"] == MsgDetails.OK):
                IsLeader = False
                print("RCV OK Msg From  ", receivedMessage["ID"])   
            if(receivedMessage["MsgID"] == MsgDetails.ELECTION):
                OkMsg = {"MsgID": MsgDetails.OK, "ID": MyID}
                Topic =  (str(receivedMessage["ID"]) + "OK").encode()
                pubSocket.send_multipart([Topic ,pickle.dumps(OkMsg)])
                print("RCV Election Msg From  ", receivedMessage["ID"])              
    except:
        pass              

    if(IsLeader == True):
        # Declare Myself as a new Leader
        LeaderIDLock.acquire()
        LeaderID.value = MyID
        LeaderIDLock.release()
        # Tell the Other Machines that I'm The New Leader
        LeaderMsg = {'MsgID': MsgDetails.NEW_LEADER, 'ID': MyID}
        Topic = "Broadcast".encode()
        pubSocket.send_multipart([Topic, pickle.dumps(LeaderMsg)]) 
    else:
        # Declare Myself as a Normal Member 
        # Receive The New Leader Msg
        Topic, receivedMessage = subSocket.recv_multipart()
        receivedMessage = pickle.loads(receivedMessage)
        if(receivedMessage['MsgID'] == MsgDetails.NEW_LEADER):
            LeaderIDLock.acquire()
            LeaderID.value = receivedMessage['ID']
            LeaderIDLock.release()
        
    ElectionModeLock.acquire()
    ElectionMode.value = 0
    ElectionModeLock.release()


def Machine_process():
    ################### Wait For All Machines to be Ready for Election ###############
    if(MyID == 0):
        InitMachines()
    else:
        Machines[0] = Machine(StartingMachineIP, 0)

    ConfigPubSubSockets(IsBlocked = False, IsTopic = False)
    InitNetwork()

    if(MyID != 0):
        InitMachines()

    # Terminate Connection
    subSocket.close()
    pubSocket.close()
    subContext.destroy()
    pubContext.destroy()
    
    ################### Start Election at the Beginning to choose the leader ###############
    ConfigPubSubSockets(IsBlocked = True, IsTopic = True, Time = 2000)
    StartElection()

    # Pause Mode is Used For Testing
    PauseModeLock.acquire()
    PauseMode.value = 0
    PauseModeLock.release()
    # Open Alive Process To Start The Hearebeat in the System
    AliveProcess = Process(target = Alive_process,
                   args = (MyID, MyIP, ElectionMode, ElectionModeLock, 
                           LeaderAmbiguityMode, LeaderAmbiguityModeLock, 
                           PauseMode, PauseModeLock,  
                           LeaderID, LeaderIDLock ))
    AliveProcess.start()  # ...and run!

    # End The TimeOut and make Receive Blocked
    while(True):
        receivedMessage = None
        try:
            while(True):
                setTimeOut(subSocket, 500)
                Topic, receivedMessage = subSocket.recv_multipart()
                receivedMessage = pickle.loads(receivedMessage)

                # No Machine Send I'm Alive Msg, Let's Start Election
                if(receivedMessage['MsgID'] == MsgDetails.START_ELECITION):
                    ElectionModeLock.acquire()
                    ElectionMode.value = 1
                    ElectionModeLock.release()
                    StartElection()
                    print("New Leader Is " , LeaderID.value)

                # Machine 0 Send Me The Real Leader ID as 
                # there were Ambiguity In the System
                elif(receivedMessage['MsgID'] == MsgDetails.REAL_LEADER):
                    print("Now I Know The Real Leader And Its ID is ", receivedMessage['LeaderID'])
                    LeaderIDLock.acquire()
                    LeaderID.value = receivedMessage['LeaderID']
                    LeaderIDLock.release()
                    LeaderAmbiguityModeLock.acquire()
                    LeaderAmbiguityMode.value = 0
                    LeaderAmbiguityModeLock.release()

    
                # Pause Msg (For Testing)
                elif(receivedMessage['MsgID'] == MsgDetails.PAUSE):
                    PauseModeLock.acquire()
                    PauseMode.value = 1
                    PauseModeLock.release()
                    TimeBefore = datetime.now()
                    TimeIn = datetime.now()
                    while(relativedelta(TimeIn, TimeBefore).seconds < 15):
                        try:
                            Topic, receivedMessage = subSocket.recv_multipart()
                        except:
                            TimeIn = datetime.now()
                            pass
                    PauseModeLock.acquire()
                    PauseMode.value = 0
                    PauseModeLock.release()


                # There's a Leader Ambiguity in the system
                # May Be The Old Leader Wake up Again So All Machines sent a Leader Ambiguity Msg
                # To Machine 0 OR a Normal Member Wake Up Again and Receive a Msg from a 
                # Leader that It think It isn't the Real Leader So It sent a Msg to Machine 0
                ########### This Msg is Sent Only To Machine 0 ############
                elif(receivedMessage['MsgID'] == MsgDetails.LEADER_AMBIGUITY):
                    RealLeaderMsg = {"MsgID": MsgDetails.REAL_LEADER, 'LeaderID': LeaderID.value}
                    Topic = "Broadcast".encode()
                    pubSocket.send_multipart([Topic ,pickle.dumps(RealLeaderMsg)])
                    LeaderAmbiguityModeLock.acquire()
                    LeaderAmbiguityMode.value = 0
                    LeaderAmbiguityModeLock.release()

        except:
            # No Alive Msg is received from leader 
            # Then send Msg to all Machines to start new election
            if(ElectionMode.value == 1):
                StartElectionMsg = {'MsgID': MsgDetails.START_ELECITION }
                Topic = "Broadcast".encode()
                pubSocket.send_multipart([Topic, pickle.dumps(StartElectionMsg)])
                StartElection()
                print("New Leader Is " , LeaderID.value)
                    
            # A Machine Receive a Msg From a another Machine 
            # which id differnt from The Leader It thought
            elif(LeaderAmbiguityMode.value == 1):
                LeaderAmbiguityMsg = {'MsgID': MsgDetails.LEADER_AMBIGUITY }
                Topic = "LeaderAmbiguity".encode()
                pubSocket.send_multipart([Topic, pickle.dumps(LeaderAmbiguityMsg)])
      
Machine_process()