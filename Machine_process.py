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

PubSocket = None
PubContext = None
SubSocket = None
SubContext = None

SharedLock = Lock()

# Stop the Heartbeat if the Network in Election state
ElectionMode = Value('i',0) 

# There are Two Leaders in the system or some node doesn't know the real leader 
LeaderAmbiguityMode = Value('i',0)

# Pause Mode is used to stop some node from doing any functionality [For Testing] 
PauseMode = Value('i',0)

# Current system leader ID
LeaderID  = Value('i',0)


def InitMachines():
    for ID, IP in zip(MachinesIDs, MachinesIPs):
        Machines[ID] = Machine(IP, ID)

def SendStartMsg():
    global PubSocket
    # Send My ID & IP to the starting Machine
    StartMsg = {'MsgId': MsgDetails.START_MSG, 'ID': MyID}
    Topic = "StartConnection".encode()
    PubSocket.send_multipart([Topic, pickle.dumps(StartMsg)])
    print("Send starting MSG To Machine 0")

def RcvStartMsgs():
    global SubSocket
    # Receive msg from other machines and create object to each one
    for i in range(1 , NumOfMachines):
        Topic, receivedMessage = SubSocket.recv_multipart()
        receivedMessage = pickle.loads(receivedMessage)
        if(receivedMessage['MsgId'] == MsgDetails.START_MSG):
            ID = receivedMessage["ID"]
            print("Recieve start MSG from Machine#" + str(ID))


def SendNetworkReady():
    global PubSocket
    # Send Network Ready to others Machines
    NetworkReadyMsg = {'id': MsgDetails.NETWORK_READY}
    Topic = "Broadcast".encode()
    PubSocket.send_multipart([Topic, pickle.dumps(NetworkReadyMsg)])
    print("Send Network is ready to all other Machines")

def RcvNetworkReady():
    global SubSocket
    # Receive Network is Ready
    Topic, receivedMessage = SubSocket.recv_multipart()
    receivedMessage = pickle.loads(receivedMessage)
    print("Receive Network is ready MSG")

def InitNetwork():
    # the starting machine receive Machines IDs & IPs 
    # then send Network is Ready to all other Machines
    if(MyID == 0):
        RcvStartMsgs()
        SendNetworkReady()
        
    # Other Machines send ther ID & IP to the starting Machine 
    # Then Receive Network is Ready Msg
    else:
        SendStartMsg()
        RcvNetworkReady()


def ConfigPubSubSockets():
    global SubSocket, SubContext, PubSocket, PubContext
   
    # Setup Subscriber Port
    ipPort = MyIP + ":" + PublishingPort

    Topics = []
    # The Kind of Messages that I want only to receive
    Topics.append("Broadcast")  # Receive General Msgs
    if(MyID == 0):
        Topics.append("LeaderAmbiguity")
        Topics.append("StartConnection")

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
        
    SubSocket, SubContext = configure_port(ipPort, zmq.SUB, 'bind', 
                                            True, -1, True, Topics)
    # Connect to all other Subscribers
    MachinesIPs = [Machine.IP for Machine in Machines.values()]

    # So Remove My IP if existed
    try:
        MachinesIPs.remove(MyIP)
    except:
        pass

    PubSocket, PubContext = configure_multiple_ports(MachinesIPs, PublishingPort, zmq.PUB)

def StartElection():
    # Announce Election Mode
    SharedLock.acquire()
    ElectionMode.value = 1
    SharedLock.release()

    # I'm A Leader unless I receive OK Msg from a Machine with Greater ID
    IsLeader = True

    # TODO: Ask How to remove that sleep
    time.sleep((NumOfMachines - 1 - MyID) * 0.01)

    # Send Election Msg to All Machines with Greater ID
    ElectionMsg = {"MsgID": MsgDetails.ELECTION, "ID": MyID}
    Topic = (str(MyID) + "LeaderElection").encode()
    PubSocket.send_multipart([Topic ,pickle.dumps(ElectionMsg)])

    try:
        while(True):
            setTimeOut(SubSocket, 500)
            Topic, receivedMessage = SubSocket.recv_multipart()
            receivedMessage = pickle.loads(receivedMessage)

            # Receive OK Msg from Nodes with Greater ID 
            if(receivedMessage["MsgID"] == MsgDetails.OK):
                IsLeader = False
                print("RCV OK Msg From  ", receivedMessage["ID"]) 

            # Receive Election Msg from Nodes with Lower ID  
            if(receivedMessage["MsgID"] == MsgDetails.ELECTION):
                # Send OK MSG To The Nodes that Send Me Election Msg
                OkMsg = {"MsgID": MsgDetails.OK, "ID": MyID}
                Topic =  (str(receivedMessage["ID"]) + "OK").encode()
                PubSocket.send_multipart([Topic ,pickle.dumps(OkMsg)])
                print("RCV Election Msg From  ", receivedMessage["ID"])              
    except:
        pass              

    if(IsLeader == True):
        # Declare Myself as a new Leader
        SharedLock.acquire()
        LeaderID.value = MyID
        SharedLock.release()
        # Tell the Other Machines that I'm The New Leader
        LeaderMsg = {'MsgID': MsgDetails.NEW_LEADER, 'ID': MyID}
        Topic = "Broadcast".encode()
        PubSocket.send_multipart([Topic, pickle.dumps(LeaderMsg)]) 
    else:
        # I'm not the Leader
        # Receive The New Leader Msg
        setTimeOut(SubSocket, -1)
        Topic, receivedMessage = SubSocket.recv_multipart()
        receivedMessage = pickle.loads(receivedMessage)
        if(receivedMessage['MsgID'] == MsgDetails.NEW_LEADER):
            SharedLock.acquire()
            LeaderID.value = receivedMessage['ID']
            SharedLock.release()
        
    SharedLock.acquire()
    ElectionMode.value = 0
    SharedLock.release()


def Machine_process():
    ################### Wait For All Machines to be Ready for Election ###############
    # Initialize the Machines Array
    InitMachines()
    # Initialize Publisher and Subscriber Ports
    ConfigPubSubSockets()
    # Initializing Network to make sure that all Machines are alive before starting Election
    InitNetwork()
    # Start Election at the Beginning to choose the leader 
    StartElection()

    # Open Alive Process To Start The Hearebeat in the System
    AliveProcess = Process(target = Alive_process,
                   args = (MyID, MyIP, ElectionMode, LeaderAmbiguityMode,  
                           PauseMode, LeaderID, SharedLock ))
    AliveProcess.start()  # ...and run!

    # End The TimeOut and make Receive Blocked
    while(True):
        receivedMessage = None
        try:
            while(True):
                setTimeOut(SubSocket, 500)
                Topic, receivedMessage = SubSocket.recv_multipart()
                receivedMessage = pickle.loads(receivedMessage)

                # No Machine Send I'm Alive Msg, Let's Start Election
                if(receivedMessage['MsgID'] == MsgDetails.START_ELECITION):
                    print("I was invited to Election")
                    print("PauseMode Value",PauseMode.value )
                    if(PauseMode.value == 0):
                        StartElection()
                    print("New Leader Is " , LeaderID.value)

                # Machine 0 Send Me The Real Leader ID as 
                # there were Ambiguity In the System
                elif(receivedMessage['MsgID'] == MsgDetails.REAL_LEADER):
                    # Assign The Real Leader ID and End Ambiguit State
                    SharedLock.acquire()
                    LeaderID.value = receivedMessage['LeaderID']
                    LeaderAmbiguityMode.value = 0
                    SharedLock.release()
                    print("Now I Know The Real Leader And Its ID is ", 
                                                       LeaderID.value)

                # Pause Msg (For Testing)
                elif(receivedMessage['MsgID'] == MsgDetails.PAUSE):
                    print("I Will be Paused for 15s")
                    # Declare Pause Mode to stop Heartbeating 
                    SharedLock.acquire()
                    PauseMode.value = 1
                    SharedLock.release()

                    # Sleep for 15 Seconds, Throw All received Msgs
                    SubSocket.unsubscribe("Broadcast")
                    time.sleep(30)
                    SubSocket.subscribe("Broadcast")

                    # End Pause Mode 
                    SharedLock.acquire()
                    PauseMode.value = 0
                    SharedLock.release()


                # There's a Leader Ambiguity in the system
                # May Be The Old Leader Wake up Again So All Machines sent a Leader Ambiguity Msg
                # To Machine 0 OR a Normal Member Wake Up Again and Receive a Msg from a 
                # Leader that It think It isn't the Real Leader So It sent a Msg to Machine 0
                ########### This Msg is Sent Only To Machine 0 ############
                elif(receivedMessage['MsgID'] == MsgDetails.LEADER_AMBIGUITY):
                    RealLeaderMsg = {"MsgID": MsgDetails.REAL_LEADER, 'LeaderID': LeaderID.value}
                    Topic = "Broadcast".encode()
                    PubSocket.send_multipart([Topic ,pickle.dumps(RealLeaderMsg)])
                    SharedLock.acquire()
                    LeaderAmbiguityMode.value = 0
                    SharedLock.release()

        except:
            # No Alive Msg is received from leader 
            # Then send Msg to all Machines to start new election
            if(ElectionMode.value == 1):
                StartElectionMsg = {'MsgID': MsgDetails.START_ELECITION }
                Topic = "Broadcast".encode()
                PubSocket.send_multipart([Topic, pickle.dumps(StartElectionMsg)])
                print("I Started Election")
                StartElection()
                print("New Leader Is " , LeaderID.value)
                    
            # A Machine Receive a Msg From a another Machine 
            # which id differnt from The Leader It thought
            elif(LeaderAmbiguityMode.value == 1):
                LeaderAmbiguityMsg = {'MsgID': MsgDetails.LEADER_AMBIGUITY }
                Topic = "LeaderAmbiguity".encode()
                PubSocket.send_multipart([Topic, pickle.dumps(LeaderAmbiguityMsg)])
      
Machine_process()