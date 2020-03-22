import os
import sys
import zmq
from utils import *
from Machine import *
import multiprocessing
from Alive_process import *
from contextlib import contextmanager


MyID = int(sys.argv[1]) 
#MyIP = get_ip()
MyIP = sys.argv[2] #TODO: it will be removed and uncomment the above line (Testing)

# Key is Machine ID and value is Machine object
Machines = {}

pubSocket = None
pubContext = None
subSocket = None
subContext = None

# shared memory manger
manager = multiprocessing.Manager()

# boolen to know if the distrubted system in election function or not 
# to stop the alive process
ElectionMode = manager.Value('i',1) #in election ,no leader 1 mean true
ElectionModeLock = multiprocessing.Lock()

# Check for fake leaders
ManyLeadersMode = manager.Value('i',0)
ManyLeadersModeLock = multiprocessing.Lock()

# Fake leader pid is pid of the leader which  wake up after new leader is elected
FakeLeaderID = manager.Value('i',0)
FakeLeaderLock = multiprocessing.Lock()

# Current system leader pid
LeaderID  = manager.Value('i',0)
LeaderIDLock = multiprocessing.Lock()

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
    time.sleep(NumOfMachines - 1 - MyID)

    # Send Election Msg to All Machines with Greater ID
    ElectionMsg = {"MsgID": MsgDetails.ELECTION, "ID": MyID}
    Topic = (str(MyID) + "LeaderElection").encode()
    pubSocket.send_multipart([Topic ,pickle.dumps(ElectionMsg)])

    try:
        while(True):
            Topic, receivedMessage = subSocket.recv_multipart()
            receivedMessage = pickle.loads(receivedMessage)
            if(receivedMessage["MsgID"] == MsgDetails.OK):
                IsLeader = False
            if(receivedMessage["MsgID"] == MsgDetails.ELECTION):
                OkMsg = {"MsgID": MsgDetails.OK, "ID": MyID}
                Topic =  (str(receivedMessage["ID"]) + "OK").encode()
                pubSocket.send_multipart([Topic ,pickle.dumps(OkMsg)])              
    except:
        pass              

    if(IsLeader == True):
        # Declare Myself as a new Leader
        LeaderIDLock.acquire()
        LeaderID.value = MyID
        LeaderIDLock.release()
        Machines[MyID].IsLeader = True
        # Tell the Other Machines that I'm The New Leader
        LeaderMsg = {'MsgID': MsgDetails.NEW_LEADER, 'ID': MyID}
        Topic = "Broadcast".encode()
        pubSocket.send_multipart([Topic, pickle.dumps(LeaderMsg)]) 
    else:
        # Declare Myself as a Normal Member 
        Machines[MyID].IsLeader = False
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
    ConfigPubSubSockets(IsBlocked = True, IsTopic = True, Time = 5000)
    StartElection()

    # Open Alive Process To Start The Hearebeat in the System
    AliveProcess = multiprocessing.Process(target = Alive_process,
                   args = (MyID, MyIP, ElectionMode, ElectionModeLock, 
                           Machines[MyID].IsLeader, ManyLeadersMode, 
                           ManyLeadersModeLock, FakeLeaderID, FakeLeaderLock,  
                           LeaderID, LeaderIDLock ))
    AliveProcess.start()  # ...and run!

    # End The TimeOut and make Receive Blocked
    subSocket.setsockopt(zmq.RCVTIMEO,  500)
    try:
        while(True):
            Topic, receivedMessage = subSocket.recv_multipart()
            receivedMessage = pickle.loads(receivedMessage)

            if(receivedMessage['MsgID'] == MsgDetails.START_ELECITION):
                ElectionModeLock.acquire()
                ElectionMode.value = 1
                ElectionModeLock.release()
                StartElection()

            elif(receivedMessage['MsgID'] == MsgDetails.NOT_LEADER):
                Machines[MyID].IsLeader = False
                LeaderIDLock.acquire()
                LeaderID.value = receivedMessage['LeaderID']
                LeaderIDLock.release()
    except:
        # No Alive Msg is received from leader 
        # Then send Msg to all Machines to start new election
        if(ElectionMode.value == 1):
                StartElectionMsg = {'MsgID': MsgDetails.START_ELECITION }
                Topic = "Broadcast".encode()
                pubSocket.send_multipart([Topic, pickle.dumps(StartElectionMsg)])
                StartElection()
                
        # The Old Leader is now Alive again, tell him that 
        # He isn't the Leader anymore
        elif(ManyLeadersMode.value == 1):
                NotLeaderMsg = {'MsgID': MsgDetails.NOT_LEADER, 'LeaderID': LeaderID.value}
                Topic = str(FakeLeaderID).encode()
                pubSocket.send_multipart([Topic ,pickle.dumps(NotLeaderMsg)])
                ManyLeadersModeLock.acquire()
                ManyLeadersMode.value = 0
                ManyLeadersModeLock.release()
                
Machine_process()