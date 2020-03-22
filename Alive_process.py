import time
import zmq
from datetime import datetime
from dateutil.relativedelta import relativedelta
from utils import *


subSocket = None
subContext = None
pubSocket = None
pubContext = None

def ConfigureConnection(MyIP):
    global subSocket, subContext, pubSocket, pubContext
    # Start new Connection as a Normal Member
    # Configure Myself as a Subscriber
    ipPort = MyIP + ":" + AlivePort
    subSocket, subContext = configure_port(ipPort, zmq.SUB, 'bind',
            openTimeOut = True, Time = 6000)
    
    # Start new Connection as a Leader
    # Connect to all other Subscribers
    MachinesIPsTemp = MachinesIPs.copy()
    MachinesIPsTemp.remove(MyIP)
    pubSocket, pubContext = configure_multiple_ports(MachinesIPsTemp, 
                                                    AlivePort, zmq.PUB)

def Alive_process(MyID, MyIP, ElectionMode, ElectionModeLock, 
                  LeaderAmbiguityMode, LeaderAmbiguityModeLock,
                  PauseMode, PauseModeLock, 
                  LeaderID, LeaderIDLock):
    
    ConfigureConnection(MyIP)
    global subSocket, subContext, pubSocket, pubContext
    while (True):
        if(ElectionMode.value == 0 and PauseMode.value == 0):
            # Leader send periodically I'm Alive Msg to others Machines
            if(LeaderID.value == MyID):
                # I'm Alive Msg that will be sent periodically
                AliveMsg = {'MsgID': MsgDetails.LEADER_MEMBER_ALIVE, 'ID':MyID}
                pubSocket.send(pickle.dumps(AliveMsg))
                print("I'M Leader And I Sent I'm Alive, My ID is ", MyID)
                # Periodically 1 sec
                time.sleep(1)

            else:
                try:
                    setTimeOut(subSocket, 6000)
                    receivedMessage = pickle.loads(subSocket.recv())
                    print("Leader Is Alive and Its ID is " , receivedMessage["ID"])
                    if(receivedMessage["MsgID"] == MsgDetails.LEADER_MEMBER_ALIVE):
                        SenderID = receivedMessage["ID"]
                        if(SenderID != LeaderID.value and MyID != 0):
                            LeaderAmbiguityModeLock.acquire()
                            LeaderAmbiguityMode.value = 1
                            LeaderAmbiguityModeLock.release()
                except:
                    ElectionModeLock.acquire()
                    ElectionMode.value = 1
                    ElectionModeLock.release()


