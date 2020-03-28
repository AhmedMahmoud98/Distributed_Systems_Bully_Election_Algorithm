import time
import zmq
from datetime import datetime
from dateutil.relativedelta import relativedelta
from utils import *

SubSocket = None
SubContext = None
PubSocket = None
PubContext = None

def ConfigureConnection(MyIP):
    global SubSocket, SubContext, PubSocket, PubContext
    # Start new Connection as a Normal Member
    # Configure Myself as a Subscriber
    ipPort = MyIP + ":" + AlivePort
    SubSocket, SubContext = configure_port(ipPort, zmq.SUB, 'bind',
                               openTimeOut = True, Time = 6000)
    
    # Start new Connection as a Leader
    # Connect to all other Subscribers
    MachinesIPsTemp = MachinesIPs.copy()
    MachinesIPsTemp.remove(MyIP)
    PubSocket, PubContext = configure_multiple_ports(MachinesIPsTemp, 
                                                    AlivePort, zmq.PUB)

def Alive_process(MyID, MyIP, ElectionMode,  
                  LeaderAmbiguityMode, PauseMode, 
                  LeaderID, SharedLock):
    
    global SubSocket, SubContext, PubSocket, PubContext

    # Initialize the Heartbeat Publisher and Subscriber Ports
    ConfigureConnection(MyIP)
    
    while (True):
        # Stop the Heartbeat if It was in Election or Pause Mode
        if(ElectionMode.value == 0 and PauseMode.value == 0):
            # Leader send periodically I'm Alive Msg to others Machines
            if(LeaderID.value == MyID):
                # I'm Alive Msg that will be sent periodically
                AliveMsg = {'MsgID': MsgDetails.LEADER_MEMBER_ALIVE, 'ID':MyID}
                PubSocket.send(pickle.dumps(AliveMsg))
                print("I'M Leader And I Sent I'm Alive, My ID is ", MyID)
                # Periodically 1 sec
                time.sleep(1)

            else:
                try:
                    # Receive Leader I'm Alive Message
                    setTimeOut(SubSocket, 8000)
                    receivedMessage = pickle.loads(SubSocket.recv())
                    print("Leader Is Alive and Its ID is " , receivedMessage["ID"])

                    if(receivedMessage["MsgID"] == MsgDetails.LEADER_MEMBER_ALIVE):
                        SenderID = receivedMessage["ID"]
                        # If It isn't the Leader that I Know
                        # [It may be Fake Leader or I don't know the Real Leader]
                        if(SenderID != LeaderID.value and MyID != 0):
                            print("There is Leader Ambiguity")
                            SharedLock.acquire()
                            LeaderAmbiguityMode.value = 1
                            SharedLock.release()
                except:
                    # If the leader doesn't send I'm Alive Message
                    # Start Election
                    print("Leader isn't Alive, I'll ask for Election")
                    SharedLock.acquire()
                    ElectionMode.value = 1
                    SharedLock.release()


