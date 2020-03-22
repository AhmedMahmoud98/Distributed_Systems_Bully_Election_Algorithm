
import sys
import time
import zmq
import signal
from contextlib import contextmanager
from utils import *
from datetime import datetime
from dateutil.relativedelta import relativedelta

def Alive_process(MyID, MyIP, ElectionMode, ElectionModeLock, 
                  IsLeader, ManyLeadersMode, ManyLeadersModeLock, 
                  FakeLeaderID, FakeLeaderLock, LeaderID, LeaderIDLock):
    
    subSocket = None
    subContext = None
    pubSocket = None
    pubContext = None

    while (True):
        if(ElectionMode.value == 0):
            # Leader send periodically I'm Alive Msg to others Machines
            if(IsLeader == True):
                if(subSocket != None and subContext != None):
                    # Terminate My Connection as Normal Member
                    subSocket.close()
                    subContext.destroy()
                    subSocket = None
                    subContext = None

                if(pubContext == None and pubSocket == None):
                    # Start new Connection as a Leader
                    # Connect to all other Subscribers
                    MachinesIPsTemp = MachinesIPs.copy()
                    MachinesIPsTemp.remove(MyIP)
                    pubSocket, pubContext = configure_multiple_ports(MachinesIPsTemp, 
                                                                 AlivePort, zmq.PUB)
                # I'm Alive Msg that will be sent periodically
                AliveMsg = {'MsgID': MsgDetails.LEADER_MEMBER_ALIVE, 'ID':MyID}
                pubSocket.send(pickle.dumps(AliveMsg))
                # Periodically 1 sec
                time.sleep(1)

            else:
                if(pubSocket != None and pubContext != None):
                    # Terminate My Connection as a Leader
                    pubSocket.close()
                    pubContext.destroy()
                    pubSocket = None
                    pubContext = None

                if(subSocket == None and subContext == None):
                    # Start new Connection as a Normal Member
                    # Configure Myself as a Subscriber
                    ipPort = MyIP + ":" + AlivePort
                   
                    subSocket, subContext = configure_port(ipPort, zmq.SUB, 'bind',
                                                    openTimeOut= True, Time = 1000)
                try:
                    while(True):
                        receivedMessage = pickle.loads(subSocket.recv())
                        print("Leader Is Alive")
                        if(receivedMessage["MsgID"] == MsgDetails.LEADER_MEMBER_ALIVE):
                            SenderID = receivedMessage["ID"]
                            if(SenderID != LeaderID.value):
                                ManyLeadersModeLock.acquire()
                                ManyLeadersMode.value = 1
                                ManyLeadersModeLock.release()

                                FakeLeaderLock.acquire()
                                FakeLeaderID.value = SenderID
                                FakeLeaderLock.release()
                except:
                    pass
                finally:
                    ElectionModeLock.acquire()
                    ElectionMode.value = 1
                    ElectionModeLock.release()

