import pprint
import sys
import time
import zmq
import signal
from contextlib import contextmanager
from utils import *
from datetime import datetime
from dateutil.relativedelta import relativedelta

def Alive_process(MyPID,checkElection,isLeader,MachinesIPs,ManyLeadersCheck,FakeLeaderPID,LeaderPID):
    
    myIp = get_ip()
    port = 30010
    # Configure myself as subscriber all machines
    ipPort = myIp + ":" + str(port)
    MyPID = MyPID
    subsocket = None
    subcontext = None
    pubSocket = None
    pubContext = None
    print("check election :",checkElection)
    print("leaderPID:",LeaderPID.value)
    while (checkElection.value == 0) :
        #leader send periodically i'm alive msg to others machines
        if(isLeader == True):
            if(subsocket != None and subcontext != None):
                # Terminate Connection
                subsocket.close()
                subcontext.destroy()

            if(pubContext == None and pubSocket ==None):        
                test = False
                
                try:
                    #setup publisher communication
                    pubSocket, pubContext = configure_port(ipPort, zmq.PUSH, 'bind')
                    
                except:
                    print("except")
                    port +=1
                    ipPort = myIp + ":" + str(port)
                    pubSocket, pubContext = configure_port(ipPort, zmq.PUSH, 'bind')
                #fe moshkla any lma ab2a member we b3den ab2a leader f hwa msh hyzbot 3la el machine el w7da we ana bgrb 3lshan 
                # hyb2a pub socket ana kont wa5do fe el process ely at3mlha terminate bl talihyl3li an el socket 3la ghazi ana mta5d .
                # f momkn a3ml random ports kda ll mwdo3 dh ybdl fehom lw 7sl el error dh we fe el sub y3ml connect 3lehom kolhom     


            # I'm Alive Msg that will be sent periodically
            msg = {'id': MsgDetails.LEADER_MEMBER_ALIVE, 'msg': "I'm Alive",'PID':MyPID,'MachineType':MachineType.Leader}
            
            # Periodically 1 sec
            pubSocket.send(pickle.dumps(msg))
            
            print("leader send:{}".format(msg))
            time.sleep(1)
        else:#
            
            if(pubSocket != None and pubContext != None):
                # Terminate Connection
                pubSocket.close()
                pubContext.destroy()
            if(subsocket == None and subcontext == None):
                #setup subscriber communication
                subSocket, subContext = configure_multiple_ports(MachinesIPs,
                                                        range(30010,30020), zmq.PULL,True)
            LeaderAlive = False
            for i in range(machinesNumber):
                try:
                    receivedMessage = None
                    #receive alive msg from leader
                    subSocket.setsockopt(zmq.RCVTIMEO,  1000)
                    receivedMessage = pickle.loads(subSocket.recv())
                    if(receivedMessage != None):
                        LeaderAlive = True
                        print("Member recv:{}".format(receivedMessage))
                        if(LeaderPID.value != receivedMessage['PID'] ):
                            ManyLeadersCheck.value = 1
                            FakeLeaderPID.value = receivedMessage['PID']
                            checkElection.value = 1       
                except:pass
                finally:
                    time.sleep(1)
                #print("except")
            if(not LeaderAlive):
                checkElection.value = 1
            if(pubSocket != None and pubContext != None):
                # Terminate Connection
                pubSocket.close()
                pubContext.destroy()
            if(subsocket != None and subcontext != None):
                # Terminate Connection
                subsocket.close()
                subcontext.destroy()

                #TODO if received alive msg with pid not equal to leader pid
                # send to leader that new leader is elected
