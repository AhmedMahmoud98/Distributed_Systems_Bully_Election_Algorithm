import os
import sys
import zmq
import time
import pickle
from utils import *

ID = sys.argv[1]
IP = sys.argv[2]
Context = zmq.Context()
Socket = Context.socket(zmq.PUB)
Socket.connect("tcp://" + IP + ":" + PublishingPort)
time.sleep(1)

Topic = (str(ID) + "Pause").encode()
PauseMsg = {'MsgID': MsgDetails.PAUSE}
Socket.send_multipart([Topic ,pickle.dumps(PauseMsg)])

