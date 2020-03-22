
import zmq
import time
import pickle

context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.connect("tcp://192.168.1.10:5556")
time.sleep(1)


for word in ['alpha', 'beta', 'gamma', 'apple', 'carrot', 'bagel']:
    socket.send_multipart([b"al", pickle.dumps(word)])
    time.sleep(1)

