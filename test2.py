import zmq
import pickle 

context = zmq.Context()
socket = context.socket(zmq.SUB)
# accept all topics (prefixed) - default is none
socket.setsockopt_string(zmq.SUBSCRIBE, "al")
socket.bind("tcp://192.168.1.10:5556")

while True:
    topic, msg = socket.recv_multipart()
    print(topic, pickle.loads(msg))