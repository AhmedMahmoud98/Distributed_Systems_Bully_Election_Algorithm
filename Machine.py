class Machine:
    def __init__(self, ip,PID, Port,isLeader=False, isAlive=False):
        self.ip = ip
        self.PID = PID
        self.Port = Port
        self.isAlive = isAlive
        self.isLeader = isLeader
