class Machine:
    def __init__(self, IP, ID, IsLeader = False, IsAlive = True):
        self.IP = IP
        self.ID = ID
        self.IsAlive = IsAlive
        self.IsLeader = IsLeader
