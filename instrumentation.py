from pyChord import Node, Peer, getHashString, RPCThreading

class InstrumentationNode(object):
    
    def __init__(self):
        self.ip = ip
        self.port = port
        self.name = "http://"+str(ip)+":"+str(port)
        self.hashid = getHash(self.name)
        self.server = RPCThreading(("",port),logRequests=False)
        #register functions here
        self.server.register_function(self.checkIn,"checkIn")
        self.aliveNodes = []
        self.deadNodes = []
        
        
    def kill(self, node):
        Peer(node.name).kill()
    
    def checkIn(self,node, status = True):
