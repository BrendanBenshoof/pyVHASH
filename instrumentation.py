from pyChord import Node, Peer, getHashString, RPCThreading
import time
import random



CHURN_RATE = 0.025

class InstrumentationNode(object):
    
    def __init__(self):
        self.ip = ip
        self.port = port
        self.name = "http://"+str(ip)+":"+str(port)
        self.hashid = getHash(self.name)
        self.server = RPCThreading(("",port),logRequests=False)
        #register functions here
        self.server.register_function(self.checkIn,"checkIn")
        self.server.register_function(self.report,"report")
        self.aliveNodes = []
        self.deadNodes = []
        
        
    # kill a random node in the network
    def killRandom(self):
        pass 
    
    # rez a random node
    def rezRandom(self):
        pass
    
    # tell a node to pretend to diaf
    def kill(self, nodeName):
        Peer(node.name).kill()
    
    
    # give a node a new identity and place it among the living 
    def rez(self, nodeName):
        pass
    
    # splits up nodes among the dead and living according to ratio
    def setupExperiment(self, ratio = 0.5 ):
        pass
        
    # public
    def checkIn(self,nodeName):
        aliveNode.append(nodeName)
        return True
        
    # public 
    def report(self,nodeName, data):
        return True
