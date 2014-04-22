from pyChord import Node, Peer, getHash, getHashString, RPCThreading

import time
import random
from threading import Thread


CHURN_RATE = 0.025

class ExperimentNode(Node):
    def __init__(self,ip,port,instrumentation):
        Node.__init__(self,ip,port)
        #super().__init__(ip,port)
        self.inst = instrumentation
        
    def create(self):
        super(ExperimentNode,self).create()
        print "derping"
        Peer(self.inst).checkIn(self.name)
        
    def join(self,nodeName):
        super(ExperimentNode,self).join(nodeName)
        Peer(self.inst).checkIn(self.name)
        
        

class InstrumentationNode(object):
    def __init__(self,ip,port):
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
        t = Thread(target=self.server.serve_forever)
        t.start()

        
        
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
        self.aliveNodes.append(nodeName)
        print nodeName, "checked in."
        return True
        
    # public 
    def report(self,nodeName, data):
        return True



port = 9100
iNode =  InstrumentationNode("127.0.0.1",port)
n1 = ExperimentNode("127.0.0.1",port+1,iNode.name)
n2 = ExperimentNode("127.0.0.1",port+2,iNode.name)

n1.create()
n2.join(n1.name)

