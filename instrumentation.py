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
        self.addNewFunc(self.kill,"kill")
        
    def create(self):
        super(ExperimentNode,self).create()
        print "derping"
        Peer(self.inst).checkIn(self.name)
        
    def join(self,nodeName):
        super(ExperimentNode,self).join(nodeName)
        Peer(self.inst).checkIn(self.name)
        
    def ping(self,nodeName):
        try:
            print "pinging",nodeName
            Peer(nodeName).isAlive()
            print "Why, it's a miracle!"
        except:
            print "He's dead, Jim."
            
    
    # public
    # paradigms
    """
        alternatives
        1) rerun init if possible
        2) manually stop threads, change IP, PORT, and create a new server
      
    """
    def kill(self,newPort, polite = False):
        self.server.finished = True
        x = self
        self = self.__init__(self.ip,newPort,self.inst)
        return True

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
    def kill(self, nodeName,newPort):
        print "Killing", nodeName
        try:
            Peer(nodeName).kill(newPort)
        except:
            print "He's already dead"
            
    
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

nodes = [n1,n2]
for i in range(3,5):
    n = ExperimentNode("127.0.0.1",port+i, iNode.name)
    n.join(random.choice(nodes).name)
    nodes.append(n)
    time.sleep(0.5)

time.sleep(0.5)
target = n2.name
iNode.kill(target,9180)
n1.ping(target)
time.sleep(0.5)
