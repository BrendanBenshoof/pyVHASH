from pyChord import Peer, getHash, getHashString, RPCThreading
from ChordDHT import DHTnode as Node  

import time
import random
from threading import Thread


CHURN_RATE = 0.025  #chance out of 1 
PORTS =  range(9101,9999)


class ExperimentNode(Node):
    def __init__(self,ip,port,instrumentation):
        Node.__init__(self,ip,port)
        #super().__init__(ip,port)
        self.inst = instrumentation
        self.addNewFunc(self.kill,"kill")
        self.addNewFunc(self.ping,"ping")
        
    def create(self):
        super(ExperimentNode,self).create()
        print "derping"
        Peer(self.inst).checkIn(self.name)
        return True
        
    def join(self,nodeName):
        super(ExperimentNode,self).join(nodeName)
        Peer(self.inst).checkIn(self.name)
        return True
        
    def ping(self,nodeName):
        try:
            print self.name, "pinging",nodeName
            Peer(nodeName).isAlive()
        except Exception as e:
            print self.name, e
        return True
    
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
        self.churn = False 


    # kill a random node in the network
    def killRandom(self): 
        self.kill(random.choice(self.aliveNodes))
        
            
    # rez a random node
    def rezRandom(self):
        self.rez(random.choice(self.deadNodes),random.choice(self.aliveNodes))
    
    # tell a node to pretend to diaf
    def kill(self,victim):
        newPort =random.choice(PORTS)
        print "Killing", victim
        try:
            oldPort =  int(victim[victim.rfind(":")+1:])
            newName =  victim[:victim.rfind(":")+1]+str(newPort)
            Peer(victim).kill(newPort)
            self.aliveNodes.remove(victim)
            self.deadNodes.append(newName)
            PORTS.remove(newPort)
            PORTS.append(oldPort) #do at end
        except Exception as e:
            print "He's already dead", e
    
    # add node back in
    def rez(self, nodeName, ringMember):
        print "Rezzing", nodeName
        try:
            Peer(nodeName).join(ringMember)
            self.deadNodes.remove(nodeName)
        except Exception as e:
            print "Error rezzing", e
    
    # splits up nodes among the dead and living according to ratio
    def setupExperiment(self):
        print "Killing all nodes and giving them new ports"
        for node in self.aliveNodes[:]:
            self.kill(node)
        print "Wanton destruction complete.", self.aliveNodes
        time.sleep(2)
        print "Creating new network",self.deadNodes
        time.sleep(2)
        n = random.choice(self.deadNodes)
        Peer(n).create()
        self.deadNodes.remove(n)
        ## adding the rest
        while len(self.aliveNodes) < len(self.deadNodes):
            self.rezRandom()
            time.sleep(1)
        print "Done."
        print self.aliveNodes
        print self.deadNodes
        time.sleep(3)
        print "Testing."
        
        
        for i in range(0,100):
            try:
                print i
                Peer(random.choice(self.aliveNodes)).store(str(i)+"blah",str(i))
            except Exception as e:
                print e
        
        for i in range(0,100):
            try:
                print Peer(random.choice(self.aliveNodes)).retrieve(str(i)+"blah")
            except Exception as e:
                print e
    # public
    def checkIn(self,nodeName):
        self.aliveNodes.append(nodeName)
        print nodeName, "checked in."
        return True
        
    def simulateChurn(self):
        while churn:
            try:
                time.sleep(1)
                if len(self.aliveNodes > 1) and random.random() < CHURN_RATE:
                    self.killRandom()
                if len(self.deadNodes > 1)  and random.random() < CHURN_RATE:
                    self.rezRandom()
            except Exception, e:
                print "Error in Churn", e
                



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
for i in range(3,9):
    n = ExperimentNode("127.0.0.1",port+i, iNode.name)
    n.join(random.choice(nodes).name)
    nodes.append(n)
    time.sleep(0.75)


iNode.setupExperiment()
