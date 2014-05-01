from pyChord import Peer, getHash, getHashString, RPCThreading, MAINT_INT
from chordReduce import ChordReduceNode as Node  

import time
import random
import sys, traceback
from threading import Thread


CHURN_RATE = 0.002  #chance out of 1 
PORTS =  range(9101,9999)
TEST_SIZE = 20
TEST_FILE = "ti.txt"


class ExperimentNode(Node):
    def __init__(self,ip,port,instrumentation):
        Node.__init__(self,ip,port)
        #super().__init__(ip,port)
        self.inst = instrumentation
        self.addNewFunc(self.kill,"kill")
        self.addNewFunc(self.ping,"ping")
        
    def create(self):
        super(ExperimentNode,self).create()
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
        self.running = False
        self.runningLock.acquire()
        self.mapLock.acquire()
        self.server.finished = True
        self.runningLock.release()
        self.mapLock.release()
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
        self.safe = []
        self.churnThread = None
        t = Thread(target=self.server.serve_forever)
        t.start()    
        self.churn = False
        self.choosing = False


    # kill a random node in the network
    def killRandom(self): 
        self.kill(random.choice(self.aliveNodes))
        
            
    # rez a random node
    def rezRandom(self):
        self.rez(random.choice(self.deadNodes),random.choice(self.aliveNodes))
    
    # tell a node to pretend to diaf
    def kill(self,victim):
        newPort = random.choice(PORTS)
        print "Killing", victim, getHashString(victim)
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
        print "Rezzing", nodeName, getHashString(nodeName)
        try:
            Peer(nodeName).join(ringMember)
            self.deadNodes.remove(nodeName)
        except Exception as e:
            print "Error rezzing", e
    
    # splits up nodes among the dead and living according to ratio
    def setupExperiment(self):
        targets = self.aliveNodes[:]
        for node in targets:
            self.kill(node)
        print "Wanton destruction complete."
        print "alive:", self.aliveNodes
        print len(targets), len(self.deadNodes)
        time.sleep(2)
        
        print "Creating new network."
        n = random.choice(self.deadNodes)
        Peer(n).create()
        self.deadNodes.remove(n)
        ## adding the rest
        while len(self.aliveNodes) < len(self.deadNodes):
            self.rezRandom()
            time.sleep(MAINT_INT)
        print "Done."
        

        print "Storing."
        self.choosing = True
        tester = random.choice(self.aliveNodes)
        self.safe.append(tester)
        self.choosing = False
        try:
            Peer(tester).storeFile(TEST_FILE)
        except:
            print "failed."
            traceback.print_exc(file=sys.stdout)
            print "terminating."
            return
        print "Store done."
        self.safe.pop()
        
        self.startChurn()


        self.choosing = True
        tester = random.choice(self.aliveNodes)
        outputAddress = getHashString( Peer(tester).name)
        print outputAddress
        self.safe.append(tester)
        self.choosing = False
        try:
            Peer(tester).stage(TEST_FILE, outputAddress)
        except:
            print "failed."
            traceback.print_exc(file=sys.stdout)
            print "terminating."
        print "Stage done."
        self.churn = False
        #self.safe.pop()

        #print "Churning the network before we retrieve."
        #time.sleep(10)

        """
        for node in self.aliveNodes:
            try:
                print Peer(node).myInfo()
            except Exception as e:
                print e 
        self.churn = False 
        print "Done."
        """

    # public
    def checkIn(self,nodeName):
        self.aliveNodes.append(nodeName)
        print nodeName, "checked in."
        return True

    def startChurn(self):
        print "Allowing network to establish before Churn."
        time.sleep(3)
        
        self.churnThread = Thread(target=self.simulateChurn)
        self.churnThread.daemon =  True
        self.churn = True
        self.churnThread.start()
        print "Started Churn."
        time.sleep(5)

        
    def simulateChurn(self):
        while self.churn:
            if not self.choosing:    
                try:
                    time.sleep(1)
                    for node in self.aliveNodes[:]:
                        if node not in self.safe and len(self.aliveNodes) > 1 and random.random() < CHURN_RATE:
                            self.kill(node)
                    joinTargets = self.aliveNodes[:]
                    for node in self.deadNodes[:]:
                        if len(self.deadNodes) > 1  and random.random() < CHURN_RATE:
                            self.rez(node, random.choice(joinTargets))
                except Exception, e:
                    print "Error in Churn", e
        print "Churning done!"
                



    # public 
    def report(self,nodeName, data):
        return True





port = PORTS[0]-1
iNode =  InstrumentationNode("127.0.0.1",port)
n1 = ExperimentNode("127.0.0.1",port+1,iNode.name)
n2 = ExperimentNode("127.0.0.1",port+2,iNode.name)

n1.create()
n2.join(n1.name)

nodes = [n1,n2]
for i in range(3,TEST_SIZE+1):
    n = ExperimentNode("127.0.0.1",port+i, iNode.name)
    n.join(random.choice(nodes).name)
    nodes.append(n)
    time.sleep(MAINT_INT)

PORTS = PORTS[TEST_SIZE+1:]

iNode.setupExperiment()
