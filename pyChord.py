from SimpleXMLRPCServer import SimpleXMLRPCServer
import xmlrpclib
import hashlib
import math
import time
import SocketServer
from threading import Thread, RLock as Lock
import sys, traceback


"""
possible bug: fix successor is called multiple times
IE fixed successor message followed by a can't connect to the guy they just got rid of
"""


HASHFUNC = hashlib.sha1
HASHSIZE = HASHFUNC().digest_size * 8
MAX = 2**(HASHSIZE)
print "HASHSIZE", HASHSIZE
MAINT_INT = 0.2
NUM_SUCCESSORS = 3

def getHash(string):
    m = HASHFUNC(string)
    return long(m.hexdigest(),16)

def getHashString(string):
    m = HASHFUNC(string)
    return m.hexdigest()

def getDist(hash1,hash2):
    tmp = hash2-hash1
    if tmp <= 0:
        return MAX + tmp
    else:
        return tmp

#
# written from scratch; check logic
# '318663', '5eb51e', '110623'
def hashBetween(target, left, right):
    if left ==  right:
        return True
    if target == left or target == right:
        return False
    #print target, "<", right, "and", target, ">", left, target < right and target > left
    if target < right and target > left:
        return True
    #print left, ">", right, left > right 
    if left > right :
        #print left, ">", target, "and", target, "<", right, left > target and target < right
        if left > target and target < right:
            return True
        #print left, "<", target, "and", target, ">", right, left < target and target > right
        if left < target and target > right:
            return True
    return False

def hashBetweenRightInclusive(target,left,right):
    if target == right:
        return True
    return hashBetween(target, left, right)



class RPCThreading(SocketServer.ThreadingMixIn, SimpleXMLRPCServer):
    finished = False;
    
    def serve_forever(self):
        while not self.finished:
            self.handle_request()


class Peer(object,xmlrpclib.ServerProxy):
    def __init__(self,name):
        xmlrpclib.ServerProxy.__init__(self,name)
        self.name = name
        self.hashid = getHash(self.name)

    def clone(self):
        return Peer(self.name)
    
    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name

class Node(object):

    # Construction and networking
    def __init__(self,ip,port):
        self.ip = ip
        self.port = port
        self.name = "http://"+str(ip)+":"+str(port)
        self.hashid = getHash(self.name)
        self.server = RPCThreading(("",port),logRequests=False)
        #register functions here
        self.server.register_function(self.notify,"notify")
        self.server.register_function(self.getPred,"getPred")
        self.server.register_function(self.find,"find")
        self.server.register_function(self.findSuccessor,"findSuccessor")
        self.server.register_function(self.isAlive,"isAlive")
        self.server.register_function(self.alert,"alert")
        self.server.register_function(self.join,"join")
        self.server.register_function(self.create,"create")
        self.server.register_function(self.getSuccessorList,"getSuccessorList")
        self.server.register_function(self.getPredID,"getPredID")
        #finger[k] = successor of (n + 2**(k-1)) % mod MAX, 1 <= k <= HASHSIZE 
        self.fingers = [None]*HASHSIZE # finger[k] = successor of (n + 2**(k-1)) % mod MAX, 1 <= k <= HASHSIZE
        self.next = 0
        self.pred = self
        self.succ = self
        self.successorList = [self.name]*NUM_SUCCESSORS
        self.running = False
        self.myThread = None
        self.successorLock =  Lock()
        self.runningLock = Lock()
        t = Thread(target=self.server.serve_forever)
        t.daemon = True
        t.start()


    def addNewFunc(self,func,name):
        self.server.register_function(func,name)
    
    def clone(self):
        return Peer(self.name)

    def getSuccessorList(self):
        temp = self.successorList[:]
        return temp

    def getPredID(self):
        if self.pred is None:
            return self.hashid
        return self.pred.hashid


## Routing
    def findSuccessor(self,hexHashid,dataRequest = False):
        trace = [self.name]
        closest, done = self.find(hexHashid,dataRequest)
        trace.append(closest)
        while(not done):
            try:
                closest, done = Peer(closest).find(hexHashid,dataRequest)
                trace.append(closest)
            except Exception as e:
                print self.name, "Could not connect to next hop", closest, e
                self.removeNodeFromFingers(closest)
                if len(trace) > 0:
                    last = trace.pop()
                    try:
                        Peer(last).alert(closest)
                    except:
                        print self.name, "couldn't alert", last
                    finally:
                        closest = last
                else:
                    print self.name, "I'm out of options"
                    closest, done = self.find(hexHashid,dataRequest)
                    trace = [self.name]
        return closest


  # public
    def find(self, hexHashid,dataRequest):
        #print self.name, "finding", hexHashid
        hashid = long(hexHashid, 16)
        if hashBetweenRightInclusive(hashid, self.hashid, self.succ.hashid):
            #print self.succ.hashid, "successor for", str(hashid) 
            return self.succ.name, True
        else: # we forward the query
            closest = self.closestPreceeding(hashid)
            if closest == self.name:  #changed
                return self.name, True
        #print self.name, "forwarding", str(hashid), "to", closest.name  
            return closest, False

    def closestPreceeding(self,hashid):
        for f in reversed(self.fingers[1:]):
            if f is not None and hashBetween(f.hashid, self.hashid, hashid):
                return f.name
        return self.name



    
    ## Ring creation/join
    def create(self):
        self.pred = None
        self.succ = self
        self.kickstart()
        return True

    ## We can write it simpler, no?  
    ## If we need to start maintanense manually, let's put that it a function
    ## public
    def join(self,othernode):
        patron = Peer(othernode)
        hexid = hex(self.hashid)
        self.pred = None
        try:
            self.succ = Peer(patron.findSuccessor(hexid))
            self.successorList[0] = self.succ.name
        except Exception as e:
            print "I could not find the server you indicated.\n Go Away.",e
            traceback.print_exc(file=sys.stdout)
            return False
            #raise Exception("Failed to connect")
        try:
            Peer(self.succ.name).notify(self.name)
        except Exception:
            self.succ = patron
            try:
                Peer(self.succ.name).notify(self.name)
            except Exception:
                print "I could not find the patron you indicated.\n Go Away."
                traceback.print_exc(file=sys.stdout)
                return False
        self.kickstart()
        return True


    def kickstart(self):
        self.running = True
        self.myThread = Thread(target=self.mainloop)
        self.myThread.daemon = True
        self.myThread.start() 


    def mainloop(self):
        self.runningLock.acquire()
        while(self.running):
            try: #mainloop must never die!
                time.sleep(MAINT_INT)
                self.stabilize()
                self.fixFingers()
                if self.pred is not None:
                    self.checkPred()
            except Exception as e:
                print "MAINLOOP EXCEPTION",e
                traceback.print_exc(file=sys.stdout)
        self.runningLock.release()



    ## maintanense
    # called periodically. n asks the successor
    # about its predecessor, verifies if n's immediate
    # successor is consistent, and tells the successor about n    
    def stabilize(self):
        done = False
        while(not done):
            try:
                sucessorPredName = Peer(self.succ.name).getPred()
                done = True
            except Exception: #my sucessor died on me
                print self.name, "my successor died in stabilize", self.succ.name
                done = False
                self.fixSuccessor()
        if sucessorPredName != "":
            x = Peer(sucessorPredName)
            if hashBetween(x.hashid, self.hashid, self.succ.hashid):
                self.succ = x
                self.successorList[0] = self.succ.name
        try:
            Peer(self.succ.name).notify(self.name)
        except Exception as e:
            print self.name, "Failed to notify", self.succ.name
            #traceback.print_exc(file=sys.stdout)
            self.fixSuccessor()
        else:
            self.updateSuccessorList()  

    # return true if poker is better pred, false if he's the same.
    # public 
    def notify(self, poker):
        poker = Peer(poker)
        if self.pred is None or hashBetween(poker.hashid, self.pred.hashid, self.hashid):
            self.pred = poker
            return True
        return False


    def fixFingers(self):
        self.next = self.next + 1
        if self.next >= HASHSIZE:
            self.next =  1
        ## currently
        target = hex((self.hashid + 2**(self.next-1))%MAX)
        self.fingers[self.next] = Peer(self.findSuccessor(target))
        
        


    def updateSuccessorList(self):
        self.successorLock.acquire()
        try:
            self.successorList = [self.succ.name] + Peer(self.succ.name).getSuccessorList()[:-1]
        except Exception as e:
            print self.name, "updateSuccessorList failed on successor", self.succ.name
            self.fixSuccessor()
        finally:
            self.successorLock.release()


    def fixSuccessor(self):  #called when MY IMMEDIATE successor fails
        self.successorLock.acquire()
        self.removeNodeFromFingers(self.succ.name)
        self.succ = Peer(self.successorList[1])
        try:
            Peer(self.succ.name).isAlive()
        except Exception:
            if(len(self.successorList) == 2):
                print self.name, "I'm all alone"
                self.succ = self
                self.successorList  = [self.name]*NUM_SUCCESSORS
            else:
                self.successorList = self.successorList[1:]
                self.fixSuccessor()
        else:
            self.updateSuccessorList()
            print self.name, "fixed successor", self.succ.name
        finally:
            self.successorLock.release()
            
            
    def fixSuccessorList(self,failedSucc):  # called when a specific successor encounters failure
        mySucc = Peer(self.succ.name)
        try:
            mySucc.alert(failedSucc)
            self.updateSuccessorList()
            print self.name, "Fixed successor list"
        except Exception:
            print self.name, "Tried to fix successor list but successor is gone!", self.succ.name
            self.fixSuccessor()

    def removeNodeFromFingers(self,nodeName):
        for i in range(1,len(self.fingers)):
            f = self.fingers[i]
            if f is not None:
                if f.name == nodeName:
                    self.fingers[i] = None

    # I was alerted of a failed lookup
    # public
    def alert(self,failedName):
        if(failedName == self.succ.name):
            self.fixSuccessor()
        elif failedName in successorList:
            self.fixSuccessorList(failedName)
            self.removeNodeFromFingers(failedName)
        else:
            self.removeNodeFromFingers(failedName)
        return True





    def checkPred(self): #we should implement this someday
        try:
            return Peer(self.pred.name).isAlive()
        except Exception: # he's dead, Jim
            self.pred = None
            return False


    #public
    def getPred(self):
        if self.pred is not None:
            return self.pred.name
        else:
            return ""

    # public
    def isAlive(self):
        return True
