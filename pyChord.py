from SimpleXMLRPCServer import SimpleXMLRPCServer
import xmlrpclib
import hashlib
import math
import time
import SocketServer
from threading import Thread
import sys, traceback

HASHFUNC = hashlib.sha1
HASHSIZE = HASHFUNC().digest_size * 8
MAX = 2**(HASHSIZE)
print "HASHSIZE", HASHSIZE
MAINT_INT = 0.5
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
def hashBetween(target, left, right):
    if left ==  right:
        return True
    if target == left or target == right:
        return False
    if target < right and target > left:
        return True
    if left > right :
        if left > target and target < right:
            return True
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
    
    def __del__(self):
        print "Server gone"


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
        #finger[k] = successor of (n + 2**(k-1)) % mod MAX, 1 <= k <= HASHSIZE 
        self.fingers = [None]*HASHSIZE # finger[k] = successor of (n + 2**(k-1)) % mod MAX, 1 <= k <= HASHSIZE
        self.next = 0
        self.pred = self
        self.succ = self
        self.successorList = [self.name]*NUM_SUCCESSORS
        self.running = False
        self.myThread = None
        t = Thread(target=self.server.serve_forever)
        t.start()

    def addNewFunc(self,func,name):
        self.server.register_function(func,name)
    
    def clone(self):
        return Peer(self.name)

    def getSuccessorList(self):
        return self.successorList[:]


## Routing
    def findSuccessor(self,hexHashid):
        trace = [self.name]
        closest, done = self.find(hexHashid)
        trace.append(closest)
        while(not done):
            try:
                closest, done = Peer(closest).find(hexHashid)
                trace.append(closest)
            except Exception:
                print "Could not connect to", closest
                self.removeNodeFromFingers(closest)
                if len(trace) > 0:
                    last = trace.pop()
                    Peer(last).alert(closest)
                    closest = last
                else:
                    closest, done = self.find(hexHashid)
                    trace = [self.name]
        return closest


  # public
    def find(self, hexHashid):
        #print self.name, "finding", hexHashid
        hashid = long(hexHashid, 16)
        if hashBetweenRightInclusive(hashid, self.hashid, self.succ.hashid):
            #print self.succ.hashid, "successor for", str(hashid) 
            return self.succ.name, True
        else: # we forward the query
            closest = self.closestPreceeding(hashid)
            if closest is self.name:
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
            print e
            print "I could not find the server you indicated.\n Go Away."
            return False
            #raise Exception("Failed to connect")
        try:
            self.succ.notify(self.name)
        except Exception:
            self.succ = patron
            try:
                self.succ.notify(self.name)
            except Exception:
                print "I could not find the patron you indicated.\n Go Away."
                return False
        self.kickstart()
        return True


    def kickstart(self):
        self.running = True
        self.myThread = Thread(target=self.mainloop)
        self.myThread.daemon = True
        self.myThread.start() 


    def mainloop(self):
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
                print self.name, "my successor died on me", self.successorList
                done = False
                self.fixSuccessor()
        if sucessorPredName != "":
            x = Peer(sucessorPredName)
            if hashBetween(x.hashid, self.hashid, self.succ.hashid):
                self.succ = x
                self.successorList[0] = self.succ.name
        try:
            self.succ.notify(self.name)
            # no idea why this is a nonetype error initially when the first node is talking to himself
            self.updateSuccessorList(self)  
        except Exception as e:
            print self.name, e, self.successorList 
            self.fixSuccessor()



    # poker thinks it might be our predecessor
    #public 
    def notify(self, poker):
        poker = Peer(poker)
        if self.pred is None or hashBetween(poker.hashid, self.pred.hashid, self.hashid):
            self.pred = poker
            return True
        return False


    def updateSuccessorList(self):
        try:
            self.successorList = [self.succ.name] + Peer(self.succ.name).getSuccessorList()[:-1]
        except Exception as e:
            print self.name, e, "My successor failed during list request" 
            self.fixSuccessor()

    def fixFingers(self):
        self.next = self.next + 1
        if self.next >= HASHSIZE:
            self.next =  1
        ## currently
        target = hex((self.hashid + 2**(self.next-1))%MAX)
        ##self.fingers[next] = Peer(self.find(hex(target)))
        ## or alternatively
        self.fingers[self.next] = Peer(self.findSuccessor(target))

    def fixSuccessor(self):  #called when MY IMMEDIATE successor fails
        self.removeNodeFromFingers(self.succ.name)
        self.succ = Peer(self.successorList[1])
        try:
            self.successorList = [self.succ.name] + Peer(self.succ.name).getSuccessorList()[:-1]
            print self.name, "fixed successor", self.succ.name, " and list", self.successorList
        except Exception:
            if(len(self.successorList) == 2):
                print self.name, "I'm all alone"
                self.succ = self
                self.successorList  = [self.name]*NUM_SUCCESSORS
            else:
                self.successorList = self.successorList [1:]
                self.fixSuccessor()

    def fixSuccessorList(self,failedSucc):  # called when a specific successor encounters failure
        mySucc = Peer(self.succ.name)
        try:
            mySucc.alert(failedSucc)
            self.successorList = [self.succ.name] + Peer(self.succ.name).getSuccessorList()[:-1]
            print self.name, "Fixed successor list"
        except Exception:
            print self.name, "My successor is gone!"
            self.fixSuccessor()
            
    def removeNodeFromFingers(self,nodeName):
        for i in range(1,len(self.fingers)):
            f = self.fingers[i]
            if f is not None:
                if f.name == nodeName:
                    self.fingers[i] = None

    # I was alerted of a failed lookup
    def alert(self,failedName):
        if(failedName == self.succ.name):
            self.fixSuccessor()
        else:
            self.removeNodeFromFingers(failedName)
        return True





    def checkPred(self): #we should implement this someday
        try:
            return Peer(self.pred.name).isAlive()
        except Exception: # he's dead, Jim
            self.pred = None


    #public
    def getPred(self):
        if self.pred is not None:
            return self.pred.name
        else:
            return ""

    # public
    def isAlive(self):
        return True
        
