from SimpleXMLRPCServer import SimpleXMLRPCServer
import xmlrpclib
import hashlib
import math
import time
from threading import Thread

HASHFUNC = hashlib.sha1
HASHSIZE = HASHFUNC().digest_size * 8
MAX = 2**(HASHSIZE)
print "HASHSIZE", HASHSIZE
MAINT_INT = 0.5


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




class Peer(object,xmlrpclib.ServerProxy):
    def __init__(self,name):
        xmlrpclib.ServerProxy.__init__(self,name)
        self.name = name
        self.hashid = getHash(self.name)

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name


class Node(object):
    """
    Giant TODO:  add the peer constructors where needed
    """

    # Construction and networking
    def __init__(self,ip,port):
        self.ip = ip
        self.port = port
        self.name = "http://"+str(ip)+":"+str(port)
        self.hashid = getHash(self.name)
        self.server = SimpleXMLRPCServer(("0.0.0.0",port),logRequests=False)
        #register functions here
        self.server.register_function(self.notify,"notify")
        self.server.register_function(self.getPred,"getPred")
        self.server.register_function(self.findSuccessor,"findSuccessor")

        """
        General assumption is finger[k] = successor of (n + 2**(k-1)) % mod MAX
        1 <= k <= HASHSIZE
        The implication of this is that finger[1] = succ ((n +2**0) % mod MAX) 
        --> your successor
        """ 
        self.fingers = [None]*HASHSIZE
        self.next = 0
        self.pred = self
        self.succ = self
        self.running = False
        self.myThread = None
        t = Thread(target=self.server.serve_forever)
        t.start()


    def addNewFunc(self,func,name):
        self.server.register_function(func,name)






    ## Routing
   
    # public
    def findSuccessor(self, hexHashid):
        hashid = long(hexHashid, 16)
        if hashBetweenRightInclusive(hashid, self.hashid, self.succ.hashid):
            #print self.succ.hashid, "successor for", str(hashid) 
            return self.succ.name
        else:  # we forward the query
            closest = self.closestPreceeding(hashid)
            if closest is self:
                return self.name
            #print self.name, "forwarding", str(hashid), "to", closest.name 
            return closest.findSuccessor(hexHashid)


    def closestPreceeding(self,hashid):
        for f in reversed(self.fingers[1:]):
            if f is not None and hashBetween(f.hashid, self.hashid, hashid):
                return Peer(f.name)
        return self



    
    ## Ring creation/join
    def create(self):
        self.pred = None
        self.succ = self
        self.kickstart()

    ## We can write it simpler, no?  
    ## If we need to start maintanense manually, let's put that it a function
    def join(self,othernode):
        patron = Peer(othernode)
        hexid = hex(self.hashid)
        self.pred = None
        self.succ = Peer(patron.findSuccessor(hexid))
        self.succ.notify(self.name)
        self.kickstart()


    def kickstart(self):
        self.running = True
        self.myThread = Thread(target=self.mainloop)
        self.myThread.daemon = True
        self.myThread.start() 


    def mainloop(self):
        while(self.running):
            time.sleep(MAINT_INT)
            self.stabilize()
            self.fixFingers()
            if self.pred is not None:
                self.checkPred()


    ## maintanense


    # called periodically. n asks the successor
    # about its predecessor, verifies if n's immediate
    # successor is consistent, and tells the successor about n    
    def stabilize(self):
        x = Peer(self.succ.getPred())
        if hashBetween(x.hashid, self.hashid, self.succ.hashid):
            self.succ = x
        self.succ.notify(self.name)

    # poker thinks it might be our predecessor
    #public 
    def notify(self, poker):
        poker = Peer(poker)
        if self.pred is None or hashBetween(poker.hashid, self.pred.hashid, self.hashid):
            self.pred = poker
        return True


    def fixFingers(self):
        self.next = self.next + 1
        if self.next >= HASHSIZE:
            self.next =  1
        ## currently
        target = hex((self.hashid + 2**(self.next-1))%MAX)
        ##self.fingers[next] = Peer(self.find(hex(target)))
        ## or alternatively
        self.fingers[self.next] = Peer(self.findSuccessor(target))


    #public
    def getPred(self):
        return self.pred.name

    
    def checkPred(self):
        pass


