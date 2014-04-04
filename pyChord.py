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



def getHash(string):
    m = HASHFUNC(string)
    return long(m.hexdigest(),16)

def getDist(hash1,hash2):
    tmp = hash2-hash1
    if tmp <= 0:
        return MAX + tmp
    else:
        return tmp

#
# written from scratch; check logic
def hashBetween(target, left, right):
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


"""
def hashBetweenRightInclusive(target,left,right):
    if left == right:
        ##Why?  all that means is that we're checking the same place.
        # IF that implies something specific here, it should be on a case by case basis, no?  
        return True
    if target == left:
        return False
    if target == right:
        return True
    if right < left: #this is the complement range
        if target > left or target < right:
            return True
        else:
            return False
    if left > right and target > left and target < right:
        return True
    return False
"""


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
        self.server = SimpleXMLRPCServer((ip,port),logRequests=False)
        #register functions here
        self.server.register_function(self.find,"find")
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

    def kickstart(self):
        self.running = True
        self.myThread = Thread(target=self.mainloop)
        self.myThread.daemon = True
        self.myThread.start()    



    ## Routing
    def getBestForward(self, hashid):
        #print "pred",self.pred
        if hashBetweenRightInclusive(hashid, self.hashid, self.succ.hashid):
            # in original implementation we had set it up 
            # so that if you were your own pred, you owned everything.  
            # I don't remember if we changed that.
            if self.name == self.succ.name:   #double check this logic!!!!!
                return None
            return self.succ
        else:
            for f in reversed(self.fingers[1:]):  ## 1
                if f and hashBetween(f.hashid, self.hashid, hashid):  
                    return f
        return self  # we should return self here

    #public
    """Technically, the way you find where something is is findSuccessor, 
    or in this case getBestForward, 
    ie do we really need this?"""
    def find(self,hashstr):
        loc = long(hashstr,16)
        best = self.getBestForward(loc)
        if best is None:
            return self.name
        else:
            print "best", best, self.name
            return Peer(best.name).find(hashstr)
        #recursively lookup the node nearest to this loc


    """Alternatively"""
    def findSuccessor(self, hashid):
        if hashid == self.hashid:
            return Peer(self.name)
        if hashBetweenRightInclusive(hashid, self.hashid, self.succ.hashid):
            return Peer(succ.name)
        else:  # we forward the query
            closest = closestPreceeding(hashid)
            return closest.findSuccessor(hashid)

    def closestPreceeding(self,hashid):
        for f in reversed(self.fingers[1:]):
            if f is not None and hashBetween(f.hashid, self.hashid, hashid):
                return f
        return self






    
    ## Ring creation/join

    def create(self):
        self.pred = None
        self.succ = self

    ## We can write it simpler, no?  
    ## If we need to start maintanense manually, let's put that it a function
    def join(self,othernode):
        patron = Peer(othernode)
        hexid = hex(self.hashid)
        #print patron, hexid
        #time.sleep(1)
        parentstr = patron.find(hexid)
        #print "postfind"
        parent = Peer(parentstr)
        self.succ = parent
        self.fingers[0] = self.succ
        self.succ.notify(self.name)
        ##find my parent
        self.running = True
        self.myThread = Thread(target=self.mainloop)
        self.myThread.daemon = True
        self.myThread.start()        
        
        #notify my parent
        #start life as a node




    ## maintenanse
    """TODO:  Add in Peer constructors at the appropriate locations""" 
    # called periodically. n asks the successor
    # about its predecessor, verifies if n's immediate
    # successor is consistent, and tells the successor about n
    #public
    def stabilize(self):
        x = Peer(self.succ.getPred()) #can I do that?   #probably peer stuff needed
        if hashBetween(x.hashid, self.hashid, self.succ.hashid):
            self.succ = x
        self.succ.notify(self)

    # poker thinks it might be our predecessor
    def notify(self, poker):
        if self.pred is None or hashBetween(poker.hashid, self.pred.hashid, self.hashid):
            self.pred = poker

    def fixFingers(self):
        self.next = next + 1
        if self.next > HASHSIZE:
            self.next =  1
        ## currently
        target = (self.hashid + 2**(next-1))%MAX
        ##self.fingers[next] = Peer(self.find(hex(target)))
        ## or alternatively
        self.fingers[next] =self.findSuccessor(target)


    #public
    def getPred(self):
        return self.pred.name

    








    """
    def notify(self,possible_pred):#inputs a list of 
        #return best guess for callers predecessor
        newguy = Peer(possible_pred)
        if self.succ.name == self.name: # I am my own grandpa
            self.succ = newguy
            self.pred = newguy
            return True
        #print "mypred",(self.pred)
        if hashBetweenRightInclusive(newguy.hashid, self.pred.hashid , self.hashid):
            oldpred = self.pred.name
            self.pred = newguy
            return True
        else:
            return False
    """



    
    def mainloop(self):
        while(self.running):
            i = 0
            if self.succ.name != self.name:
                self.succ = Peer(self.find((hex((self.hashid+1)%MAX))))#find successor
                his_pred = Peer(self.succ.getPred())
                if hashBetweenRightInclusive(his_pred.hashid,self.hashid,self.succ.hashid):
                    self.succ = his_pred
                self.succ.notify(self.name)
                self.check_pred()
                #notfied by successor
            self.update_finger(i)
            i = (i+1)%HASHSIZE

    def check_pred(self):
        newpred = self.find(hex(self.pred.hashid))
        self.pred = Peer(newpred)

    def update_finger(self,i):
        time.sleep(0.5)
        ideal_id = (self.hashid + 2**i)%MAX
        self.fingers[i] = Peer(self.find(hex(ideal_id)))



print hashBetween(1,80,80)
print hashBetween(3,7,4)
print hashBetween(6,2,4)
print range(0,10)[1:]