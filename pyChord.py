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

def hashBetweenRightInclusive(target,left,right):
    if left == right:
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
        self.fingers = [None]*HASHSIZE
        self.pred = self
        self.succ = self
        self.running = False
        self.myThread = None
        t = Thread(target=self.server.serve_forever)
        t.start()

    def addnewFunc(self,func,name):
        self.server.register_function(func,name)

    def getBestForward(self, hashid):#double check this logic!!!!!
        #print "pred",self.pred
        if hashBetweenRightInclusive(hashid, self.hashid, self.succ.hashid):
            if self.name == self.succ.name:
                return None
            return self.succ
        else:
            for f in reversed(self.fingers):
                if f and hashBetweenRightInclusive(f.hashid, self.hashid, hashid):
                    return f
        return None

        
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
        pass

    def kickstart(self):
        self.running = True
        self.myThread = Thread(target=self.mainloop)
        self.myThread.daemon = True
        self.myThread.start()    
    #public
    def find(self,hashstr):
        loc = long(hashstr,16)
        best = self.getBestForward(loc)
        if best is None:
            return self.name
        else:
            print "best", best, self.name
            return Peer(best.name).find(hashstr)
        #recursively lookup the node nearest to this loc
        
    #public
    def getPred(self):
        return self.pred.name

    #public
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




