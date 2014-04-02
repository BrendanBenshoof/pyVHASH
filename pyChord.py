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
        return MAX-tmp
    else:
        return tmp

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
        self.fingers = [None]*HASHSIZE
        self.pred = self
        self.running = False
        self.myThread = None
        t = Thread(target=self.server.serve_forever)
        t.start()

    def addnewFunc(self,func,name):
        self.server.register_function(func,name)

    def getBestForward(self, hashid):#double check this logic
        #print "pred",self.pred
        bestdist = getDist(self.pred.hashid, hashid)
        bestpeer = None
        for p in self.fingers:
            if not p is None:
                #print p
                tmp = getDist(p.hashid,hashid)
                if tmp < bestdist:
                    bestdist = tmp
                    bestpeer = p
        return bestpeer

    def join(self,othernode):
        patron = Peer(othernode)
        hexid = hex(self.hashid)
        #print patron, hexid
        #time.sleep(1)
        parentstr = patron.find(hexid)
        #print "postfind"
        parent = Peer(parentstr)
        self.fingers[0] = Peer(parent.notify(self.name))
        ##find my parent
        self.running = True
        self.myThread = Thread(target=self.mainloop)
        self.myThread.daemon = True
        self.myThread.start()        
        
        #notify my parent
        #start life as a node
        pass

    #public
    def find(self,hashstr):
        loc = long(hashstr,16)
        best = self.getBestForward(loc)
        if best is None:
            return self.name
        else:
            #print "best", best
            return best.find(hashstr)
        #recursively lookup the node nearest to this loc
        

    #public
    def notify(self,possible_pred):#inputs a list of 
        newpred = Peer(possible_pred)
        newdist = getDist(newpred.hashid, self.hashid)
        olddist = getDist(newpred.hashid, self.pred.hashid)
        if newdist < olddist:
            self.pred = newpred
            return self.name
        else:
            return self.pred.name

    
    def mainloop(self):
        while(self.running):
            for i in range(1,len(self.fingers)):
                time.sleep(0.5)
                if self.fingers[0]:
                    self.fingers[0].notify(self.name)
                ideal_id = (self.hashid + 2**i)%self.hashid
                self.fingers[i] = Peer(self.find(hex(ideal_id)))

        pass


