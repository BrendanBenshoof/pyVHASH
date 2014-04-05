from SimpleXMLRPCServer import SimpleXMLRPCServer
import xmlrpclib
import hashlib
import math
import time
from threading import Thread

HASHFUNC = hashlib.sha1
HASHSIZE = HASHFUNC().digest_size * 8
HALF = 2**(HASHSIZE/2)
print "HASHSIZE", HASHSIZE

def loc2String(loc):
    return map(lambda x: hex(x)[2:-2],loc)

def string2loc(loc):
    return map(lambda x: long(x,16),loc)

def getHash(string):
    m = HASHFUNC(string)
    return long(m.hexdigest(),16)

def getLoc(hashid):
    x = hashid % HALF
    y = hashid / HALF
    return (x,y)

def Name2loc(name):
    return getLoc(getHash(name))
def Name2locString(name):
    return tuple(loc2String(getLoc(getHash(name))))

def getDist(loc1,loc2):
    delta_x = (loc2[0]-loc1[0])
    delta_y = (loc2[1]-loc1[1])
    return (delta_x**2.0+delta_y**2.0)**0.5

def getMidpoint(loc1,loc2):#nasty question in a modulus space, there really are 2 midpoints
    delta_x = (loc2[0]-loc1[0])
    delta_y = (loc2[1]-loc1[1])
    return ((loc1[0]+delta_x)%HALF,(loc1[1]+delta_y)%HALF)

def cleanup_peers(peers, myname):
    output = []
    for p in peers:
        if not p is None and not p.name==myname:
            output.append(p)
    return output

class Peer(object,xmlrpclib.ServerProxy):
    def __init__(self,name):
        xmlrpclib.ServerProxy.__init__(self,name)
        self.name = name
        self.hashid = getHash(self.name)
        self.loc = getLoc(self.hashid)

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
        self.loc = getLoc(self.hashid)
        self.server = SimpleXMLRPCServer((ip,port),logRequests=False)
        #register functions here
        self.server.register_function(self.find,"find")
        self.server.register_function(self.notify,"notify")
        self.nearPeers = []
        self.farPeers = []
        self.peerpool = set()
        self.running = False
        self.myThread = None
        t = Thread(target=self.server.serve_forever)
        t.start()

    def addNewFunc(self,func,name):
        self.server.register_function(func,name)

    def getBestForward(self, loc):
        mydist = getDist(self.loc,loc)
        try:
            bestpeer = min(self.nearPeers+self.farPeers, key= lambda x: getDist(loc,x.loc))
        except ValueError:
            return None
        peerdist = getDist(bestpeer.loc,loc)
        if mydist < peerdist:
            return None #I am the owner
        else:
            return bestpeer

    def evaluatePeers(self,peerpool):
        finalpeers = []
        failpeers = []
        if len(peerpool)>0:
            peerpool = map(lambda x: Peer(x),peerpool)
            peerpool = cleanup_peers(peerpool,self.name)
            peerpool = sorted(peerpool,key= lambda x: getDist(self.loc,x.loc))
            finalpeers.append(peerpool.pop())
            for p in peerpool:
                mid = getMidpoint(self.loc,p.loc)
                mydist = getDist(self.loc,mid)
                competitor_dist = min(map(lambda x: getDist(x.loc,mid), finalpeers) )
                if mydist <= competitor_dist:
                    finalpeers.append(p)
                else:
                    failpeers.append(p)
        return finalpeers, failpeers



    def join(self,othernode):
        patron = Peer(othernode)
        self.peerpool.add(othernode) #add our know node for shits and giggles
        locstr = loc2String(self.loc)
        parentstr = patron.find(locstr)
        parent = Peer(parentstr) #find my parent
        self.peerpool.add(parentstr) #add my parent
        self.nearPeers, self.farPeers = self.evaluatePeers(list(self.peerpool)) #update my peerlist
        for n in parent.notify([self.name]+map(str,self.nearPeers)):#send a notify to my parent
            self.peerpool.add(n)
        self.nearPeers, self.farPeers = self.evaluatePeers(list(self.peerpool)) #update my peerlist
        self.running = True
        self.myThread = Thread(target=self.mainloop)
        self.myThread.daemon = True
        self.myThread.start()
        
        #notify my parent
        #start life as a node
        pass

    #public
    def find(self,locstr):
        loc = string2loc(locstr)
        best = self.getBestForward(loc)
        if best is None:
            return self.name
        else:
            return best.find(locstr)
        #recursively lookup the node nearest to this loc
        

    #public
    def notify(self,peerlist):#inputs a list of 
        for p in peerlist:
            self.peerpool.add(p)
        #tell me you and your friends exist
        return [self.name]+map(str,self.nearPeers)

    
    def mainloop(self):
        while(self.running):
            self.nearPeers, self.farPeers = self.evaluatePeers(list(self.peerpool)) #update my peerlist
            for p in self.nearPeers:
                p = Peer(p.name)
                p.notify([self.name]+map(str,self.nearPeers))
        pass


