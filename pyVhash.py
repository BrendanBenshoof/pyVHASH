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

def Name2locString(name):
    return loc2String(getLoc(getHash(name)))

def getDist(loc1,loc2):
    deltax = min( [math.fabs(loc1[0]-loc2[0]), HALF-math.fabs(loc1[0]-loc2[0])**2.0])
    deltay = min( [math.fabs(loc1[1]-loc2[1]), HALF-math.fabs(loc1[1]-loc2[1])**2.0])
    return (deltax + deltay)**2.0

def getMidpoint(loc1,loc2):#nasty question in a modulus space, there really are 2 midpoints
    delta_x = (loc1[0]-loc2[0])/2
    delta_y = (loc1[1]-loc2[1])/2
    return (loc1[0]+delta_x,loc1[1]+delta_y)

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

    def addnewFunc(self,func,name):
        self.server.register_function(func,name)

    def getBestForward(self, loc):
        mydist = getDist(self.loc,loc)
        try:
            bestpeer = min(self.nearPeers+self.farPeers, key= lambda x: getDist(x.loc,loc))
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
            peerpool = sorted(peerpool,key= lambda x: getDist(x.loc,self.loc))
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
        self.peerpool.add(othernode)#add our know node for shits and giggles
        parentstr = patron.find(loc2String(self.loc))
        parent = Peer(parentstr)##find my parent
        self.peerpool.add(parentstr)##add my parent
        self.nearPeers, self.farPeers = self.evaluatePeers(list(self.peerpool)) #update my peerlist
        parent.notify([self.name]+map(str,self.nearPeers))#send a notify to my parent
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
            print "best", best
            return best.find(locstr)
        #recursively lookup the node nearest to this loc
        

    #public
    def notify(self,peerlist):#inputs a list of 
        for p in peerlist:
            self.peerpool.add(p)
        #tell me you and your friends exist
        return [self.name]+map(str,self.nearPeers)

    #public
    def put(key,val):
        #store a value, at a key
        pass

    #public
    def get(key):
        #retrive a value, at a key
        pass
    
    def mainloop(self):
        while(self.running):
            time.sleep(0.5)
            self.nearPeers, self.farPeers = self.evaluatePeers(list(self.peerpool)) #update my peerlist
            for p in self.nearPeers:
                p.notify([self.name]+map(str,self.nearPeers))
                time.sleep(0.5)
        pass


