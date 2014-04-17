from pyChord import Node, getHashString, Peer
from cfs import DataAtom, KeyFile, makeBlocks
class DHTnode(Node):
    def __init__(self,host,ip):
        Node.__init__(self,host,ip)
        self.data = {} # data I'm responsible for 
        self.backups = {} # data I'm holding onto for someone else
        self.addNewFunc(self.put,"put")
        self.addNewFunc(self.get,"get")
        self.addNewFunc(self.backup,"backup")
        

    def myInfo(self):
        me = [self.name.split(":")[2], str(self.hashid)[0:4]]
        su = [self.succ.name.split(":")[2], str(self.succ.hashid)[0:4]]
        pr = [self.pred.name.split(":")[2], str(self.pred.hashid)[0:4]]
        return [pr[1], me[1], su[1]]
        #print self.data


    def store(self,key,val):
        loc = getHashString(key)
        target = self.findSuccessor(loc)  # if fails do wut?
        Peer(target).put(key,val)

    def retrive(self,key):
        loc = getHashString(key)
        target = self.findSuccessor(loc) # if fails do wut?
        return Peer(target).get(key), target

    def storeFile(self, filename):
        keyfile, blocks =  makeBlocks(filename)
        target = self.findSuccessor(keyfile.hashid)
        #print "storing", keyfile, "at", keyfile.hashid
        Peer(target).put(keyfile.hashid, keyfile)  # ???
        for block in blocks:
            target = self.findSuccessor(block.hashid)
            Peer(target).put(block.hashid, block)
            #print "stored", block, "at", block.hashid



    def retrieveFile(self, filename):
        key = getHashString(filename)
        target = self.findSuccessor(key)
        keyfile =  Peer(target).get(key) # why is this a dict?  rpc it turns out
        keys = keyfile['keys']
        blocks = []
        for key in keys:
            target = self.findSuccessor(key)
            #print key[:4], hex(Peer(target).hashid)[:6]
            block = Peer(target).get(key)
            blocks.append(block)
        return blocks


    def backupToNewSuccessor(self, newSuccessor):
        newSucc = Peer(newSuccessor)
        for k, v in self.data.iteritems():
            try:
                newSucc.backupThis(k,v)
            except Exception: # and.... it's gone
                self.fixSuccessorList()


    def backupNewData(self, key, val):
        fails = []
        for s in self.successorList:
            try:
                Peer(s).backup(key,val)
            except Exception, e:
                fails.append(s)
                continue
        if (len(fails) >= 1):
            for f in fails:
                self.fixSuccessorList(f)


    def relinquishData(self,key,val):
        try:
            self.pred.put(key,val)
        except Exception:
            self.pred = None  #or fix by searching for his hash -1
        else:
            del data[key]



    #public
    def put(self,key,val):
        #print "stored", val, "at", key
        self.data[key]=val
        # can I create new threads to do this?
        self.backupNewData(key,val)
        return True

    def get(self,key):
        if key in self.data.keys():
            return self.data[key]
        else:
            return "FAIL"
            

    # public
    def backup(self,key,val):
        self.backups[key]=val
        return True
