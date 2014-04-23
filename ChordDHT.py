from pyChord import Node, getHashString, Peer
from cfs import DataAtom, KeyFile, makeBlocks
class DHTnode(Node):
    def __init__(self,host,ip):
        Node.__init__(self,host,ip)
        self.data = {} # data I'm responsible for 
        self.backups = {} # data I'm holding onto for someone else
        self.addNewFunc(self.put,"put")
        self.addNewFunc(self.get,"get")
        self.addNewFunc(self.store,"store")
        self.addNewFunc(self.store,"storeFile")
        self.addNewFunc(self.retrieve,"retrieve")
        self.addNewFunc(self.retrieveFile,"retrieveFile")
        self.addNewFunc(self.backup,"backup")
        self.addNewFunc(self.myInfo,"myInfo")
        

    def myInfo(self):
        return self.name + "\n"+ str(self.data) +  "\n" + str(self.backups) + "\n\n"
        #print self.data

    def findSuccessor(self, key, dataRequest = False):
        if dataRequest and (key in self.data.keys() or key in self.backups.keys()):
            print self.name, "short circuited the request"
            return self.name
        else:
            return super(DHTnode,self).findSuccessor(key,dataRequest)

    def find(self, key, dataRequest):
        if dataRequest and (key in self.data.keys() or key in self.backups.keys()):
                print self.name, "short circuited the request"
                return self.name, True
        else:
            return super(DHTnode,self).find(key,dataRequest)

    def updateSuccessorList(self):
        oldList  = self.successorList
        super(DHTnode,self).updateSuccessorList()
        newSuccessors = [node for node in self.successorList if node not in oldList]
        for node in newSuccessors:
            self.backupToNewSuccessor(node)


    def store(self,name,val):
        key = getHashString(name)
        target = self.findSuccessor(key)  # if fails do wut?  I don't think it will
        try:
            Peer(target).put(key,val)
        except Exception as e:
            print self.name, e, "the node I tried to store in literally just died"
        return True

    def retrieve(self,name):
        key = getHashString(name)
        target = self.findSuccessor(key,True) # if fails do wut?
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
        return True

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
                newSucc.backup(k,v)
            except Exception: # and.... it's gone
                self.fixSuccessorList(newSuccessor)

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
        elif key in self.backups.keys():
            return self.backups[key]
        else: 
            return "FAIL"
            

    # public
    def backup(self,key,val):
        self.backups[key]=val
        return True
