from pyChord import Node, getHashString, Peer
from cfs import DataAtom, KeyFile, makeBlocks
class DHTnode(Node):
    def __init__(self,host,ip):
        Node.__init__(self,host,ip)
        self.data = {}
        self.addNewFunc(self.put,"put")
        self.addNewFunc(self.get,"get")

    def myInfo(self):
        me = [self.name.split(":")[2], str(self.hashid)[0:4]]
        su = [self.succ.name.split(":")[2], str(self.succ.hashid)[0:4]]
        pr = [self.pred.name.split(":")[2], str(self.pred.hashid)[0:4]]
        return [pr[1], me[1], su[1]]
        #print self.data


    def store(self,key,val):
        loc = getHashString(key)
        target = self.findSuccessor(loc)
        Peer(target).put(key,val)

    def retrive(self,key):
        loc = getHashString(key)
        target = self.findSuccessor(loc)
        return Peer(target).get(key), target

    def storeFile(self, filename):
        keyfile, blocks =  makeBlocks(filename)
        target = self.findSuccessor(keyfile.hashid)
        #print "storing", keyfile, "at", keyfile.hashid
        Peer(target).put(keyfile.hashid, keyfile)  # ???
        for block in blocks:
            print "block!"
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
            block = Peer(target).get(key)
            blocks.append(block)
        return blocks


    #public
    def put(self,key,val):
        #print "stored", val, "at", key
        self.data[key]=val
        return True

    def get(self,key):
        if key in self.data.keys():
            return self.data[key]
        else:
            return "FAIL"