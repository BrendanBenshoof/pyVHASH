from pyChord import Node, getHashString, Peer

class DHTnode(Node):
    def __init__(self,host,ip):
        Node.__init__(self,host,ip)
        self.data = {}
        self.addNewFunc(self.put,"put")
        self.addNewFunc(self.get,"get")

    def store(self,key,val):
        loc = getHashString(key)
        target = self.findSuccessor(loc)
        Peer(target).put(key,val)

    def retrive(self,key):
        loc = getHashString(key)
        target = self.findSuccessor(loc)
        return Peer(target).get(key), target

    #public
    def put(self,key,val):
        self.data[key]=val
        return True

    def get(self,key):
        if key in self.data.keys():
            return self.data[key]
        else:
            return "FAIL"