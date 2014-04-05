from pyVhash import Node, Name2locString, Peer

class DHTnode(Node):
    def __init__(self,host,ip):
        Node.__init__(self,host,ip)
        self.data = {}
        self.addNewFunc(self.put,"put")
        self.addNewFunc(self.get,"get")

    def store(self,key,val):
        loc = Name2locString(key)
        target = self.find(loc)
        Peer(target).put(key,val)

    def retrive(self,key):
        loc = Name2locString(key)
        target = self.find(loc)
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