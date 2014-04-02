from pyVhash import Node, Name2locString, Peer
import time

class DHTNode(Node):
    def __init__(self,ip,port):
        Node.__init__(self,ip,port)
        self.data = {}
        self.addnewFunc(self.put,"put")
        self.addnewFunc(self.get,"get")

    def store(self,key,val):
        target = self.find(Name2locString(key))
        Peer(target).put(key,val)

    def retrive(self,key):
        target = self.find(Name2locString(key))
        return Peer(target).get(key)

    #public
    def put(self,key,val):
        self.data[key] = val
        print self.name, key, val
        return True
    #public
    def get(self,key):
        if key in self.data.keys():
            return self.data[key]
        else:
            return "FAIL"

n1 = DHTNode("127.0.0.1",9005)
n2 = DHTNode("127.0.0.1",9006)

n1.join(n2.name)
n2.join(n1.name)
time.sleep(1)

n1.store("test","blah")
print n2.retrive("test")