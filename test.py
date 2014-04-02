from pyVhash import Node, Peer, Name2locString
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
        return True
    #public
    def get(self,key):
        if key in self.data.keys():
            return self.data[key]
        else:
            return "FAIL"

n1 = DHTNode("127.0.0.1",9010)
n2 = DHTNode("127.0.0.1",9011)

n1.join(n2.name)
n2.join(n1.name)
port = 9012
nodes = [n1,n2]
for i in range(0,20):
    time.sleep(1)
    n = DHTNode("127.0.0.1",port+i)
    nodes.append(n)
    n.join(n1.name)

time.sleep(5)

for i in range(0,100):
    n1.store(str(i),str(i)+"blah")
for i in range(0,100):
    print nodes[-1].retrive(str(i))