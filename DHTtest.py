from VhashDHT import DHTnode as Node
import time, random

port = 9500


n1 = Node("127.0.0.1",port+1)
n2 = Node("127.0.0.1",port+2)

n1.join(n2.name)
n2.join(n1.name)
time.sleep(1)

nodes = [n1,n2]
for i in range(3,20):
    n = Node("127.0.0.1",port+i)
    print "started", n
    n.join(random.choice(nodes).name)
    nodes.append(n)


"""
for n in nodes:
    print n.loc, n.nearPeers, len(n.farPeers)

"""
print "prepare to sleep"
time.sleep(5)
for i in range(0,1000):
    n = random.choice(nodes)
    n.store(str(i)+"blah",str(i))

for i in range(0,1000):
    n = random.choice(nodes)
    print n.retrive(str(i)+"blah")


