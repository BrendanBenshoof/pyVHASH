from pyChord import Node
import time

port = 9500

n1 = Node("127.0.0.1",port+1)
n2 = Node("127.0.0.1",port+2)

n1.create()
n2.join(n1.name)
time.sleep(1)

nodes = [n1,n2]
for i in range(3,10):
    time.sleep(0.5)
    n = Node("127.0.0.1",port+i)
    print "started", n
    nodes.append(n)
    n.join(n1.name)
