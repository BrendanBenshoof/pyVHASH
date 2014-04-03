from VhashTheStampede import MapReduceNode
import time

port = 9500

n1 = MapReduceNode("127.0.0.1",port+1)
n2 = MapReduceNode("127.0.0.1",port+2)

n1.join(n2.name)
n2.join(n1.name)
time.sleep(1)

nodes = [n1,n2]
for i in range(3,6):
    time.sleep(0.5)
    n = MapReduceNode("127.0.0.1",port+i)
    print "started", n
    nodes.append(n)
    n.join(n1.name)

print "prepare to sleep"
time.sleep(5)

n1.setup("constitution.txt")
time.sleep(10)
print n1.results()