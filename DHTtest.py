from ChordDHT import DHTnode as Node
import time, random

port = 9100


n1 = Node("98.251.48.221",port+1)
n2 = Node("98.251.48.221",port+2)

n1.create()
n2.join("http://98.251.48.221:9101")
time.sleep(1)

nodes = [n1,n2]
for i in range(3,10):
    time.sleep(1.0)
    n = Node("98.251.48.221",port+i)
    print "started", n
    n.join(random.choice(nodes).name)
    nodes.append(n)


"""
print "prepare to sleep"

time.sleep(3)
for i in range(0,100):
    n = random.choice(nodes)
    n.store(str(i)+"blah",str(i))



for i in range(0,100):
    n = random.choice(nodes)
    print n.retrive(str(i)+"blah")

info = []    
for n in nodes:
    info.append(n.myInfo())

print sorted(info, key= lambda x: x[1])
"""