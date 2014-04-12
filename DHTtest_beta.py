from ChordDHT import DHTnode as Node
import time, random

port = 9200


n1 = Node("127.0.0.1",port+1)
n2 = Node("127.0.0.1",port+2)

n1.join("http://127.0.0.1:9101")
print "yay1"

time.sleep(3)

n2.join("http://127.0.0.1:9101")
print "yay2"
"""
time.sleep(1)

nodes = [n1,n2]
for i in range(3,10):
    time.sleep(1.0)
    n = Node("127.0.0.1",port+i)
    print "yay"+str(i)
    print "started", n
    n.join(random.choice(nodes).name)
    nodes.append(n)



print "prepare to sleep"
time.sleep(3)
n = random.choice(nodes)
n.storeFile("constitution.txt")
n = random.choice(nodes)

blocks = n.retrieveFile("constitution.txt")
for block in blocks:
    print block




for i in range(0,100):
    print "progress"
    n = random.choice(nodes)
    n.store(str(i)+"blah",str(i))



for i in range(0,100):
    n = random.choice(nodes)
    print n.retrive(str(i)+"blah")


info = []    
for n in nodes:
    info.append(n.myInfo())

print sorted(info, key= lambda x: x[1])"""
