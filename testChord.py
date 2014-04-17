from pyChord import Node
import time
import random
from threading import Thread
from hashlib import sha1

def randomLookups(nodes):
    for i in range(1000):
        target = sha1(str(random.randint(0,1000000))).hexdigest()
        print i
        n = random.choice(nodes)
        n.findSuccessor(target)
    print "done"


port = 9500

n1 = Node("127.0.0.1",port+1)
n2 = Node("127.0.0.1",port+2)

n1.create()
n2.join(n1.name)
time.sleep(1)

nodes = [n1,n2]
for i in range(3,5):
    time.sleep(0.5)
    n = Node("127.0.0.1",port+i)
    print "started", n
    nodes.append(n)
    n.join(n1.name)


t1 = Thread(target = randomLookups, args = [nodes])
t2 = Thread(target = randomLookups, args = [nodes])
t3 = Thread(target = randomLookups, args = [nodes])
t4 = Thread(target = randomLookups, args = [nodes])
t5 = Thread(target = randomLookups, args = [nodes])
t1.start()
t2.start()
t3.start()
t4.start()
t5.start()
