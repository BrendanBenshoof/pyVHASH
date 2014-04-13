from ChordDHT import DHTnode as Node
import time, random
from threading import Thread
import multiprocessing

port = 9100

if __name__ == '__main__':


    n1 = Node("127.0.0.1",port+1)
    n2 = Node("127.0.0.1",port+2)

    time.sleep(1)

    n1.create()
    print "yay1"
    n2.join(n1.name)
    print "yay2"

    time.sleep(1)

    nodes = [n1,n2]
    for i in range(3,5):
        n = Node("127.0.0.1",port+i)
        print "yay"+str(i)
        print "started", n
        n.join(random.choice(nodes).name)
        nodes.append(n)
        time.sleep(1.0)

    print "prepare to sleep"
    time.sleep(3)
    n = random.choice(nodes)
    n.storeFile("constitution.txt")
    n = random.choice(nodes)

    blocks = n.retrieveFile("constitution.txt")
    for block in blocks:
        print block

    for n in nodes:
        print n.successorList

    """

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
