from chordReduce import ChordReduceNode as Node
import time, random
from threading import Thread

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
    for i in range(3,6):
        n = Node("127.0.0.1",port+i)
        print "yay"+str(i)
        print "started", n
        n.join(random.choice(nodes).name)
        nodes.append(n)
        time.sleep(1.0)

    keys = map(lambda x: hex(x.hashid)[:6], nodes)
    print keys

    print "prepare to sleep"
    time.sleep(3)
    n1.storeFile("ti.txt")
    #blocks = n.retrieveFile("shakespeare.txt")
    #for block in blocks:
    #    print block
    print n1.startMapReduce("ti.txt")
