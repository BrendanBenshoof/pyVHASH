from instrumentation import InstrumentationNode
from instrumentation import ExperimentNode as Node
import time, random
from threading import Thread


port = 9100
iNode =  InstrumentationNode("127.0.0.1",port)
n1 = Node("127.0.0.1",port+1,iNode.name)
n2 = Node("127.0.0.1",port+2,iNode.name)

n1.create()


