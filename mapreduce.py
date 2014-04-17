#chordReduce on ChordDHT
from pyChord import Peer, getHashString

from ChordDHT import DHTnode

from threading import Thread, Lock

class ChordReduceNode(DHTnode):
    def __init__(self,host,ip):
        DHTnode.__init__(self,host,ip)
        self.addNewFunc(self.doMapReduce,"doMapReduce")
        self.jobQueue = []
        self.outQueue = []

    def mapFunc(self,key):
        print "mapfunc", key, self.name
        data = self.get(key) #get the chunk from local storage
        if type(data) == type(dict()):
            data = data['contents']
        words = data.split()
        output = {}
        for w in words:
            if w in output.keys():
                output[w]+=1
            else:
                output[w]=1
        return output

    def reduceFunc(self,data0,data1):
        if data0 == False:
            return data1
        elif data1 == False:
            return data0
        else:
            #merge the dicts!
            for k in data0.keys():
                if k in data1.keys():
                    data1[k]+=data0[k]
                else:
                    data1[k]=data0[k]
        return data1 #overload this to describe reduce function