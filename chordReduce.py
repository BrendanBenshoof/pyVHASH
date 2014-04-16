#chordReduce on ChordDHT
from pyChord import Peer, getHashString

from ChordDHT import DHTnode

from threading import Thread, Lock

class ChordReduceNode(DHTnode):
    def __init__(self,host,ip):
        DHTnode.__init__(self,host,ip)
        self.addNewFunc(self.doMapReduce,"doMapReduce")

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

    #private
    def startMapReduce(self,filename):
        key = getHashString(filename)
        target = self.findSuccessor(key)
        keyfile =  Peer(target).get(key) # why is this a dict?  rpc it turns out
        keys = keyfile['keys'] 
        return self.doMapReduce(keys)

    #public
    def doMapReduce(self,subKeys):
        print  self.name, "doing Mapreduce"
        buckets = self.buckitizeKeys(subKeys)
        mytodo = []
        print buckets

        if self.name in buckets.keys():
            mytodo = buckets[self.name]
            del buckets[self.name]
            print  self.name, "got my work"
        threads = []
        for othernode in buckets.keys():
            threads.append(threadedMapReduceManager(othernode, buckets[othernode]))
            print  self.name, "sending to ", othernode
        for t in threads:
            t.start()
        #start work on my stuff while I wait for my peers
        if len(mytodo) > 0:
            print  self.name, "doing map"
            results = map(self.mapFunc,mytodo)
            if len(results) > 1:
                result = reduce(self.reduceFunc, results)
            else:
                result = results[0]
            print  self.name, "reduce done"
        else:
            result = False
        for t in threads:
            t.join()
        for t in threads:
            t.lock.acquire()
            result = reduce(self.reduceFunc, [t.result, result])
            t.lock.release()
        return result


        #report accumulated results

    def buckitizeKeys(self,keylist):
        output = {}
        for k in keylist:
            owner = self.findSuccessor(k)
            print owner
            if owner in output.keys():
                output[owner].append(k)
            else:
                output[owner] = [k]
        return output


class threadedMapReduceManager(Thread):
    def __init__(self,remoteName, datapile):
        Thread.__init__(self)
        self.lock = Lock()
        self.remoteName = remoteName
        self.datapile = datapile
        self.result = None

    def run(self):
        self.lock.acquire()
        remote = Peer(self.remoteName)
        print remote
        print "start",self.name
        self.result = remote.doMapReduce(self.datapile)
        print "done",self.name
        self.lock.release()
        return True

        