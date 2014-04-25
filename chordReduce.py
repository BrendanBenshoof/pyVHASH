#chordReduce on ChordDHT
from pyChord import Peer, getHashString
from ChordDHT import DHTnode
from cfs import DataAtom
from threading import Thread, Lock
import time



class MapAtom(object):
    jobid = None
    hashid = None
    stuffToMap = []
    resultsAddress = 0


class ReduceAtom(object):
    jobid = None
    results = []
    keysInResults = []
    resultsAddress = 0

class ChordReduceNode(DHTnode):
    def __init__(self,host,ip):
        DHTnode.__init__(self,host,ip)
        self.addNewFunc(self.doMapReduce,"doMapReduce")
        self.mapQueue = []
        self.reduceQueue = []
        self.outQueue = []
        self.backupMaps = {}  #
        self.backupReduces = []
        
        

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
        
    
    
    
    # public
    def stage(self,filename):
        # retrieve the key file
        keyfile =  self.getKeyfile(filename)
        keys = keyfile['keys']
        self.distributeMapTasks(keys)
        # distribute map tasks
        # master reduce node
        return True
        
    
    #public
    # need to work out threading details for this
    # this is a big advantage here that should be mentioned in the paper
    # one node doesn't have to the lookup for each piece
    # that work is distributed
    def distributeMapTasks(self, keys):
        pass
        buckets =  self.bucketizeKeys(keys)
        #using short circuiting only is a nifty idea iff we don't have any churn 
        
    
    # group each key into a bucket 
    def bucketizeKeys(self,keys):
        pass
    
    
    # public
    def handleMapTask(self,key):
        pass
        # put it in the map Queue
        # back it up
        # return True
    
    
    # keep on doing maps
    def doMapLoop(self):
        while True:
            pass
            # pop off the queue
            # exceute the job
            # send reduce out
            # inform backups I am done with map
            # backup the reduce atom 


    # reduce my jobs to one
    def reduceLoop(self):
        while True:
            sleep(MAINT_INT*2)
            while len(self.reduceQueue) >= 2:
                atom1 = self.reduceQueue.pop()
                atom2 = self.reduceQueue.pop()
                self.reduceQueue.append(self.reduceFunc(atom1,atom2))
            if len(self.reduceQueue):
                pass
                #dataAtom= self.reduceQueue.pop() 
                #while not sent
                #   find best the one hop neighbor back to the successor
                #   send the message there
