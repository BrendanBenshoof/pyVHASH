#chordReduce on ChordDHT
from pyChord import Peer, getHashString
from ChordDHT import DHTnode
from cfs import DataAtom
from threading import Thread, Lock
import time



class MapAtom(object):
    def __init__(self,hashid,outputAddress):
        self.hashid = hashid
        self.outputAddress = outputAddress


class ReduceAtom(object):
    def __init__(self,results,keysInResults,outputAddress):
        self.results = results
        self.keysInResults = keysInResults
        self.outputAddress = outputAddress

class ChordReduceNode(DHTnode):
    def __init__(self,host,ip):
        DHTnode.__init__(self,host,ip)
        self.addNewFunc(self.stage,"stage")
        self.addNewFunc(self.distributeMapTasks,"distributeMapTasks")
        self.addNewFunc(self.handleReduceAtom, "handleReduceAtom")
        self.mapQueue = []
        self.reduceQueue = []
        self.outQueue = []
        self.mapsDone = {}
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

    def reduceFunc(self,a,b):
        #merge the dicts!
        for k in a.keys():
            if k in b.keys():
                b[k]+=a[k]
            else:
                b[k]=a[k]
        return b #overload this to describe reduce function
        
    # public
    def stage(self,filename, outputAddress):
        # retrieve the key file
        keyfile =  self.getKeyfile(filename)
        keys = keyfile['keys']
        # distribute map tasks
        self.distributeMapTasks(keys,outputAddress)
        # master reduce node
        return True
        
    
    #public
    # need to work out threading details for this
    # this is a big advantage here that should be mentioned in the paper
    # one node doesn't have to the lookup for each piece
    # that work is distributed
    """ need to handle wrong person getting the maps (IE the person thinks he has the data but he actually doesn't) """ 
    def distributeMapTasks(self, keys, outputAddress):
        buckets =  self.bucketizeKeys(keys) #using short circuiting only is a nifty idea iff we don't have any churn
        myWork = []
        if self.name in buckets.keys():
            myWork = buckets[self.name] #keep my keys
            del buckets[self.name]
            print  self.name, "got my work"
        self.mapQueue = self.mapQueue + myWork #add my keys to queue
        #FT: make backups

        #send other keys off
        threads = []
        for dest in buckets.keys():
            t =  Thread(target = self.sendMapJobs, args = (dest, buckets[dest], outputAddress,))
            t.daemon = True
        for t in threads():
            t.start()
        return True
        

    def sendMapJobs(self,node,keys,outputAddress):
        try:
            Peer(node).distributeMapTasks(keys,outputAddress)
        except Exception as e:
            print self.name, "couldn't send to", node
            if node ==  self.succ.name:
                self.fixSuccessor()
            elif node in self.successorList:
                self.fixSuccessorList(node)
            else:
                self.removeNodeFromFingers(node)
            #retry
            newTarget, done = self.find(Peer(node).hashid)
            self.sendMapJobs(self,newTarget, keys, outputAddress)


    def sendReduceJob(self, atom):
        sent  = False
        while not sent:
            target, done = self.find(outputAddress)
            try:
                Peer(target).handleReduceAtom(atom)
                sent =  True
            except:
                print self.name, "can't send reduce to ", target
                if target ==  self.succ.name:
                    self.fixSuccessor()
                elif target in self.successorList:
                    self.fixSuccessorList(target)
                else:
                    self.removeNodeFromFingers(target)

    #public 
    def handleReduceAtom(self, atomDict):
        self.reduceQueue.append(ReduceAtom(atomDict['results'],atomDict['keysInResults'], atomDict['outputAddress']))
        return True
    
    # group each key into a bucket 
    def bucketizeKeys(self,keys):
        output = {}
        for k in keylist:
            owner = None
            if k in self.data.keys():  # the real question is why doesn't it work without this.  It should now
                owner = self.name
            else:
                owner, t = self.find(k)
            print owner
            if owner in output.keys():
                output[owner].append(k)
            else:
                output[owner] = [k]
        return output
    
    
    
    # keep on doing maps
    def mapLoop(self):
        while True:
            if len(mapQueue):
                sleep(MAINT_INT)
                work  = self.mapQueue.pop() # pop off the queue
                results = self.mapFunc(work.hashid) # excute the job
                # put reduce in my queue 
                self.reduceQueue.append(ReduceAtom(results, {word.hashid : 1},  work.outputAddress))
                # FT: inform backups I am done with map 
                # FT: backup the reduce atom 


    # reduce my jobs to one
    def reduceLoop(self):
        while True:
            time.sleep(MAINT_INT*2)
            while len(self.reduceQueue) >= 2:
                atom1 = self.reduceQueue.pop()
                atom2 = self.reduceQueue.pop()
                results = self.reduceFunc(atom1.results,atom2.results)
                keysInResults =  self.mergeKeyResults(atom1.keysInResults, atom2.keysInResults)
                outputAddress = atom1.outputAddress
                self.reduceQueue.append(ReduceAtom(results,keysInResults,outputAddress))
            if len(self.reduceQueue) >= 1:
                atom = self.reduceQueue.pop() 
                self.sendReduceJob(atom)

    def mergeKeyResults(self, a, b):
        for k in a.keys():
            if k in b.keys():
                b[k]+=a[k]
            else:
                b[k]=a[k]
        return b
