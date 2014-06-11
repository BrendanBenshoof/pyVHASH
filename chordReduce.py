from pyChord import Peer, getHashString, MAINT_INT,hashBetweenRightInclusive
from ChordDHT import DHTnode
from cfs import DataAtom
from threading import Thread, Lock
import time
from Queue import Queue
import sys, traceback
import copy


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
        self.addNewFunc(self.createBackupMap,"createBackupMap")
        self.addNewFunc(self.createBackupReduce, "createBackupReduce")
        self.addNewFunc(self.createBackupResults,"createBackupResults")
        self.addNewFunc(self.deleteBackupMap,"deleteBackupMap")
        self.addNewFunc(self.deleteBackupReduce, "deleteBackupReduce")
        self.addNewFunc(self.takeoverMap,"takeoverMap")
        self.addNewFunc(self.takeoverReduce,"takeoverReduce")
        self.addNewFunc(self.takeoverResults,"takeoverResults")
        self.mapQueue = []
        self.mapLock = Lock()
        self.myReduceAtoms = {}
        self.reduceQueue = Queue()  # turn this into an actual Queue and we can handle FT on the way back
        self.outQueue = []
        self.backupMaps = []  # MapAtoms
        self.backupReduces = {}
        self.mapThread = None
        self.reduceThread = None
        self.resultsThread =  None
        self.resultsHolder  = False
        self.results  = None  # make a reduce atom
        self.backupResults = None




""" 
Mapreduce fault tolerence ACTIONS starts here 
Detecting what's gone wrong isn't the problem
When things Go Wrong, this is how we ensure that things Go Right.
I've split up the functions up into three categories:

Results - The head honcho node collecting all the ReduceAtoms
Maps -  Making sure the Map tasks get done
Reduce - Making sure the Reduce tasks make it to the final destination

I've further divided each part into two subparts:
Normal Operations - How things SHOULD go, but won't because Murphy has a very sadistic sense of humor
Churn Operations - How things are handled when Murphy decides to simulate an exploding computer or a new victim joining.

An aside to the reader, all faults are considered equal.  It's a primitive solution, but I am just one man.
"""
### Results

## Normal Operations 

# backing up the results I just received.
def backupNewResult():
    pass

# inform the owners of these keys that the reduceAtoms corresponding to the hashkeys were received


## Churn operations
# My predecessor has died.  He had the results.  Bugger.

# I'm holding the results and decided that my predecessor is a better candidate to hold this ticking time bomb we call "results" than me.



### Maps
## Normal Operations 
#Send backup of map task to successor list 

#Notify sucessors of completion of map task




### Reduce

## Normal Actions
# Backup the reduce atom I just created.  Also let them know that if I die, I want them to avenge me.  Or at the very least, ensure the reduce atom makes its way to the 
# Notify successors that the reduce atom successfully made it's way to the results 
"""
End Mapreduce Fault tolerence actions 
""" 
