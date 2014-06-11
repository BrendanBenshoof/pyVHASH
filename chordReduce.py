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
    Mapreduce fault tolerence DETECTION STARTS

    """



    """
    Mapreduce fault tolerence DETECTION ENDS 

    """











    """ 
    Mapreduce fault tolerence ACTIONS starts here 
    Detecting what's gone wrong isn't the problem
    When things Go Wrong, this is how we ensure that things Go Right.
    I've split up the functions up into three categories:

    Results - The head honcho node collecting all the ReduceAtoms
    Maps -  Making sure the Map tasks get done
    Reduce - Making sure the Reduce tasks make it to the final destination

    I've further divided each part into two subparts:
    Normal Actions - How things SHOULD go, but won't because Murphy has a very sadistic sense of humor
    Churn Actions - How things are handled when Murphy decides to simulate an exploding computer or a new victim joining.  Consider predecessor dying, new node joining as pred, new succ.

    An aside to the reader, all faults are considered equal.  It's a primitive solution, but I am just one man.
    """


    ### Maps
    ## Normal Actions 
    # Send backup of map task to successor list 
    #$Receive backup map, assume value is already there
    def createBackupMap(self,key, outputAddress):
        print self.name, "created backup map job of ", key
        self.backupMaps.append(MapAtom(key, outputAddress))
        #print "\n\n\n\n\n\n\n\n",key in self.backups.keys()
        return key in self.backups.keys()


    # When I finish doing a map task, I need to notify sucessors of completion of map task by sending a backup of the reduce atom I just created.  This leads into the section below
    #$Receive map task done






    ## Churn Actions
    # My predecessor has died, but despair not, for I, the successor will take up his mantle and accomplish his task.  Or my successors will! 1) Add mapatom to the queue 2) Make sure all my successors are backups now using an above function

    # Hand over work to newly joined predecessor.  He should receive the task and back it up.  I need to make sure I know I'm not responsible for it any more.
    

    #$Receive the work that belongs to me
    def takeoverMap(self, atom):
        print self.name, " was told to take  over map", atom['hashid'] 
        self.mapQueue.append(MapAtom(atom['hashid'], atom['outputAddress']))
        return True

    # Handover all map backup to new successor 
    #$I'm a new successor getting all the map backups

    ### Reduce
    ## Normal Actions
    # Backup the reduce atom I just created.  Also let them know that if I die, I want them to avenge me.  Or at the very least, ensure the reduce atom makes its way to the output address
    #$Receive backup of reduce atom
    def createBackupReduce(self, key, reduceDict):
        self.backupReduces[key] = self.dictToReduce(reduceDict)
        return True


    # receive the ack from the output address and notify successors that my reduce atom in question successfully made it's way to the results 
    #$backups get the results ack


    ## Churn Actions
    # My predecessor died and he didn't tell me the reduce was successfully sent, so I'm going to resend it.  This may result in too many copies of one result, but that's okay in the long scheme of things

    # A new predecessor joins and he should hold onto the reduce and.... this is actually a toughy.  Let's go with I tell him he's responsible for it and since he'll receive the ack, he'll have to tell everyone about it.   I also give him the reduce atom
    #$ Now I get the reduce atom
    def takeoverReduce(self,key,reduceDict):  # no need to send it along, the guy handing it over should do that.
        atom = self.dictToReduce(reduceDict) 
        self.myReduceAtoms[key] = atom
        return True

    # 
    #$ and the corresponding action


    ### Results

    ## Normal Actions 
    # send the results I just received to backups.
    def backupNewResults(self,atom):
        fails = []
        for s in self.successorList:
            try:
                Peer(s).createBackupResults(atom)
            except Exception, e:
                print self.name, "failed backing up results to", s
                traceback.print_exc(file=sys.stdout)
                fails.append(s)
        if (len(fails) >= 1):
            for f in fails:
                self.fixSuccessorList(f)


    #$recieving the backup of the results.
    #public
    def createBackupResults(self, resultsDict):
        if self.backupResults is None:
            self.backupResults = self.dictToReduce(resultsDict)
        else:
            self.backupResults.results = self.mergeKeyResults(resultsDict['results'], self.backupResults.results)
            self.backupResults.keysInResults = self.mergeKeyResults(resultsDict['keysInResults'], self.backupResults.keysInResults)
        return True
    
    # Give the new successor an infodump of my results
    #$Backup the infodump of results to backup


    # inform the owners of these keys that the reduceAtoms corresponding to the hashkeys were received.  The other half of this is in reduce actions
    


    ## Churn Actions
    # My predecessor has died.  He had the results.  Bugger.  This means I need to be responsible for the results.
    # I'm holding the results and decided that my predecessor is a better candidate to hold this ticking time bomb we call "results" than me.
        def relinquishResults(self):
        self.resultsHolder =  False
        print self.name, "Waiting for my resultsThread to finish"
        self.resultsThread.join()
        print self.name, "ResultsThread is done"
        try:
            Peer(pred.name).takeoverResults(results)
            self.backupResults =  self.results
            self.results = None
        except Exception, e:
            print self.name, "I'm still the results owner"
            self.results = self.backupResults
            self.resultsHolder = True
            self.resultsThread = Thread(target =  self.areWeThereYetLoop)
            self.resultsThread.daemon = True
            self.resultsThread.start()  #begin waiting for stuff to come back


    #$I'm taking over the results for my successor.
    def takeoverResults(self,resultsDict):
        print self.name, "taking over results"
        self.results = self.dictToReduce(resultsDict)   
        self.resultsHolder = True
        self.resultsThread = Thread(target =  self.areWeThereYetLoop)
        self.resultsThread.daemon = True
        self.resultsThread.start()  #begin waiting for stuff to come back
        return True


    # Potential: due to churn, I somehow am assumed to be respobsible for this result (and results in general).  I am totally not that node.   Solution:  generate a fault!

    """
    End Mapreduce Fault tolerence actions 
    """ 







    """
    Begin debug actions. 

    These actions let me see the progress of the overall program
    """
    def areWeThereYetLoop(self):
        while self.resultsHolder:
            missingKeys  = self.getMissingKeys()
            if len(missingKeys) > 0:
                print self.name, "MAPPED", MAPPED
                print self.name, "Waiting on ", missingKeys
            else:
                print self.name, "\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\nDone"
                print self.results.results
                print self.results.keysInResults
                break
            time.sleep(MAINT_INT*50)

    def getMissingKeys(self):
        missingKeys = []
        try:
            for key in self.results.keysInResults.keys()[:]:
                if self.results.keysInResults[key]  ==  0:
                    missingKeys.append(key)
        except Exception as e:
            print self.name, "how the bollocks did this happen?", e
        return missingKeys



    """
    End debug actions. 
    """ 
