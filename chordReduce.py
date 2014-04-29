#chordReduce on ChordDHT
from pyChord import Peer, getHashString, MAINT_INT,hashBetweenRightInclusive
from ChordDHT import DHTnode
from cfs import DataAtom
from threading import Thread, Lock
import time
from Queue import Queue
import sys, traceback


"""
3 pieces are needed in order to achieve fault tolerance.

1)
    The final results must be backed up as they are added to the results dict.
    In addition, the backups need to take over if they discover they are the new results man
    If a better results man comes in, he needs to be the results man.

2)
    When A sends his atoms to B, then B suddenly fails, A will know to find another one hop person to pass to 
    If node N fails with map jobs in the Queue,  N's successor will take over.
    Mapbackups will assume everything is peachy, unless their predecessor list changes.  If it does they will either
        No longer have to back the data up
        Need to take over
    When N finishes a map job, he should inform the backups to destroyMapBackup()
    Finally, if N's predecessor changes, N passes all data, reduceatoms and mapatoms that are that guy's 
    In that case, N should inform the backups to destroyMapBackup() of the respective keys 

3)
    We don't want to have to manually ask for reduce tasks to finish getting one of each ReduceAtom
    When passing a ReduceAtom back, if the next hop fails before sending to his next hop, I need to resend.
    If the ReduceAtom's Owner fails, the successor 
    (it is acceptable if somehow we get too many instances of a reduce atom; we can unreduce)
    The ReduceAtom can be stored like other stuff, but the intermediate results must be handled by the senders


4) In addition:
    We need to figure out if a Peer returns, but his caller is no longer there, what exception occurs, if any, and where
    And most importantly, what ends up catching it.



"""


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
        self.backupReduces = []
        self.mapThread = None
        self.reduceThread = None
        self.resultsThread =  None
        self.resultsHolder  = False
        self.results  = None  # make a reduce atom
        self.backupResults = None
        
        
    def shouldKeepBackup(self,key):
        if self.pred is None or self.pred.name == self.name:
            return True
        return hashBetweenRightInclusive(long(key,16), Peer(self.predecessorList[0]).hashid, self.hashid)

    # overrides
    
   
    
    def kickstart(self):
        super(ChordReduceNode, self).kickstart()
        self.mapThread = Thread(target = self.mapLoop) 
        self.mapThread.daemon = True
        self.reduceThread = Thread(target = self.reduceLoop)
        self.reduceThread.daemon = True
        self.mapThread.start()
        self.reduceThread.start()

    def notify(self,poker):
        hasNewPred = super(ChordReduceNode, self).notify(poker)
        if hasNewPred:  # then he was better than my previous guy
            if self.resultsHolder:
                if not keyIsMine(self.results.outputAddress):
                    self.relinquishResults()
        return hasNewPred

    ## we purge our cache of backups by 
    # 1) throwing away backups we're no longer responsible for
    # 2) taking over backups we are now responsible for 
    def purgeBackups(self):
        super(ChordReduceNode,self).purgeBackups()
        self.purgeBackupResults()
        self.mapLock.acquire()
        for atom in self.backupMaps[:]:
            if self.keyIsMine(atom.hashid):
                self.mapQueue.append(MapAtom(atom['hashid'], atom['outputAddress']))
                self.deleteBackupMap(atom.hashid)
            elif not self.shouldKeepBackup(atom.hashid):
                self.deleteBackupMap(atom.hashid)
        for atom in self.backupReduces[:]:
            if keyIsMine(atom.hashid):
                self.myReduceAtoms[key] = atom
                self.deleteBackupReduce(atom.hashid)
                #should I selnd it off? no assume it got sent already
            elif not self.shouldKeepBackup(atom.hashid):
                self.deleteBackupReduce(atom.hashid)
        self.mapLock.release()
        
        
    ## TODO create additional backups as we acquire new successors.
    def backupToNewSuccessor(self, newSuccessor):
        try:
            for k, v in self.data.items():
                Peer(newSuccessor).backup(k,v)
        except Exception as e: # and.... it's gone
            print self.name, "failed backing up stuff to", newSuccessor 
            traceback.print_exc(file=sys.stdout)
            self.fixSuccessorList(newSuccessor)

    def relinquishData(self,key):
        val = None
        mapAtom = None
        reduceAtom = None
        print self.name, "acquired reliquish lock" 
        self.mapLock.acquire()
        try:
            val = self.data[key]
            for x in self.mapQueue:  #possible logic error
                    if x.hashid ==  key:
                        atom = x
                        break
            if key in self.myReduceAtoms.keys():
                reduceAtom = self.myReduceAtoms[key]
        except:
            print self.name, "Well that was weird", key, self.data
        else:
            try:
                Peer(self.pred.name).put(key,val)
                if mapAtom is not None:
                    Peer(self.pred.name).takeoverMap(mapAtom) 
                if reduceAtom is not None:
                    Peer(self.pred.name).takeoverReduce(key,reduceAtom)
            except Exception:
                self.pred = None  #or fix by searching for his hash -1
                raise Exception("he died on me")
            else:
                self.backups[key] = val
                del self.data[key]
                if mapAtom is not None:
                    self.mapQueue.remove(mapAtom)
                if reduceAtom is not None:
                   del self.reduceAtoms[key]
        finally:
            self.mapLock.release()
            print self.name, "released reliquish lock" 
            

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
            self.resultsHolder = True
            self.resultsThread = Thread(target =  self.areWeThereYetLoop)
            self.resultsThread.daemon = True
            self.resultsThread.start()  #begin waiting for stuff to come back


    #public
    def takeoverResults(self,resultsDict):
        self.results = self.dictToReduce(resultsDict)   
        self.resultsHolder = True
        self.resultsThread = Thread(target =  self.areWeThereYetLoop)
        self.resultsThread.daemon = True
        self.resultsThread.start()  #begin waiting for stuff to come back
        return True

    #public
    def takeoverMap(self, atom):
        self.mapQueue.append(MapAtom(atom['hashid'], atom['outputAddress']))
        return True
    
    #public
    def takeoverReduce(self,key,reduceDict):  # no need to send it along, the guy handing it over should do that.
        atom = self.dictToReduce(reduceDict) 
        self.myReduceAtoms[key] = atom
        return True


    def purgeBackupResults(self):
        if not self.resultsHolder and self.backupResults is not None: 
            if self.keyIsMine(self.backupResults.outputAddress):
                try:
                    Peer(self.name).takeoverResults(self.backupResults)
                    self.backupResults = None
                except:
                    self.name, "I failed to talk to ... myself? I couldn't take over the results"
            elif not self.shouldKeepBackup(self.backupResults.outputAddress):
                self.backupResults = None



    def addToResults(self,atom):
        self.results.results =  self.mergeKeyResults(atom.results, self.results.results)
        self.results.keysInResults =  self.mergeKeyResults(atom.keysInResults, self.results.keysInResults)
        self.backupNewResults(atom) #FT backup stuff added to results

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
                
    
    
    # public 
    # assume value is already there
    def createBackupMap(self,key, outputAddress):
        self.backupMaps.append(MapAtom(key, outputAddress))
        #print "\n\n\n\n\n\n\n\n",key in self.backups.keys()
        return key in self.backups.keys()

    #public
    def createBackupReduce(self, reduceDict):
        self.backupReduces.append(self.dictToReduce(reduceDict))
        return True

    #public
    def createBackupResults(self, resultsDict):
        if self.backupResults is None:
            self.backupResults = self.dictToReduce(resultsDict)
        else:
            self.backupResults.results = self.mergeKeyResults(resultsDict['results'], self.backupResults.results)
            self.backupResults.keysInResults = self.mergeKeyResults(resultsDict['keysInResults'], self.backupResults.keysInResults)
        return True
        
    #public
    def deleteBackupMap(self, key):
        for atom in self.backupMaps[:]:
            if atom.hashid == key:
                del self.backupMaps[key]
                return True
        return False
        
    #public
    def deleteBackupReduce(self,key):
        for atom in self.backupReduces[:]:
            if atom.hashid == key:
                del self.backupReduces[key]
                return True
        return False



    def mapFunc(self,key):
        print "mapfunc", key, self.name
        data = self.get(key) #get the chunk from local storage
        if type(data) == type(dict()):
            data = data['contents']
        text = data.split()
        output = {}
        for word in text:
            word = word.lower()
            word = word.strip(" !?.,;:\"\'*[]()/<>-*~%")
            if word is u"" or word is u'':
                continue
            if word in output.keys():
                output[word]+=1
            else:
                output[word]=1
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
    # We get to assume the node calling this remains alive until it's done
    # output address must be hex 
    def stage(self,filename, outputAddress):
        
        keyfile =  self.getKeyfile(filename)
        keys = keyfile['keys'] # retrieve the key file

        self.results =  ReduceAtom({}, {}, outputAddress) # create results
        for key in keys:
            self.results.keysInResults[key] =  0
        self.backupNewResults(self.results) #FT and back them up

        self.resultsHolder = True
        self.resultsThread = Thread(target =  self.areWeThereYetLoop)
        self.resultsThread.daemon = True
        self.resultsThread.start()  #begin waiting for stuff to come back
        # distribute map tasks
        # This may have to become a thread
        self.distributeMapTasks(keys,outputAddress)
        return True





    #public
    # need to work out threading details for this
    # this is a big advantage here that should be mentioned in the paper
    # one node doesn't have to the lookup for each piece
    # that work is distributed
    """FT need to handle wrong person getting the maps (IE the person thinks he has the data but he actually doesn't) """ 
    def distributeMapTasks(self, keys, outputAddress):
        buckets =  self.bucketizeKeys(keys) #using short circuiting only is a nifty idea iff we don't have any churn
        myWork = []
        if self.name in buckets.keys():
            myWork = buckets[self.name] #keep my keys
            del buckets[self.name]
            print  self.name, "got my work"
        #print self.name, "adding to map queue"
        myAtoms = [MapAtom(hashid, outputAddress) for hashid in myWork]
        self.mapLock.acquire()
        self.mapQueue = self.mapQueue + myAtoms
        self.mapLock.release()
        #FT: make backups
        fails= []
        for s in self.successorList:
            for key in myWork:
                try:
                    Peer(s).createBackupMap(key,outputAddress)
                except Exception, e:
                    print self.name, "failed backing up maps to", s
                    traceback.print_exc(file=sys.stdout)
                    fails.append(s)
        if (len(fails) >= 1):
            for f in fails:
                self.fixSuccessorList(f)

        #send other keys off
        threads = []
        for dest in buckets.keys():
            t =  Thread(target = self.sendMapJobs, args = (dest, buckets[dest], outputAddress,))
            t.daemon = True
            threads.append(t)
        for t in threads:
            t.start()
        # I may have to join() here
        # no you don't because when this function returns, he made backups of his work
        # yes you do, because he only made backups of his stuff not the stuff he's sending
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
            newTarget, done = self.find(Peer(node).hashid,False)
            self.sendMapJobs(self,newTarget, keys, outputAddress)

    
    
    #might need to be a thread
    def sendReduceJob(self, atom):
        sent = False
        while not sent:
            target, done = self.find(atom.outputAddress, False)
            try:
                Peer(target).handleReduceAtom(atom)  #FT what if he dies after I hand it off?
                # # FTI might eb able to use python's queue and  task_done() and join to to this 
                sent =  True
            except:
                print self.name, "can't send reduce to ", target
                if target == self.succ.name:
                    self.fixSuccessor()
                elif target in self.successorList:
                    self.fixSuccessorList(target)
                else:
                    self.removeNodeFromFingers(target)

    #public 
    def handleReduceAtom(self, reduceDict):
        self.reduceQueue.put(self.dictToReduce(reduceDict))
        return True



    def dictToReduce(self,reduceDict):
        return ReduceAtom(reduceDict['results'],reduceDict['keysInResults'], reduceDict['outputAddress'])


    # group each key into a bucket 
    def bucketizeKeys(self,keylist):
        print self.name, "bucketizing"
        output = {}
        for k in keylist:
            owner = None
            if k in self.data.keys():  # the real question is why doesn't it work without this.  It should now
                owner = self.name
            else:
                owner, t = self.find(k,False)
            if owner in output.keys():
                output[owner].append(k)
            else:
                output[owner] = [k]
        return output
    




    """
    Thread Loops 
    """
    # keep on doing maps
    def mapLoop(self):
        while self.running:
            self.mapLock.acquire()
            if len(self.mapQueue):
                time.sleep(MAINT_INT)
                work  = self.mapQueue.pop() # pop off the queue
                results = self.mapFunc(work.hashid) # excute the job
                # put reduce in my queue.
                self.reduceQueue.put(ReduceAtom(results, {work.hashid : 1},  work.outputAddress))
                # FT: inform backups I am done with map 
                # FT: backup the reduce atom self.myReduceAtoms #deepcopy of atom
            self.mapLock.release() 


    # reduce my jobs to one
    def reduceLoop(self):
        while self.running:
            time.sleep(MAINT_INT*2)
            while self.reduceQueue.qsize() >= 2:
                atom1 = self.reduceQueue.get()
                atom2 = self.reduceQueue.get()
                results = self.reduceFunc(atom1.results,atom2.results)
                keysInResults =  self.mergeKeyResults(atom1.keysInResults, atom2.keysInResults)
                outputAddress = atom1.outputAddress
                self.reduceQueue.put(ReduceAtom(results,keysInResults,outputAddress))
            if not self.reduceQueue.empty() >= 1:
                atom = self.reduceQueue.get()
                if self.keyIsMine(atom.outputAddress):  #FT I thought it was, later it turns out not to be the case
                    self.addToResults(atom)
                else:
                    self.sendReduceJob(atom)


    def areWeThereYetLoop(self):
        while self.resultsHolder:
            time.sleep(MAINT_INT*10)
            missingKeys  = self.getMissingKeys()
            if len(missingKeys) > 0:
                print self.name, "Waiting on ", missingKeys
            else:
                print self.name, "\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\nDone"
                print self.results.results
                print self.results.keysInResults
                break




    def mergeKeyResults(self, a, b):
        for k in a.keys():
            if k in b.keys():
                b[k]+=a[k]
            else:
                b[k]=a[k]
        return b


    def getMissingKeys(self):
        missingKeys = []
        for key in self.results.keysInResults.keys()[:]:
            if self.results.keysInResults[key]  ==  0:
                missingKeys.append(key)
        return missingKeys


