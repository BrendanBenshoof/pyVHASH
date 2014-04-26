from pyChord import Node, getHashString, Peer, hashBetweenRightInclusive, NUM_SUCCESSORS
from cfs import DataAtom, KeyFile, makeBlocks

deletions = []

NUM_PREDECESSORS = NUM_SUCCESSORS+1
class DHTnode(Node):
    def __init__(self,host,ip):
        Node.__init__(self,host,ip)
        self.data = {} # data I'm responsible for 
        self.backups = {} # data I'm holding onto for someone else
        self.predecessorList = [self.name]*NUM_PREDECESSORS
        self.addNewFunc(self.getPredecessorList, "getPredecessorList")
        self.addNewFunc(self.put,"put")
        self.addNewFunc(self.get,"get")
        self.addNewFunc(self.store,"store")
        self.addNewFunc(self.storeFile,"storeFile")
        self.addNewFunc(self.retrieve,"retrieve")
        self.addNewFunc(self.retrieveFile,"retrieveFile")
        self.addNewFunc(self.backup,"backup")
        self.addNewFunc(self.myInfo,"myInfo")
        

    def myInfo(self):
        return str(self.predecessorList) + self.name + str(self.successorList)+ "\n" +self.ringInfo() + "\nData: "
        + str(self.data.values()) +  "\nBackups: " + str(self.backups.values()) + "\n\n"
        #print self.data

    def ringInfo(self):
        info = ""
        for x in self.predecessorList:
            info = info + str(Peer(x).hashid)[:6] + " "
        info = info + str(self.hashid)[:6] + " "
        for x in self.successorList:
            info = info + str(Peer(x).hashid)[:6] + " "
        return info




    def findSuccessor(self, key, dataRequest = False):
        if dataRequest and (key in self.data.keys() or key in self.backups.keys()):
            #print self.name, "short circuited the request"
            return self.name
        else:
            return super(DHTnode,self).findSuccessor(key,dataRequest)

    def find(self, key, dataRequest):
        if dataRequest and (key in self.data.keys() or key in self.backups.keys()):
                #print self.name, "short circuited the request"
                return self.name, True
        else:
            return super(DHTnode,self).find(key,dataRequest)

    def updateSuccessorList(self):
        oldList  = self.successorList
        super(DHTnode,self).updateSuccessorList()
        newSuccessors = [node for node in self.successorList if node not in oldList]
        for node in newSuccessors:
            self.backupToNewSuccessor(node)

    def notify(self,poker):
        hasNewPred = super(DHTnode,self).notify(poker)
        self.updatePredecessorList()
        if hasNewPred:
            try:
                for key in self.data.keys()[:]:
                    if hashBetweenRightInclusive(long(key,16), Peer(self.predecessorList[-2]).hashid,self.pred.hashid):  #check here for weird behavioer
                        self.relinquishData(key)
            except Exception:
                print self.name, "fix this"
                self.predecessorList.pop()
                self.pred = Peer(self.predecessorList[-1])
                self.updatePredecessorList()
        return True
    
    def checkPred(self):
        if super(DHTnode,self).checkPred():
            self.purgeBackups()
            return True
        else:
            self.predecessorList.pop()
            self.pred = Peer(self.predecessorList[-1])
            self.updatePredecessorList()
            return False

    def updatePredecessorList(self):  # what if pred  = self?
        try:
            self.predecessorList = Peer(self.pred.name).getPredecessorList()[1:] + [self.pred.name]
        except Exception as e:
            # infinite recursion occured here
            print self.name, "failed to updatePredecessorList", self.pred.name, self.predecessorList
            self.predecessorList.pop()
            if len(self.predecessorList) == 0:
                self.pred =  None
                [self.name]*NUM_PREDECESSORS
            else:
                self.pred = Peer(self.predecessorList[-1])
                self.updatePredecessorList()

    # public
    def getPredecessorList(self):
        return self.predecessorList[:]

    def purgeBackups(self):
        for key in self.backups.keys()[:]:
            if self.pred is not None and not self.pred.name == self.name:  #possible logic error location
                if not hashBetweenRightInclusive(long(key,16), Peer(self.predecessorList[0]).hashid, self.hashid):
                    try:
                        deletions.append((self.backups[key], str(Peer(self.predecessorList[0]).hashid)[:6], str(key)[:6], str(self.hashid)[:6]))
                        del self.backups[key]
                    except Exception, e:
                        print self.backups, e
                elif self.keyIsMine(key):
                    self.makeBackupMine(key)






    def store(self,name,val):
        key = getHashString(name)
        target = self.findSuccessor(key,False)  # if fails do wut?  I don't think it will
        try:
            Peer(target).put(key,val)
        except Exception as e:
            print self.name, e, "the node I tried to store in literally just died"
        return True

    def retrieve(self,name):
        key = getHashString(name)
        target = self.findSuccessor(key,True) # if fails do wut?
        return Peer(target).get(key), target, str(Peer(target).hashid)[:6]

    def storeFile(self, filename):
        keyfile, blocks =  makeBlocks(filename)
        chunks =  [keyfile] + blocks
        #target = self.findSuccessor(keyfile.hashid)
        #Peer(target).put(keyfile.hashid, keyfile)  # ???
        for block in chunks:
            done  = False
            while not done:
                try:
                    target = self.findSuccessor(block.hashid)
                    Peer(target).put(block.hashid, block)
                except Exception as e:
                    print self.name, "failed put at", target, "retrying"  
                    time.sleep(MAINT_INT)
                else:
                    done = True
        return True




    def retrieveFile(self, filename):
        keyfile =  self.getKeyfile(filename)
        keys = keyfile['keys']
        blocks = []
        for key in keys:
            done = False
            tries = 0
            while not done and tries < 10:
                try:
                    target = self.findSuccessor(key,True)
                    block = Peer(target).get(key)
                    blocks.append(block)
                    done = True
                except:
                    tries = tries + 1
                    print self.name, "failed to get a piece from", target, "retrying" 
                    time.sleep(MAINT_INT)
            if not done:
                print "retrieve failed"
                return False
        return blocks

    def getKeyfile(self, filename):
        key = getHashString(filename)
        while True:
            try:
                target = self.findSuccessor(key)
                keyfile =  Peer(target).get(key) # why is this a dict?  rpc it turns out
                return keyfile
            except Exception as e:
                print self.name, "failed to retrieve keyfile from", target 
                time.sleep(MAINT_INT)

    #make more efficient
    def backupToNewSuccessor(self, newSuccessor):
            try:
                for k, v in self.data.items():
                    Peer(newSuccessor).backup(k,v)
            except Exception as e: # and.... it's gone
                print self.name, "failed backing up stuff to", newSuccessor 
                self.fixSuccessorList(newSuccessor)

    def backupNewData(self, key, val):
        fails = []
        for s in self.successorList:
            try:
                Peer(s).backup(key,val)
            except Exception, e:
                print self.name, "failed backing up new stuff to", s  
                fails.append(s)
        if (len(fails) >= 1):
            for f in fails:
                self.fixSuccessorList(f)
                
                
    
    def relinquishData(self,key):
        val = None
        try:
            val = self.data[key]
        except:
            print self, "Well that was weird", key, self.data
        else:
            try:
                Peer(self.pred.name).put(key,val)
            except Exception:
                self.pred = None  #or fix by searching for his hash -1
                raise Exception("he died on me")
            else:
                self.backups[key] = val
                del self.data[key]

    def makeBackupMine(self, key):
        val = None
        try:
            val = self.backups[key]
        except:
            print self.name, "Well that was weirder", key
        else:
            self.data[key] = val
            del self.backups[key]
            self.backupNewData(key,val)



    def keyIsMine(self, key):
        if self.pred is None:
            return True
        return hashBetweenRightInclusive(long(key,16), self.pred.hashid, self.hashid)



    # returns the list of keys in this list
    def myKeysInList(self,keyList):
        return [x for x in keyList if x in self.data.keys()]

    def myKeysNotInList(self,keyList):
        return [x for x in keyList if x not in self.data.keys()]

    def backupsInList(self,keyList):
        return [x for x in keyList if x in self.backups.keys()]

    def backupsNotInList(self,keyList):
        return [x for x in keyList if x in self.backups.keys()]



    #public
    def put(self,key,val):
        #print "stored", val, "at", key
        self.data[key]=val
        # can I create new threads to do this?
        self.backupNewData(key,val)
        return True

    def get(self,key):
        if key in self.data.keys():
            return self.data[key]
        elif key in self.backups.keys():
            return self.backups[key]
        else: 
            return "FAIL"
            

    # public
    def backup(self,key,val):
        self.backups[key]=val
        return True
