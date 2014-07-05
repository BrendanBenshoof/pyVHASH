#chordReduce on ChordDHT


MAPPED = {}
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




5)  We keep getting a none error.  One solution is to ensure that if the node add

6) I didn't handle the following: map done, I'm going to send a reduce.... and I die.  The reduce never gets sent. 
"""



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
        
        
    

    # overrides
    
   
    
    
    