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




""" Mapreduce fault tolerence ACTIONS starts here """



""" End Mapreduce Fault tolerence actions """ 
