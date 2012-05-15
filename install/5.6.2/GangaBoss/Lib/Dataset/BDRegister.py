#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
from Ganga.GPIDev.Base import GangaObject
from LogicalFile import *
from BesDataset import *
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
from DIRAC.Interfaces.API.Badger import Badger

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class BDRegister(GangaObject):
    '''Class for handling Badger dataset register.

    Example Usage:

    bdr = BDRegister (
    dqflag = "All" ,
    path = "/Boss/Collision09/Beam450GeV-VeloOpen-MagDown/Real Data + \
RecoToDST-07/90000000/DST" ,
    type = "Path" 
    ) 

    '''
    schema = {}
    docstr = 'data type'
    schema['dataType'] = SimpleItem(defvalue='' ,doc=docstr)
    docstr = 'event type'
    schema['eventType'] = SimpleItem(defvalue='' ,doc=docstr)
    docstr = 'stream ID'
    schema['streamId'] = SimpleItem(defvalue='' ,doc=docstr)
    docstr = 'resonance'
    schema['resonance'] = SimpleItem(defvalue='' ,doc=docstr)
    docstr = 'experiment number'
    schema['expNum'] = SimpleItem(defvalue='',doc=docstr)
    docstr = 'boss version'
    schema['bossVer'] = SimpleItem(defvalue='Path',doc=docstr)
    _schema = Schema(Version(1,2), schema)
    _category = ''
    _name = "BDRegister"
    _exportmethods = ['createDir', 'registerFile']

    def __init__(self, dataType='', eventType='',streamId='', resonance='',expNum='', bossVer=''):
        super(BDRegister, self).__init__()
        self.badger = Badger()
        self.dataType = dataType
        self.eventType = eventType
        self.streamId = streamId
        self.resonance = resonance
        self.expNum = expNum
        self.bossVer = bossVer

    def __construct__(self, args):
        if (len(args) != 1) or (type(args[0]) is not type('')):
            super(BDRegister,self).__construct__(args)
        else:
            self.dataType = args[0]

    def createDir(self):
        '''create directory for the dataset'''
        metaDic = {'dataType': self.dataType, 'eventType': self.eventType, 'streamId': self.streamId, \
                   'resonance': self.resonance, 'expNum': self.expNum,'bossVer': self.bossVer}
        fcdir = self.badger.registerHierarchicalDir(metaDic)
        logger.error("zhangxm log: create file catalog directory for later files registeration in FC!\n")
        return fcdir

    def registerFile(self, jobs):
        '''register file with metadata'''
        for sj in jobs:
            if sj.status=='completed':
               sj.application.register()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

