#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
from Ganga.GPIDev.Base import GangaObject
from LogicalFile import *
from BesDataset import *
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
from BESDIRAC.Badger.API.Badger import Badger
from DIRAC.Core.Security.ProxyInfo                        import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers.Operations  import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals   import getVO
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

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
    docstr = 'Metadata'
    schema['metadata'] = SimpleItem(defvalue={}, doc=docstr)
    _schema = Schema(Version(1,2), schema)
    _category = ''
    _name = "BDRegister"
    _exportmethods = ['createDir', 'registerFile']

    def __init__(self, metadata):
        super(BDRegister, self).__init__()
        self.badger = Badger()
        self.metadata = metadata

    def __construct__(self, args):
        if (len(args) != 1) or (type(args[0]) is not type('')):
            super(BDRegister,self).__construct__(args)
        else:
            self.dataType = args[0]

    def __getUserDir(self):
        '''get username and it's initial to construct the rootDir'''
        username = getProxyInfo()['Value']['username']
        if not username:
            import getpass
            username = getpass.getuser()
        initial = username[:1]

        vo = getVO()
        if not vo:
            vo = 'bes'

        ops = Operations(vo = vo)
        user_prefix = ops.getValue('LFNUserPrefix', 'user')

        basePath = '/' + vo + '/' + user_prefix + '/' + initial + '/' + username

        return basePath

    def __checkUserDir(self, basePath):
        '''if the user dir does not exit, create it'''
        _fcType = 'DataManagement/FileCatalog'
        fc = FileCatalogClient(_fcType)
        result = fc.listDirectory(basePath)
        if not result['Value']['Successful']:
            fc.createDirectory(basePath)

    def createDir(self,userType = 'official'):
        '''create directory for the dataset'''
        metaDic = {}
        metaDic['bossVer']     = self.metadata.get('bossVer',     'x.x.x')
        metaDic['resonance']   = self.metadata.get('resonance',   'unknown')
        metaDic['dataType']    = self.metadata.get('dataType',    'unknown')
        metaDic['eventType']   = self.metadata.get('eventType',   'unknown')
        metaDic['round']       = self.metadata.get('round',       'roundxx')
        metaDic['streamId']    = self.metadata.get('streamId',    'streamxxx')
        metaDic['jobGroup']    = self.metadata.get('jobGroup',    '')
        metaDic['runL']        = self.metadata.get('runL',        0)
        metaDic['runH']        = self.metadata.get('runH',        0)
        metaDic['submitTime']  = self.metadata.get('submitTime',  '1970-01-01 08:00:00')

        if userType == 'official':
            fcdir = self.badger.registerHierarchicalDir(metaDic)
        else:
            rootDir = self.__getUserDir() #yant add
            self.__checkUserDir(rootDir) #yant add
            fcdir = self.badger.registerHierarchicalDir(metaDic,rootDir)
        logger.debug("zhangxm log: create file catalog directory for later files registeration in FC!\n")
        return fcdir

    def registerFile(self, jobs):
        '''register file with metadata'''
        for sj in jobs:
            if sj.status=='completed':
               sj.application.register()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

