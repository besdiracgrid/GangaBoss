#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
from Ganga.GPIDev.Base import GangaObject
from LogicalFile import *
from BesDataset import *
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
#from BESDIRAC.Badger.API.Badger import Badger
from DIRAC.Core.Security.ProxyInfo                        import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers.Operations  import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals   import getVO
from DIRAC.Resources.Catalog.FileCatalogClient            import FileCatalogClient

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
    _category = 'dataset'
    _name = "BDRegister"
    _exportmethods = ['createDir', 'registerFile']

    def __init__(self, metadata):
        super(BDRegister, self).__init__()
#        self.badger = Badger()
        self.dfcOperation = DfcOperation()
        self.metadata = metadata.copy()

    def __construct__(self, args):
        if (len(args) != 1) or (type(args[0]) is not type('')):
            super(BDRegister,self).__construct__(args)
        else:
            self.dataType = args[0]

    def registerFile(self, jobs):
        '''register file with metadata'''
        for sj in jobs:
            if sj.status=='completed':
                sj.application.register()

    def createDataset(self):
        return self.dfcOperation.createDataset(self.metadata)

    def getFileDirName(self):
        return self.dfcOperation.getDirName(self.metadata)

    def getLogDirName(self):
        metaDic = self.metadata.copy()
        metaDic['dataType'] = 'log'
        return self.dfcOperation.getDirName(metaDic)

    def createFileDir(self):
        '''create directory for the dataset'''
        return self.dfcOperation.createDir(self.metadata)

    def createLogDir(self):
        '''create log directory for the dataset'''
        metaDic = self.metadata.copy()
        metaDic['dataType'] = 'log'
        metaDic.pop('jobOptions', None)
        metaDic.pop('decayCard', None)
        return self.dfcOperation.createDir(metaDic)

    def getUnusedStream(self):
        '''get an unused stream ID'''
        return self.dfcOperation.getUnusedStream(self.metadata)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class DfcOperation:
    '''Class for DFC operation'''

    _username = ''
    _groupname = ''
    _rootdir = ''

    def __init__(self):
        _fcType = 'DataManagement/FileCatalog'
        self.client = FileCatalogClient(_fcType)

    def getUserName(self):
        '''get username'''
        if not DfcOperation._username:
            DfcOperation._username = getProxyInfo()['Value'].get('username', 'unknown')
        return DfcOperation._username

    def getGroupName(self):
        '''get groupname'''
        if not DfcOperation._groupname:
            DfcOperation._groupname = getProxyInfo()['Value'].get('group', 'unknown')
        return DfcOperation._groupname

    def getRootDir(self):
        if not DfcOperation._rootdir:
            if self.getGroupName() == 'production':
                DfcOperation._rootdir = self.getOfficialRootDir()
            DfcOperation._rootdir = self.getUserRootDir()
        return DfcOperation._rootdir

    def getOfficialRootDir(self):
        return '/bes'

    def getUserRootDir(self):
        '''get user's initial root directory'''
        username = self.getUserName()
        initial = username[:1]

        vo = getVO()
        if not vo:
            vo = 'bes'

        ops = Operations(vo = vo)
        user_prefix = ops.getValue('LFNUserPrefix', 'user')

        basePath = '/' + vo + '/' + user_prefix + '/' + initial + '/' + username

        return basePath
        
    def isDirExists(self,dir):
        '''check whether dir on DFC exists'''
        result = self.client.listDirectory(dir)
        if result['OK'] and result['Value']['Successful']:
            return True
        return False

    def validateDir(self, dir):
        '''if the dir on DFC does not exist, create it'''
        if not self.isDirExists(dir):
            logger.debug('Creating dir: %s', dir)
            return self.client.createDirectory(dir)

    def _getDatasetPrefix(self):
        if self.getGroupName() == 'production':
            return 'Prod'
        return 'User_' + self.getUserName()

    def createDataset(self, metadata):
        metaDic = {}
        metaDic['resonance'] = metadata.get('resonance', 'unknown')
        metaDic['bossVer']   = metadata.get('bossVer',   'xxx')
        metaDic['eventType'] = metadata.get('eventType', 'unknown')
        metaDic['round']     = metadata.get('round',     'roundxx')
        metaDic['streamId']  = metadata.get('streamId',  'streamxxx')
        metaDic['dataType']  = metadata.get('dataType',  'unknown')
        metaDic['Path']      = self.getRootDir() + ('/Log' if metadata.get('dataType', 'unknown').lower() == 'log' else '/File')
        runFrom = metadata.get('runFrom', '0')
        runTo = metadata.get('runTo', '0')
        metaDic['runL']      = {'>=': runFrom}
        metaDic['runH']      = {'<=': runTo}

        datasetName = '%s_%s_%s_%s_%s_%s_%s_%s_%s' % (self._getDatasetPrefix(),
                                                      metaDic['resonance'], metaDic['bossVer'], metaDic['eventType'], metaDic['round'],
                                                      runFrom, runTo, metaDic['streamId'], metaDic['dataType'])
        result = self.client.addDataset(datasetName, metaDic)
        if not result['OK']:
            logger.warning("Can not create dataset: %s", result['Message'])

        return datasetName

    def getDirName(self, metadata):
        return self._getDirNames(metadata)[6]

    def _getDirNames(self, metadata):
        rootDir = self.getRootDir()
        dir_start = rootDir + ('/Log' if metadata.get('dataType', 'unknown').lower() == 'log' else '/File')
        dir_resonance = dir_start + '/' + metadata.get('resonance', 'unknown')
        dir_bossVer = dir_resonance + '/' + metadata.get('bossVer', 'xxx')
        dir_data_mc = dir_bossVer + ('/data' if metadata.get('streamId', 'streamxxx').lower() == 'stream0' else '/mc')
        dir_eventType = dir_data_mc + '/' +metadata.get('eventType', 'unknown')
        dir_round = dir_eventType + '/' + metadata.get('round', 'roundxx')
        dir_streamId = dir_round + '/' + metadata.get('streamId', 'streamxxx')
        return (dir_start, dir_resonance, dir_bossVer, dir_data_mc, dir_eventType, dir_round, dir_streamId)

    def createDir(self, metadata):
#        (dir_start, dir_resonance, dir_bossVer, dir_data_mc, dir_eventType, dir_round, dir_streamId) = self._getDirNames(metadata)
        dirNames = self._getDirNames(metadata)

        dirs = {}
        dirs[dirNames[0]] = None
        dirs[dirNames[1]] = {'resonance':  metadata.get('resonance', 'unknown')}
        dirs[dirNames[2]] = {'bossVer':    metadata.get('bossVer',   'xxx')}
        dirs[dirNames[3]] = None
        dirs[dirNames[4]] = {'eventType':  metadata.get('eventType', 'unknown')}
        dirs[dirNames[5]] = {'round':      metadata.get('round',     'roundxx')}
        dirs[dirNames[6]] = {'streamId':   metadata.get('streamId',  'streamxxx'),
                             'dataType':   metadata.get('dataType',  'unknown'),}
        if metadata.has_key('jobOptions'):
            dirs[dirNames[6]]['jobOptions'] = self.truncate(metadata['jobOptions'], 4000)
        if metadata.has_key('decayCard'):
            dirs[dirNames[6]]['decayCard'] = self.truncate(metadata['decayCard'], 4000)

        for dirName in dirNames:
            if not self.isDirExists(dirName):
                result = self.client.createDirectory(dirName)
                if not result['OK']:
                    logger.error(result['Message'])
                    continue

                if dirs[dirName]:
                    result = self.client.setMetadata(dirName, dirs[dirName])
                    if not result['OK']:
                        logger.error(result['Message'])

        return dirNames[6]

    def getUnusedStream(self, metadata):
        dirNames = self._getDirNames(metadata)
        dir_round = dirNames[5]
        result = self.client.listDirectory(dir_round)
        streamDirs = []
        if result['OK'] and result['Value']['Successful']:
            for fn in result['Value']['Successful'][dir_round]['SubDirs'].keys():
                streamDirs.append(os.path.basename(fn))

        streamIds = []
        for streamDir in streamDirs:
            if streamDir.startswith('stream'):
                try:
                    streamId = int(streamDir[6:])
                except ValueError:
                    pass
                else:
                    streamIds.append(streamId)

        maxId = max(streamIds) if streamIds else 0
        newId = 'stream001'
        if maxId != 999:
            newId = maxId + 1
        else:
            for id in range(1, 10000):
                if not id in streamIds:
                    newId = id
                    break
        return 'stream%03d' % newId


    def truncate(self, srcstr, limit):
        '''Truncate too long strings'''
        last_line = '\n# File is too long. There are still lines not kept here...\n'

        if len(srcstr) <= limit:
            return srcstr

        trunc_pos = limit - len(last_line)
        truncated = srcstr[:trunc_pos] + last_line
        return truncated

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
