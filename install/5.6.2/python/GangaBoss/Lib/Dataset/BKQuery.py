#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
from Ganga.GPIDev.Base import GangaObject
from LogicalFile import *
from BesDataset import *
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
from BESDIRAC.Badger.API.Badger import Badger

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class BKQuery(GangaObject):
    '''Class for handling bookkeeping queries.

    Currently 4 types of queries are supported: Path, RunsByDate, Run and
    Production.  These correspond to the Dirac API methods
    Dirac.bkQuery<type> (see Dirac docs for details).  


    Path formats are as follows:

    type = "Path":
    /<ConfigurationName>/<Configuration Version>/\
<Sim or Data Taking Condition>/<Processing Pass>/<Event Type>/<File Type>

    type = "RunsByDate":
     /<ConfigurationName>/<Configuration Version>/<Processing Pass>/\
<Event Type>/<File Type> 

    type = "Run":
    /<Run Number>/<Processing Pass>/<Event Type>/<File Type>
    - OR -
    /<Run Number 1>-<Run Number 2>/<Processing Pass>/<Event Type>/<File Type>
    
    type = "Production":
    /<ProductionID>/<Processing Pass>/<Event Type>/<File Type>

    Example Usage:

    bkq = BKQuery (
    dqflag = "All" ,
    path = "/Boss/Collision09/Beam450GeV-VeloOpen-MagDown/Real Data + \
RecoToDST-07/90000000/DST" ,
    type = "Path" 
    ) 

    bkq = BKQuery (
    startDate = "2010-05-18" ,
    selection = "Runs" ,
    endDate = "2010-05-20" ,
    dqflag = "All" ,
    path = "/Boss/Collision10/Real Data/90000000/RAW" ,
    type = "RunsByDate" 
    ) 
    
    bkq = BKQuery (
    dqflag = "All" ,
    path = "/63566-63600/Real Data + RecoToDST-07/90000000/DST" ,
    type = "Run" 
    ) 

    bkq = BKQuery (
    dqflag = "All" ,
    path = "/5842/Real Data + RecoToDST-07/90000000/DST" ,
    type = "Production" 
    ) 

    then (for any type) one can get the data set by doing the following:
    data = bkq.getDataset()

    This will query the bookkeeping for the up-to-date version of the data.
    N.B. BKQuery objects can be stored in your Ganga box.

    '''
    schema = {}
    docstr = 'Bookkeeping query path (type dependent)'
    schema['path'] = SimpleItem(defvalue='' ,doc=docstr)
    docstr = 'Start date string yyyy-mm-dd (only works for type="RunsByDate")'
    schema['startDate'] = SimpleItem(defvalue='' ,doc=docstr)
    docstr = 'End date string yyyy-mm-dd (only works for type="RunsByDate")'
    schema['endDate'] = SimpleItem(defvalue='' ,doc=docstr)
    docstr = 'Data quality flag (string or list of strings).'
    schema['dqflag'] = SimpleItem(defvalue='All',typelist=['str','list'],
                                  doc=docstr)
    docstr = 'Type of query (Path, RunsByDate, Run, Production)'
    schema['type'] = SimpleItem(defvalue='Path',doc=docstr)
    docstr = 'Selection criteria: Runs, ProcessedRuns, NotProcessed (only \
    works for type="RunsByDate")'
    schema['selection'] = SimpleItem(defvalue='',doc=docstr)
    _schema = Schema(Version(1,2), schema)
    _category = ''
    _name = "BKQuery"
    _exportmethods = ['getDataset']

    def __init__(self, path=''):
        super(BKQuery, self).__init__()
        self.path = path

    def __construct__(self, args):
        if (len(args) != 1) or (type(args[0]) is not type('')):
            super(BKQuery,self).__construct__(args)
        else:
            self.path = args[0]

    #need to fix with AMGA interface
    def getDataset(self):
        '''Gets the dataset from the bookkeeping for current path, etc.'''
        files = []
        if not self.path: return None
        if not self.type in ['Path','RunsByDate','Run','Production']:
            raise GangaException('Type="%s" is not valid.' % self.type)
        lfnf = open('/afs/ihep.ac.cn/users/z/zhangxm/ganga/Ganga/install/5.6.2/python/GangaBoss/Lib/Dataset/lfns','r')
        for line in lfnf.readlines(): 
            newline = line.strip()
            files.append(newline)
        ds = BesDataset()
        for f in files: ds.files.append(LogicalFile(f))
        
        return GPIProxyObjectFactory(ds)

    #need to fix with AMGA interface
    def queryCatalog(self, path, type):
        '''Gets the dataset by querying catalog'''
        lfnList = []
        f = open('/afs/ihep.ac.cn/users/z/zhangxm/ganga/Ganga/install/5.6.2/python/GangaBoss/Lib/Dataset/lfns','r')
        for line in f.readlines():
            lfnList.append(line)
        return lfnList

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class BDQueryByMeta(GangaObject):
    """Class for handling Badger queries using metadata.
    
    Example Usage:
    
    bkqd = BKQueryByMeta(condition='resonance=psip bossVer=655 expNum=exp2 streamId=stream002')
    data = bkqd.getDataset()
    """
    
    schema = {}
    docstr = 'Dirac badger query condition.'
    schema['condition'] = SimpleItem(defvalue='', doc=docstr)
    _schema = Schema(Version(1,0), schema)
    _category = ''
    _name = "BDQueryByMeta"
    _exportmethods = ['getDataset']

    def __init__(self):
        super(BDQueryByMeta, self).__init__()

    def __construct__(self, args):
        if (len(args) != 1) or (type(args[0]) is not type({})):
            super(BDQueryByMeta,self).__construct__(args)
        else:
            self.condition = args[0]
            
    def getDataset(self):
        '''Gets the dataset from the bookkeeping for current dict.'''
        if not self.condition: return None
        badger = Badger()
        files = []
        files = badger.getFilesByMetadataQuery(self.condition) 

        ds = BesDataset()
        for f in files: 
           logicalFile = "LFN:"+f  
           logger.debug("zhangxm log: data files LFN: %s", f)
           ds.files.append(logicalFile)

        
        return GPIProxyObjectFactory(ds)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class BDQueryByName(GangaObject):
    """Class for handling Badger queries using name.

    Example Usage:

    bkqd = BKQueryByName(name='')
    data = bkqd.getDataset()
    """

    schema = {}
    docstr = 'Dirac badger dataset name.'
    schema['name'] = SimpleItem(defvalue='', doc=docstr)
    _schema = Schema(Version(1,0), schema)
    _category = ''
    _name = "BDQueryByName"
    _exportmethods = ['getDataset']

    def __init__(self):
        super(BDQueryByName, self).__init__()

    def __construct__(self, args):
        if (len(args) != 1) or (type(args[0]) is not type({})):
            super(BDQueryByName,self).__construct__(args)
        else:
            self.name = args[0]

    def getDataset(self):
        '''Gets the dataset from the bookkeeping for current dict.'''
        if not self.name: return None
        badger = Badger()
        files = []
        files = badger.getFilesByDatasetName(self.name)

        ds = BesDataset()
        for f in files:
           logicalFile = "LFN:"+f
           logger.debug("zhangxm log: data files LFN: %s", f)
           ds.files.append(logicalFile)


        return GPIProxyObjectFactory(ds)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
