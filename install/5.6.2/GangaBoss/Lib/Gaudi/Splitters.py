from __future__ import division
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

import time
import string
import copy
import tempfile
from Ganga.Core import ApplicationConfigurationError
from GangaBoss.Lib.Dataset import *
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import  File
from Ganga.GPIDev.Lib.Job import Job
from Ganga.GPIDev.Adapters.ISplitter import ISplitter, SplittingError
from Ganga.Utility.util import unique 
import Ganga.Utility.logging
from GangaBoss.Lib.Dataset.DatasetUtils import *
from GangaBoss.Lib.Dataset.BDRegister import BDRegister
from Francesc import GaudiExtras
from PythonOptionsParser import PythonOptionsParser
from Ganga.Utility.Shell import Shell

import MySQLdb
logger = Ganga.Utility.logging.getLogger()
config = Ganga.Utility.Config.getConfig('Boss')

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def copy_app(app):
    cp_app = app.__new__(type(app))
    cp_app.__init__()
    for name,item in cp_app._schema.allItems():
        if not item['copyable']:
            setattr(cp_app,name,cp_app._schema.getDefaultValue(name))
        else:
            c = copy.copy(getattr(app,name))
            setattr(cp_app,name,c)
    if not hasattr(app,'extra'): return cp_app
    cp_app.extra = GaudiExtras() 
    cp_app.extra.input_buffers = app.extra.input_buffers.copy()
    cp_app.extra.input_files = app.extra.input_files[:]
    cp_app.extra.outputsandbox = app.extra.outputsandbox[:]
    cp_app.extra.outputdata = app.extra.outputdata
    return cp_app 

def create_gaudi_subjob(job, inputdata):
    j = Job()
    j.name = job.name
    j.application = copy_app(job.application)
    j.backend = job.backend # no need to deepcopy 
    if inputdata:
        j.inputdata = inputdata
        if hasattr(j.application,'extra'):
            j.application.extra.inputdata = j.inputdata
    else:
        j.inputdata = None
        if hasattr(j.application,'extra'):
            j.application.extra.inputdata = BesDataset()
    j.outputsandbox = job.outputsandbox[:]
    j.outputdata = job.outputdata
    return j

def simple_split(files_per_job, inputs):
    """Just splits the files in the order they came"""
    
    def create_subdataset(data_inputs,iter_begin,iter_end):
        dataset = BesDataset()
        dataset.depth = data_inputs.depth
        dataset.files = data_inputs.files[iter_begin:iter_end]
        return dataset

    result = []
    end = 0
    inputs_length = len(inputs.files)
        
    for i in range(inputs_length // files_per_job):
        start = i * files_per_job
        end = start + files_per_job
        result.append(create_subdataset(inputs,start,end))
            
    if end < (inputs_length):
        result.append(create_subdataset(inputs,end,None))
            
    #catch file loss
    result_length = 0
    for r in result: result_length += len(r.files)
    if result_length != inputs_length:
        raise SplittingError('Data files lost during splitting, please send '\
                             'a bug report to the Ganga team.')
        
    return result    

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class SplitByFiles(ISplitter):
    """Splits a job into sub-jobs by partitioning the input data

    SplitByFiles can be used to split a job into multiple subjobs, where
    each subjob gets an unique subset of the inputdata files.
    """
    _name = 'SplitByFiles'
    docstr = 'Maximum number of files to use in a masterjob (-1 = all files)'
    _schema = Schema(Version(1,0),{
        'filesPerJob' : SimpleItem(defvalue=10,
                                   doc='Number of files per subjob'),        
        'maxFiles' : SimpleItem(defvalue=-1, doc=docstr)})

    def _splitFiles(self,inputs):
        # don't let user use this if they're using the Dirac backend
        job = None 
        try:
            job = self.getJobObject()
        except:
            pass
        if job:
            if job.backend.__module__.find('Dirac') > 0:
                msg = 'SplitByFiles should not be used w/ the Dirac backend.'\
                      ' You probably want the DiracSplitter.'
                raise SplittingError(msg)

        return simple_split(self.filesPerJob,inputs)

    def split(self,job):
        if self.filesPerJob < 1:
            logger.error('filesPerJob must be greater than 0.')
            raise SplittingError('filesPerJob < 1 : %d' % self.filesPerJob)

        subjobs=[]
        inputdata = job.inputdata
        if hasattr(job.application,'extra'):
            inputdata = job.application.extra.inputdata
        inputs = BesDataset()
        inputs.depth = inputdata.depth
        if int(self.maxFiles) == -1:
            inputs.files = inputdata.files[:]
            logger.info("Using all %d input files for splitting" % len(inputs))
        else:
            inputs.files = inputdata.files[:self.maxFiles]
            logger.info("Only using a maximum of %d inputfiles"
                        % int(self.maxFiles))
        
        datasetlist = self._splitFiles(inputs)
        for dataset in datasetlist:
            subjobs.append(create_gaudi_subjob(job,dataset))
                
        return subjobs
    
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class OptionsFileSplitter(ISplitter):
    '''Split a jobs based on a list of option file fragments
    
    This Splitter takes as argument a list of option file statements and will
    generate a job for each item in this list. The value of the indevidual list
    item will be appended to the master options file. A use case of this
    splitter would be to change a parameter in an algorithm (e.g. a cut) and to
    recreate a set of jobs with different cuts
    '''
    _name = "OptionsFileSplitter"
    docstr = "List of option-file strings, each list item creates a new subjob"
    _schema =Schema(Version(1,0),
                    {'optsArray': SimpleItem(defvalue=[],doc=docstr)})

    def split(self,job):        
        subjobs=[]
        for i in self.optsArray:
            j = create_gaudi_subjob(job, job.inputdata)
            j.application.extra.input_buffers['data.py'] += i
            subjobs.append(j)
        return subjobs

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class GenSplitter(ISplitter):
    """Create a set of Gauss jobs based on the total number of jobs and the
    number of events per subjob.
    
    This Splitter will create a set of Gauss jobs using two parameters:
    'eventsPerJob' and 'numberOfJobs'. Each job uses a different random seed
    using the Gaudi options file statement 'GaussGen.FirstEventNumber' and will
    produce the amount of events sepcified in 'eventsPerJob'. The total number
    of generated events therefore will be 'eventsPerJob*numberOfJob'.
    """
    _name = "GenSplitter"
    _schema =Schema(Version(1,0),{
            'eventsPerJob': SimpleItem(defvalue=5,doc='Number of '  \
                                       'generated events per job'),
            'numberOfJobs': SimpleItem(defvalue=2,doc="No. of jobs to create")
            })

    def split(self,job):
        subjobs=[]
        for i in range(self.numberOfJobs):
            j = create_gaudi_subjob(job, job.inputdata)
            first = i*self.eventsPerJob + 1
            opts = 'from Gaudi.Configuration import * \n'
            #opts += 'from Configurables import GenInit \n'
            opts += 'ApplicationMgr().EvtMax = %d\n' % self.eventsPerJob
            j.application.extra.input_buffers['data.py'] += opts
            logger.debug("Creating job %d w/ FirstEventNumber = %d"%(i,first))
            subjobs.append(j)
            
        return subjobs

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class BossSplitter(ISplitter):
    _name = "BossSplitter"
    _schema =Schema(Version(1,0),{
            'evtMaxPerJob': SimpleItem(defvalue=5,doc='Number of '  \
                                       'events per job'),
            })

    def split(self,job):
        evtMaxPerJob = self.evtMaxPerJob
        subjobs=[]
        rndmSeed = 0
        dbuser = config["dbuser"]
        dbpass = config["dbpass"]
        dbhost = config["dbhost"]
        shell = self._getShell()
        optsfiles = [fileitem.name for fileitem in job.application.optsfile]
        parser = PythonOptionsParser(optsfiles,job.application.extraopts,shell) 
        runRangeBlocks = parser.get_run_range()

        evtMax = parser.get_EvtMax()
        outputFileName = parser.get_OutputFileName()
        head = '_'.join(outputFileName.split('_')[0:4])
        bossVer = outputFileName.split('_')[0]
        resonance = outputFileName.split('_')[1]
        eventType = outputFileName.split('_')[2]
        streamId = outputFileName.split('_')[3]

        # CN: get Boss release from output file name, but in '.' format
        bossVer_split = list(bossVer)
        bossRelease = bossVer_split[0]+"."+bossVer_split[1]+"."+bossVer_split[2]

        logger.debug("Boss release is %s" % bossRelease)

        # get initial random seed from DB
        connection = MySQLdb.connect(user=dbuser, passwd=dbpass, host=dbhost, db="offlinedb")
        cursor = connection.cursor()
        sql_rndm = 'select MaxSeed from seed;'
        cursor.execute(sql_rndm)
        # there is only one row in this table, so just fetch the first row
        rndmSeed = cursor.fetchone()[0]

        # CN: loop over all the run range 'blocks' to get total lumi
        # Looping over all the run range blocks twice, very inefficient, can we do it better?
        lumAll = 0
        for runRange in runRangeBlocks:
            runFrom = runRange[0]
            runTo = runRange[1]

            sql = self._generateSQL(bossRelease, runFrom, runTo)[0]
            connection = MySQLdb.connect(user=dbuser, passwd=dbpass, host=dbhost, db="offlinedb")
            cursor = connection.cursor()
            cursor.execute(sql)
            for row in cursor.fetchall():
                logger.error("zhangxm log: row[0] %d, row[1] %f\n" % (row[0], row[1]))
                lumAll = lumAll + row[1]

        # CN: loop over all the run range blocks again to do the job splitting
        for runRange in runRangeBlocks:
            runFrom = runRange[0]
            runTo = runRange[1]

            # CN: read round number from text file (contains run numbers
            # and corresponding exp / round numbers)
            # TODO: use absolute path to 'official' data file for this
            expNum = self._getRoundNum("RoundSearch.txt", runFrom, runTo)

            sql, sftVer, parVer = self._generateSQL(bossRelease, runFrom, runTo)

            connection = MySQLdb.connect(user=dbuser, passwd=dbpass, host=dbhost, db="offlinedb")
            cursor = connection.cursor()
            cursor.execute(sql)
            cursor1 = connection.cursor()
            logger.error('zhangxm log: parameters for file catalog: \
                         eventType->%s, streamId->%s, resonance->%s, expNum->%s, bossVer->%s' \
                         % (eventType, streamId, resonance, expNum, bossVer))
            fcdir = self._createFcDir(job, eventType, streamId, resonance, expNum, bossVer)
            for row in cursor.fetchall():
                runId = row[0] 
                lum = row[1]
                sql = 'select EventID from McNextEventID where RunID = %d && SftVer = "%s";' % (runId, sftVer) 
                print "sql: %s" % sql
                if cursor1.execute(sql): 
                    for rowE in cursor1.fetchall():
                        eventIdIni = rowE[0]
                        print  "eventIdIni: %d" % eventIdIni
                else:
                    eventIdIni = 0
                currentNum = (lum/lumAll)*evtMax
                logger.error("zhangxm log: currentNum %f, evtMax, %d\n" % (currentNum, evtMax))
                i = 0
                if (currentNum-evtMaxPerJob) > 0 : 
                    ratio = currentNum/evtMaxPerJob
                    logger.error("zhangxm log: ratio %f\n" % (ratio))
                    for i in range(1, int(ratio)+1):
                        eventId = eventIdIni+(i-1)*evtMaxPerJob
                        fileId = head + "_run%d_file000%d.rtraw" % (runId, i)
                        rndmSeed = rndmSeed + 1
                        subjob = self._createSubjob(job, runId, eventId, fileId, evtMaxPerJob, rndmSeed, fcdir)
                        subjobs.append(subjob)
                logger.error("zhangxm log: i %d\n" % i)
                eventId = eventIdIni+i*evtMaxPerJob
                nextEventId = eventIdIni + currentNum
                sql = 'select EventID from McNextEventID where RunID = %d && SftVer = "%s";' % (runId, sftVer) 
                if cursor1.execute(sql):
                    sql = 'update McNextEventID set EventID = %d where RunID = %d && SftVer = "%s";' % (nextEventId, runId, sftVer)
                    print "sql: %s" % sql
                else:
                    sql = 'INSERT INTO McNextEventID (EventID, RunID, SftVer) VALUES(%d, %d, "%s");' % (nextEventId, runId, sftVer)
                    print "sql: %s" % sql
                if cursor1.execute(sql):
                    print "OK!"
                fileId = head + "_run%d_file000%d.rtraw" % (runId, i+1)
                eventNum = currentNum - i*evtMaxPerJob
                logger.error("zhangxm log: eventNum %d, currentNum %d\n" % (eventNum, currentNum))
                rndmSeed = rndmSeed + 1
                subjob = self._createSubjob(job, runId, eventId, fileId, eventNum, rndmSeed, fcdir)
                subjobs.append(subjob)
            # end of for loop over entries returned by SQL query
        # end of for loop over all run range blocks


        sql_rndm = 'update seed set MaxSeed = %d;' % rndmSeed
        cursor1.execute(sql_rndm)
        connection.commit()
        cursor.close()
        cursor1.close()
        connection.close()
        return subjobs

    def _createFcDir(self, job, eventType, streamId, resonance, expNum, bossVer):
        dataType = 'rtraw'
        if job.application.recoptsfile:
           dataType = 'dst'
        bdr = BDRegister(dataType, eventType, streamId, resonance, expNum, bossVer)
        fcdir = bdr.createDir()
        logger.error("zhangxm log: use BDRegister to create directory!\n")
        return fcdir

    def _createSubjob(self, job, runId, eventId, fileId, eventNum, rndmSeed, fcdir):
        j = create_gaudi_subjob(job, job.inputdata)
        opts = 'from Gaudi.Configuration import * \n'
        opts += 'importOptions("data.opts")\n'
        sopts = 'RealizationSvc.InitEvtID = %d;\n' % eventId
        sopts += 'RealizationSvc.RunIdList = {%d};\n' % runId
        sopts += 'RootCnvSvc.digiRootOutputFile = "%s";\n' % fileId
        sopts += 'ApplicationMgr.EvtMax = %d;\n' % eventNum
        sopts += 'BesRndmGenSvc.RndmSeed = %d;\n' % rndmSeed
        logger.error("zhangxm log: data.opts_sopts:%s", sopts)
        j.application.extra.input_buffers['data.opts'] += sopts
        j.application.extra.input_buffers['data.py'] += opts
        j.application.extra.outputdata.files = "LFN:" + fcdir + "/" + fileId
        j.application.outputfile = fcdir + "/" + fileId
        j.application.runL = runId 
        #j.application.extra.outputdata.location = fcdir
        if j.application.recoptsfile:
           opts = 'from Gaudi.Configuration import * \n'
           opts += 'importOptions("recdata.opts")\n'
           sopts = 'EventCnvSvc.digiRootInputFile = {"%s"};\n' % fileId
           recfileId = os.path.splitext(fileId)[0] + '.dst' 
           sopts += 'EventCnvSvc.digiRootoutputFile = "%s";\n' % recfileId
           sopts += 'ApplicationMgr.EvtMax = %d;\n' % eventNum
           sopts += 'BesRndmGenSvc.RndmSeed = %d;\n' % rndmSeed
           logger.error("zhangxm log: data.opts:%s", sopts)
           j.application.extra.input_buffers['recdata.opts'] += sopts
           j.application.extra.input_buffers['recdata.py'] += opts
           j.application.extra.outputdata.files = "LFN:" + fcdir + "/" + recfileId
           j.application.outputfile = fcdir + "/" + recfileId
        return j 

    def _getShell(self):
        fd = tempfile.NamedTemporaryFile()
        script = '#!/bin/sh\n'
        gaudirunShell = os.environ["GAUDIRUNENV"]
        #cmd = '%s' % (gaudirunShell)
        cmd = 'source %s' % (gaudirunShell)
        script += '%s \n' % cmd
        fd.write(script)
        fd.flush()
        logger.error("zhangxm log: run boss env script:\n%s" % script)

        shell = Shell(setup=fd.name)
        return shell 

    def _getRoundNum(self, infoFile, runL, runH):
        f = open(infoFile, 'r')
        allLines = f.readlines()
        f.close()

        roundNum = ""
        for line in allLines:
            data = line.strip()
            items = data.split(',')
            file_runL = string.atoi(items[0])
            file_runH = string.atoi(items[1])
                                     
            if runL >= file_runL and runH <= file_runH:
                roundNum = string.lower(items[6])

        return roundNum
        
    def _generateSQL(self, bossRelease, runFrom, runTo):

        dbhost = config["dbhost"]

        # CN: TODO: when db admin gives 'production' user permissions for CalVtxLumVer
        # table, change these back to config version instead of hard-coding guest user
        guest_connection = MySQLdb.connect(user="guest", passwd="guestpass", host=dbhost, db="offlinedb")

        # CN: get SftVer and ParVer for this run range
        sql = 'select SftVer, ParVer from CalVtxLumVer where BossRelease = "%s" and RunFrom <= %d and RunTo >= %d and DataType = "LumVtx";' % (bossRelease, runFrom, runTo)
        sftVer = ""
        parVer = ""
        guest_cursor = guest_connection.cursor()
        guest_cursor.execute(sql)
        for row in guest_cursor.fetchall():
            sftVer = row[0]
            parVer = row[1]

        # CN: generate SQL query to get RunNo and luminosity for this run range
        sql = 'select RunNo,OfflineTwoGam from OfflineLum where RunNo >= %d and RunNo <= %d && SftVer = "%s" and ParVer = "%s";' % (runFrom, runTo, sftVer, parVer)

        guest_cursor.close()
        guest_connection.close()

        return sql, sftVer, parVer


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
                                                                                         
