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
from GangaBoss.Lib.Gaudi.RTHUtils import *
from GangaBoss.Lib.Dataset.DatasetUtils import *
from GangaBoss.Lib.Dataset.BDRegister import BDRegister
from GangaBoss.Lib.DIRAC.DiracTask import gDiracTask
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
    cp_app.extra.output_name = app.extra.output_name
    cp_app.extra.output_files = app.extra.output_files[:]
    cp_app.extra.data_type = app.extra.data_type.copy()
    cp_app.extra.metadata = app.extra.metadata.copy()
    cp_app.extra.run_ranges = app.extra.run_ranges[:]
    cp_app.extra.ana_file_nos = app.extra.ana_file_nos[:]
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

class BossBaseSplitter(ISplitter):
    _name = "BossBaseSplitter"
    _schema = Schema(Version(1,0), {
            'evtMaxPerJob': SimpleItem(defvalue=50,doc='Number of events per job'),
            'evtTotal': SimpleItem(defvalue=100,doc='Total event number'),
            'seed': SimpleItem(defvalue=None,typelist=['int','type(None)'],doc='Random number seed'),
            'outputEvtNum': SimpleItem(defvalue='',doc='Output event number file'),
            })

    def split(self,job):
        if self.evtMaxPerJob > 50000:
            raise SplittingError('evtMaxPerJob is larger than 50000 : %d. Please set a smaller number' % self.evtMaxPerJob)

        self._jobProperties = []

        self._prepare(job)

        if self.seed is None:
            seedStart = self._getSeedStart()
        else:
            seedStart = self.seed

        if self.outputEvtNum:
            f = open(self.outputEvtNum, 'w')
            for jobProperty in self._jobProperties:
                print >>f, '%8d %8d %8d'%(jobProperty['runL'], jobProperty['runH'], jobProperty['eventNum'])
            f.close()

        realTotalNum = 0
        subjobs=[]
        rndmSeed = seedStart
        for jobProperty in self._jobProperties:
            realTotalNum += jobProperty['eventNum']

            subjob = create_gaudi_subjob(job, job.inputdata)

            self._createSimJob(subjob, jobProperty, rndmSeed)
            if job.application.recoptsfile:
                self._createRecJob(subjob, jobProperty, rndmSeed)
                if job.application.anaoptsfile:
                    self._createAnaJob(subjob, jobProperty)

            subjob.application.extra.output_name          = jobProperty['filename']
            subjob.application.extra.metadata['round']    = jobProperty['round']
            subjob.application.extra.metadata['runFrom']  = jobProperty['runFrom']
            subjob.application.extra.metadata['runTo']    = jobProperty['runTo']

            for step in subjob.application.output_step:
                if step in subjob.application.extra.data_type:
                    subjob.application.extra.output_files.append(subjob.application.extra.output_name + '.' + subjob.application.extra.data_type[step])

            subjobs.append(subjob)
            rndmSeed += 1

        (runFrom, runTo) = get_runLH(job.application.extra.run_ranges)
        round = get_round_nums(job.application.extra.run_ranges)[0]
        taskInfo = {}
        taskInfo['SplitterType'] = self.__class__.__name__
        taskInfo['SeedStart'] = seedStart
        taskInfo['TotalEventNum'] = self.evtTotal
        taskInfo['EventMax'] = self.evtMaxPerJob
        taskInfo['RealTotalEventNum'] = realTotalNum
        taskInfo['Round'] = round
        taskInfo['RunFrom'] = runFrom
        taskInfo['RunTo'] = runTo
        gDiracTask.updateTaskInfo(taskInfo)

        return subjobs

    def _prepare(self):
        pass

    def _addRunEventId(self, jobProperty):
        return ''

    def _createSimJob(self, job, jobProperty, rndmSeed):
        simfilename = jobProperty['filename'] + '.' + job.application.extra.data_type['sim']
        opts = self._addRunEventId(jobProperty)
        opts += 'RootCnvSvc.digiRootOutputFile = "%s";\n' % simfilename
        opts += 'ApplicationMgr.EvtMax = %d;\n' % jobProperty['eventNum']
        opts += 'BesRndmGenSvc.RndmSeed = %d;\n' % rndmSeed
        logger.debug("zhangxm log: data.opts_sopts:%s", opts)
        job.application.extra.input_buffers['data.opts'] += opts

        job.application.runL = jobProperty['runL']
        job.application.runH = jobProperty['runH']
        job.application.eventNumber = jobProperty['eventNum']
        job.application.seed = rndmSeed

#        job.application.extra.outputdata.location = fcdir
        job.application.extra.outputdata.files = simfilename
        job.application.outputfile = simfilename

    def _createRecJob(self, job, jobProperty, rndmSeed):
        simfilename = jobProperty['filename'] + '.' + job.application.extra.data_type['sim']
        recfilename = jobProperty['filename'] + '.' + job.application.extra.data_type['rec']
        opts = 'EventCnvSvc.digiRootInputFile = {"%s"};\n' % simfilename
        opts += 'EventCnvSvc.digiRootOutputFile = "%s";\n' % recfilename
        opts += 'ApplicationMgr.EvtMax = %d;\n' % jobProperty['eventNum']
        opts += 'BesRndmGenSvc.RndmSeed = %d;\n' % rndmSeed
        logger.debug("zhangxm log: recdata.opts:%s", opts)
        job.application.extra.input_buffers['recdata.opts'] += opts

        job.application.extra.outputdata.files = recfilename
        job.application.outputfile = recfilename

    def _createAnaJob(self, job, jobProperty):
        recfilename = jobProperty['filename'] + '.' + job.application.extra.data_type['rec']
        anafilename = jobProperty['filename'] + '.' + job.application.extra.data_type['ana']
        opts = 'EventCnvSvc.digiRootInputFile = {"%s"};\n' % recfilename
        if job.application.extra.ana_file_nos:
            opts += 'NTupleSvc.Output = { "%s DATAFILE=\'%s\' OPT=\'NEW\' TYP=\'ROOT\'"};\n' % (job.application.extra.ana_file_nos[0], anafilename)
        opts += 'ApplicationMgr.EvtMax = %d;\n' % jobProperty['eventNum']
        logger.debug("zhangxm log: anadata.opts:%s", opts)
        job.application.extra.input_buffers['anadata.opts'] += opts

        job.application.extra.outputdata.files = anafilename
        job.application.outputfile = anafilename

    def _getSeedStart(self):
        dbuser = config["dbuser"]
        dbpass = config["dbpass"]
        dbhost = config["dbhost"]

        # get initial random seed from DB
        connection = MySQLdb.connect(user=dbuser, passwd=dbpass, host=dbhost, db="offlinedb")
        cursor = connection.cursor()
        sql_rndm = 'select MaxSeed from seed;'
        cursor.execute(sql_rndm)
        # there is only one row in this table, so just fetch the first row
        rndmSeedStart = cursor.fetchone()[0]

        rndmSeedEnd = rndmSeedStart + self._getJobNumber()

        sql_rndm = 'update seed set MaxSeed = %d;' % rndmSeedEnd
        cursor.execute(sql_rndm)
        connection.commit()
        cursor.close()
        connection.close()

        return rndmSeedStart

    def _getJobNumber(self):
        return len(self._jobProperties)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class BossOldSplitter(ISplitter):
    _name = "BossOldSplitter"
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
                logger.debug("zhangxm log: row[0] %d, row[1] %f\n" % (row[0], row[1]))
                #CN: check that lumi > 0
                if row[1] > 0:
                    lumAll = lumAll + row[1]

        # CN: loop over all the run range blocks again to do the job splitting
        for runRange in runRangeBlocks:
            runFrom = runRange[0]
            runTo = runRange[1]

            # CN: read round number from text file (contains run numbers
            # and corresponding exp / round numbers)
            # TODO: use absolute path to 'official' data file for this
            round = self._getRoundNum("/afs/.ihep.ac.cn/bes3/offline/ExternalLib/gangadist/RoundSearch.txt", runFrom, runTo)

            sql, sftVer, parVer = self._generateSQL(bossRelease, runFrom, runTo)

            connection = MySQLdb.connect(user=dbuser, passwd=dbpass, host=dbhost, db="offlinedb")
            cursor = connection.cursor()
            cursor.execute(sql)
            cursor1 = connection.cursor()
            logger.debug('zhangxm log: parameters for file catalog: \
                         eventType->%s, streamId->%s, resonance->%s, round->%s, bossVer->%s' \
                         % (eventType, streamId, resonance, round, bossVer))
            fcdir = self._createFcDir(job, eventType, streamId, resonance, round, bossVer)
            for row in cursor.fetchall():
                runId = row[0]
                #CN: check that lumi > 0
                if row[1] > 0:
                    lum = row[1]
                    sql = 'select EventID from McNextEventID where RunID = %d && SftVer = "%s";' % (runId, sftVer)
                    logger.debug("sql: %s" % sql)
                    if cursor1.execute(sql):
                        for rowE in cursor1.fetchall():
                            eventIdIni = rowE[0]
                            logger.debug("eventIdIni: %d" % eventIdIni)
                    else:
                        eventIdIni = 0
                    currentNum = (lum/lumAll)*evtMax
                    logger.debug("zhangxm log: currentNum %f, evtMax, %d\n" % (currentNum, evtMax))
                    i = 0
                    if (currentNum-evtMaxPerJob) > 0 :
                        ratio = currentNum/evtMaxPerJob
                        logger.debug("zhangxm log: ratio %f\n" % (ratio))
                        for i in range(1, int(ratio)+1):
                            eventId = eventIdIni+(i-1)*evtMaxPerJob
                            fileId = head + "_run%d_file%04d.rtraw" % (runId, i)
                            rndmSeed = rndmSeed + 1
                            subjob = self._createSubjob(job, runId, eventId, fileId, evtMaxPerJob, rndmSeed, fcdir)
                            subjobs.append(subjob)
                    logger.debug("zhangxm log: i %d\n" % i)
                    eventId = eventIdIni+i*evtMaxPerJob
                    nextEventId = eventIdIni + currentNum
                    sql = 'select EventID from McNextEventID where RunID = %d && SftVer = "%s";' % (runId, sftVer)
                    if cursor1.execute(sql):
                        sql = 'update McNextEventID set EventID = %d where RunID = %d && SftVer = "%s";' % (nextEventId, runId, sftVer)
                        logger.debug("sql: %s" % sql)
                    else:
                        sql = 'INSERT INTO McNextEventID (EventID, RunID, SftVer) VALUES(%d, %d, "%s");' % (nextEventId, runId, sftVer)
                        logger.debug("sql: %s" % sql)
                    if cursor1.execute(sql):
                        logger.debug("OK!")
                    fileId = head + "_run%d_file%04d.rtraw" % (runId, i+1)
                    eventNum = currentNum - i*evtMaxPerJob
                    logger.debug("zhangxm log: eventNum %d, currentNum %d\n" % (eventNum, currentNum))
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

    def _createFcDir(self, job, eventType, streamId, resonance, round, bossVer):
        dataType = 'rtraw'
        if job.application.recoptsfile:
           dataType = 'dst'
        bdr = BDRegister(dataType, eventType, streamId, resonance, round, bossVer)
        fcdir = bdr.createDir()
        logger.debug("zhangxm log: use BDRegister to create directory!\n")
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
        logger.debug("zhangxm log: data.opts_sopts:%s", sopts)
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
           sopts += 'EventCnvSvc.digiRootOutputFile = "%s";\n' % recfileId
           sopts += 'ApplicationMgr.EvtMax = %d;\n' % eventNum
           sopts += 'BesRndmGenSvc.RndmSeed = %d;\n' % rndmSeed
           logger.debug("zhangxm log: data.opts:%s", sopts)
           j.application.extra.input_buffers['recdata.opts'] += sopts
           j.application.extra.input_buffers['recdata.py'] += opts
           j.application.extra.outputdata.files = "LFN:" + fcdir + "/" + recfileId
           j.application.outputfile = fcdir + "/" + recfileId
        return j

    def _getShell(self):
        fd = tempfile.NamedTemporaryFile()
        script = '#!/bin/bash\n'
        gaudirunShell = os.environ["GAUDIRUNENV"]
        #cmd = '%s' % (gaudirunShell)
        cmd = 'source %s' % (gaudirunShell)
        script += '%s \n' % cmd
        fd.write(script)
        fd.flush()
        logger.debug("zhangxm log: run boss env script:\n%s" % script)

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
                roundNum = string.lower(items[5])

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

class UserSplitterByRun(BossBaseSplitter):
    _name = "UserSplitterByRun"
    _schema = BossBaseSplitter._schema.inherit_copy()

    def _prepare(self, job):
        evtMaxPerJob = self.evtMaxPerJob
        evtTotal = self.evtTotal

        (runFrom, runTo) = get_runLH(job.application.extra.run_ranges)

        bossRelease = job.application.version
        bossVer = job.application.extra.metadata.get('bossVer', 'xxx')
        resonance = job.application.extra.metadata.get('resonance', 'unknown')
        eventType = job.application.extra.metadata.get('eventType', 'unknown')
        streamId = job.application.extra.metadata.get('streamId', 'streamxxx')
        head = bossVer + '_' + resonance + '_' + eventType + '_' + streamId


        dbuser = config["dbuser"]
        dbpass = config["dbpass"]
        dbhost = config["dbhost"]


        # database for the eventId
        connection = MySQLdb.connect(user=dbuser, passwd=dbpass, host=dbhost, db="offlinedb")
        cursor = connection.cursor()

        # database for the luminosity of all the runs
        guest_connection = MySQLdb.connect(user="guest", passwd="guestpass", host=dbhost, db="offlinedb")
        guest_cursor = guest_connection.cursor()

        lumAll = 0
        lums = []
        for runRange in job.application.extra.run_ranges:
            sql, sftVer, parVer = self._generateSQL(guest_cursor, bossRelease, runRange[0], runRange[1])
            cursor.execute(sql)
            for row in cursor.fetchall():
                logger.debug("zhangxm log: row[0] %d, row[1] %f\n" % (row[0], row[1]))
                #CN: check that lumi > 0
                if row[1] > 0:
                    lumAll = lumAll + row[1]
                    lums.append((row[0], row[1], sftVer))

        guest_cursor.close()
        guest_connection.close()


        for runId, lum, sftVer in lums:
            currentNum = int((lum/lumAll)*evtTotal)
            logger.debug("zhangxm log: currentNum %f, evtTotal, %d\n" % (currentNum, evtTotal))

            i = 0
            leftNum = currentNum
            eventIdIni = self._getEventId(cursor, runId, sftVer, currentNum)
            while leftNum > 0:
                i += 1
                jobProperty = {}
                jobProperty['filename'] = head + "_%s_%s_file%04d" % (runId, runId, i)
                jobProperty['eventNum'] = evtMaxPerJob if leftNum > evtMaxPerJob else leftNum

                jobProperty['runFrom'] = runFrom
                jobProperty['runTo'] = runTo
                jobProperty['runL'] = runId
                jobProperty['runH'] = runId
                jobProperty['eventId'] = eventIdIni

                jobProperty['round'] = get_round_nums([(runId, runId)])[0]

                if jobProperty['eventNum'] > 0:
                    self._jobProperties.append(jobProperty)

                leftNum -= evtMaxPerJob
                eventIdIni += evtMaxPerJob

        cursor.close()
        connection.close()

    def _addRunEventId(self, jobProperty):
        opts = 'RealizationSvc.InitEvtID = %d;\n' % jobProperty['eventId']
        opts += 'RealizationSvc.RunIdList = {%d};\n' % jobProperty['runL']
        return opts


    def _getEventId(self, cursor, runId, sftVer, currentNum):
        eventIdIni = 0

        sql = 'select EventID from McNextEventID where RunID = %d && SftVer = "%s";' % (runId, sftVer)
        logger.debug("sql: %s" % sql)
        if cursor.execute(sql):
            for rowE in cursor.fetchall():
                eventIdIni = rowE[0]
                logger.debug("eventIdIni: %d" % eventIdIni)
        else:
            eventIdIni = 0

        nextEventId = eventIdIni + currentNum

        sql = 'select EventID from McNextEventID where RunID = %d && SftVer = "%s";' % (runId, sftVer)
        if cursor.execute(sql):
            sql = 'update McNextEventID set EventID = %d where RunID = %d && SftVer = "%s";' % (nextEventId, runId, sftVer)
            logger.debug("sql: %s" % sql)
        else:
            sql = 'INSERT INTO McNextEventID (EventID, RunID, SftVer) VALUES(%d, %d, "%s");' % (nextEventId, runId, sftVer)
            logger.debug("sql: %s" % sql)
        if cursor.execute(sql):
            logger.debug("OK!")

        return eventIdIni

    def _generateSQL(self, guest_cursor, bossRelease, runFrom, runTo):

        br = bossRelease
        if bossRelease == '6.6.4.p02':
            br = '6.6.4.p01'
        elif bossRelease == '7.0.2.p02':
            br = '7.0.2.p01'

        # CN: get SftVer and ParVer for this run range
        sql = 'select SftVer, ParVer from CalVtxLumVer where BossRelease = "%s" and RunFrom <= %d and RunTo >= %d and DataType = "LumVtx";' % (br, runFrom, runTo)
        sftVer = ""
        parVer = ""
        guest_cursor.execute(sql)
        for row in guest_cursor.fetchall():
            sftVer = row[0]
            parVer = row[1]

        # CN: generate SQL query to get RunNo and luminosity for this run range
        sql = 'select RunNo,OfflineTwoGam from OfflineLum where RunNo >= %d and RunNo <= %d && SftVer = "%s" and ParVer = "%s";' % (runFrom, runTo, sftVer, parVer)

        return sql, sftVer, parVer


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class UserSplitterByEvent(BossBaseSplitter):
    _name = "UserSplitterByEvent"
    _schema = BossBaseSplitter._schema.inherit_copy()

    def _prepare(self, job):
        evtMaxPerJob = self.evtMaxPerJob
        evtTotal = self.evtTotal
        metadata = job.application.extra.metadata

        (runFrom, runTo) = get_runLH(job.application.extra.run_ranges)
        round = get_round_nums(job.application.extra.run_ranges)[0]

        bossVer = job.application.extra.metadata.get('bossVer', 'xxx')
        resonance = job.application.extra.metadata.get('resonance', 'unknown')
        eventType = job.application.extra.metadata.get('eventType', 'unknown')
        streamId = job.application.extra.metadata.get('streamId', 'streamxxx')
        head = '%s_%s_%s_%s_%s_%s' % (bossVer, resonance, eventType, streamId, runFrom, runTo)


        i = 0
        leftNum = evtTotal
        while leftNum > 0:
            i += 1
            jobProperty = {}
            jobProperty['filename'] = head + "_file%04d" % i
            jobProperty['eventNum'] = evtMaxPerJob if leftNum > evtMaxPerJob else leftNum

            jobProperty['runFrom'] = runFrom
            jobProperty['runTo'] = runTo
            jobProperty['runL'] = runFrom
            jobProperty['runH'] = runTo
            jobProperty['round'] = round

            self._jobProperties.append(jobProperty)

            leftNum -= evtMaxPerJob

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class BossSplitter(UserSplitterByRun):
    _name = "BossSplitter"
    _schema = UserSplitterByRun._schema.inherit_copy()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class FakeSplitterByRun(BossBaseSplitter):
    _name = "FakeSplitterByRun"
    _schema = BossBaseSplitter._schema.inherit_copy()

    def _prepare(self, job):
        evtMaxPerJob = self.evtMaxPerJob
        evtTotal = self.evtTotal

        (runFrom, runTo) = get_runLH(job.application.extra.run_ranges)

        bossRelease = job.application.version
        bossVer = job.application.extra.metadata.get('bossVer', 'xxx')
        resonance = job.application.extra.metadata.get('resonance', 'unknown')
        eventType = job.application.extra.metadata.get('eventType', 'unknown')
        streamId = job.application.extra.metadata.get('streamId', 'streamxxx')
        head = bossVer + '_' + resonance + '_' + eventType + '_' + streamId


        dbuser = config["dbuser"]
        dbpass = config["dbpass"]
        dbhost = config["dbhost"]


        # database for the eventId
        connection = MySQLdb.connect(user=dbuser, passwd=dbpass, host=dbhost, db="offlinedb")
        cursor = connection.cursor()

        # database for the luminosity of all the runs
        guest_connection = MySQLdb.connect(user="guest", passwd="guestpass", host=dbhost, db="offlinedb")
        guest_cursor = guest_connection.cursor()

        lumAll = 0
        lums = []
        for runRange in job.application.extra.run_ranges:
            sql, sftVer, parVer = self._generateSQL(guest_cursor, bossRelease, runRange[0], runRange[1])
            cursor.execute(sql)
            for row in cursor.fetchall():
                #CN: check that lumi > 0
                if row[1] > 0:
                    lumAll = lumAll + row[1]
                    lums.append((row[0], row[1], sftVer))

        guest_cursor.close()
        guest_connection.close()

        cursor.close()
        connection.close()


        i = 0
        for runId, lum, sftVer in lums:
            i += 1
            jobProperty = {}
            jobProperty['filename'] = head + "_%s_%s_file%04d" % (runId, runId, i)
            jobProperty['eventNum'] = evtMaxPerJob

            jobProperty['runFrom'] = runFrom
            jobProperty['runTo'] = runTo
            jobProperty['runL'] = runId
            jobProperty['runH'] = runId

            jobProperty['round'] = get_round_nums([(runId, runId)])[0]

            self._jobProperties.append(jobProperty)

    def _addRunEventId(self, jobProperty):
        opts = 'RealizationSvc.RunIdList = {%d};\n' % jobProperty['runL']
        return opts

    def _generateSQL(self, guest_cursor, bossRelease, runFrom, runTo):

        br = bossRelease
        if bossRelease == '6.6.4.p02':
            br = '6.6.4.p01'

        # CN: get SftVer and ParVer for this run range
        sql = 'select SftVer, ParVer from CalVtxLumVer where BossRelease = "%s" and RunFrom <= %d and RunTo >= %d and DataType = "LumVtx";' % (br, runFrom, runTo)
        sftVer = ""
        parVer = ""
        guest_cursor.execute(sql)
        for row in guest_cursor.fetchall():
            sftVer = row[0]
            parVer = row[1]

        # CN: generate SQL query to get RunNo and luminosity for this run range
        sql = 'select RunNo,OfflineTwoGam from OfflineLum where RunNo >= %d and RunNo <= %d && SftVer = "%s" and ParVer = "%s";' % (runFrom, runTo, sftVer, parVer)

        return sql, sftVer, parVer

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
