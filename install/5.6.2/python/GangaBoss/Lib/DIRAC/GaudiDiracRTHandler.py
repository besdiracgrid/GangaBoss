#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from Ganga.Utility.Config import getConfig
from DiracUtils import *
from DiracScript import *
from GangaBoss.Lib.Gaudi.RTHUtils import *
from GangaBoss.Lib.Dataset.BDRegister import BDRegister
from Ganga.GPIDev.Lib.File import FileBuffer, File
from Ganga.Utility.Shell import Shell

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def gaudi_dirac_wrapper(cmdline):
    return """#!/usr/bin/env python
'''Script to run Gaudi application'''

from os import curdir, system, environ, pathsep, sep, getcwd
from os.path import join
import sys

def prependEnv(key, value):
    if environ.has_key(key): value += (pathsep + environ[key])
    environ[key] = value
    print key
    print value 

# Main
if __name__ == '__main__':

    prependEnv('LD_LIBRARY_PATH', getcwd() + '/lib')
    prependEnv('PYTHONPATH', getcwd() + '/InstallArea/python')
    system('source bossenv_dirac_gaudirun.sh')
    #sys.exit(system(%s)/256)
  """ % cmdline

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def gaudi_run_wrapper():
    return """#!/bin/bash

bossVer=$1

besRoot=/cvmfs/boss.cern.ch
cd ${besRoot}/slc5_amd64_gcc43/${bossVer}
source setup.sh
source scripts/${bossVer}/setup.sh
source dist/${bossVer}/TestRelease/*/cmt/setup.sh
cd $OLDPWD

gaudirun.py -n -o options_rantrg.opts options.pkl data.py
"""

def boss_run_wrapper():
    return """#!/bin/bash

bossVer=$1
prefix=$2

besRoot=/cvmfs/boss.cern.ch
cd ${besRoot}/slc5_amd64_gcc43/${bossVer}
source setup.sh
source scripts/${bossVer}/setup.sh
source dist/${bossVer}/TestRelease/*/cmt/setup.sh
cd $OLDPWD

gaudirun.py -n -v -o ${prefix}options.opts ${prefix}options.pkl ${prefix}data.py
boss.exe ${prefix}options.opts 1>>${prefix}bosslog 2>>${prefix}bosserr
result=$?
if [ $result != 0 ]; then 
   echo "ERROR: boss.exe on ${prefix}options failed" >&2
   exit $result
fi

if [[ $prefix == 'rec' ]]; then
   cnvsvc=EventCnvSvc
else
   cnvsvc=RootCnvSvc
fi

gaudirun.py -n -o ${prefix}options.py ${prefix}options.pkl ${prefix}data.py
outputfile=`python -c "print eval(open('${prefix}options.py').read())['${cnvsvc}']['digiRootOutputFile']"`
if [ ! -f $outputfile ]; then
   echo "ERROR: $outputfile not generated" >&2
   exit 2
fi
"""

def boss_script_wrapper(bossVer, lfn, loglfn, se, eventNumber, runL, runH):
    script_head = """#!/usr/bin/env python

import os, sys, shutil, re, time, random
from subprocess import Popen, call

from DIRAC import S_OK, S_ERROR

from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = False )

from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport

from DIRAC.Interfaces.API.Dirac import Dirac
dirac = Dirac()

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
fccType = 'DataManagement/FileCatalog'
fcc = FileCatalogClient(fccType)

delDisableWatchdog = False

logFile = open('script.log', 'w')
errFile = open('script.err', 'w')
rantrgLogFile = open('rantrg.log', 'w+')
rantrgErrFile = open('rantrg.err', 'w')

jobID = os.environ.get('DIRACJOBID', '0')
bossVer = '%s'
lfn = '%s'
loglfn = '%s'
se = '%s'
eventNumber = %s
runL = %s
runH = %s
""" % (bossVer, lfn, loglfn, se, eventNumber, runL, runH)

    script_body = """
def cmd(args):
    startcmd = '%s\\n%s  Start Executing: %s' % ('='*80, '>'*16, args)
    print >>logFile, startcmd
    print >>errFile, startcmd
    logFile.flush()
    errFile.flush()

    result = call(args, stdout=logFile, stderr=errFile)

    endcmd = '%s  End Executing: %s\\n%s\\n' % ('<'*16, args, '='*80)
    print >>logFile, endcmd
    print >>errFile, endcmd
    logFile.flush()
    errFile.flush()
    return result

def cp(src, dst):
    if os.path.exists(src) and os.path.exists(dst):
        shutil.copy(src, dst)

def setJobStatus(message):
    if jobID != '0':
        jobReport = JobReport(int(jobID), 'BossScript')
        jobReport.setApplicationStatus(message)

def setRantrgSEJobStatus():
    rantrgLogFile.seek(0)
    rantrgOutput = rantrgLogFile.read()
    rantrgSe = 'unknown'
    m = re.search('^Determine SE: (.*)$', rantrgOutput, re.M)
    if m:
        rantrgSe = m.group(1)
    setJobStatus('Random Trigger Downloaded from: %s' % rantrgSe)

def uploadData(lfn, se):
    path = os.path.basename(lfn)
    result = dirac.addFile(lfn, path, se)
    for i in range(0, 5):
        if result['OK'] and result['Value']['Successful'] and result['Value']['Successful'].has_key(lfn):
            break
        time.sleep(random.randint(180, 600))
        print '- Upload to %s on SE %s failed, try again' % (lfn, se)
    if result['OK']:
        if result['Value']['Successful'] and result['Value']['Successful'].has_key(lfn):
            print >>logFile, 'Successfully uploading %s to %s. Retry %s' % (lfn, se, i+1)
            return result
        else:
            print >>errFile, 'Failed type 2 uploading %s to %s. Retry %s' % (lfn, se, i+1)
            return S_ERROR('Upload to %s on SE %s failed' % (lfn, se))
    else:
        print >>errFile, 'Failed type 1 uploading %s to %s. Retry %s' % (lfn, se, i+1)
        return result

def removeData(lfn):
    result = dirac.removeFile(lfn)
    for i in range(0, 16):
        if result['OK'] and result['Value']['Successful'] and result['Value']['Successful'].has_key(lfn):
            break
        time.sleep(random.randint(60, 300))
        print '- Remove %s failed, try again' % lfn
    if result['OK']:
        if result['Value']['Successful'] and result['Value']['Successful'].has_key(lfn):
            print >>logFile, 'Successfully remove %s. Retry %s' % (lfn, i+1)
            return result
        else:
            print >>errFile, 'Failed type 2 remove %s. Retry %s' % (lfn, i+1)
            return S_ERROR('Remove %s failed' % lfn)
    else:
        print >>errFile, 'Failed type 1 remove %s. Retry %s' % (lfn, i+1)
        return result

def registerMetadata(lfn, metadata):
    for i in range(0, 16):
        result = fcc.setMetadata(lfn, metadata)
        if result['OK']:
            break
        time.sleep(random.randint(30, 120))
        print '- Register metadata for %s failed, try again' % lfn
    if not result['OK']:
        print >>errFile, 'Failed to register metadata for %s. Retry %s' % (lfn, i+1)
        return S_ERROR('Register metadata for %s failed' % lfn)
    print >>logFile, 'Successfully register metadata for %s. Retry %s' % (lfn, i+1)
    return S_OK({lfn: metadata})

def uploadLog(loglfn, se):
    path = os.path.basename(loglfn)

    logdir = 'log_' + jobID
    if not os.path.exists(logdir):
        os.mkdir(logdir)
    cp('bosslog', logdir)
    cp('bosserr', logdir)
    cp('recbosslog', logdir)
    cp('recbosslog', logdir)
    cp('script.log', logdir)
    cp('script.err', logdir)
    cp('rantrg.log', logdir)
    cp('rantrg.err', logdir)

    # tar path logdir
    import tarfile
    tgzfile = path
    tf = tarfile.open(tgzfile,"w:gz")
    tf.add(logdir)
    tf.close()

    result = uploadData(loglfn, se)
    if not result['OK']:
        return result

    metadata = {'jobId': jobID}
    result = registerMetadata(loglfn, metadata)

    return result

def disableWatchdog():
    if not os.path.exists('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK'):
        open('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK', 'w').close()
        delDisableWatchdog = True

def enableWatchdog():
    if delDisableWatchdog:
        shutil.rmtree('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK', True)
        delDisableWatchdog = False

def bossjob():
    # download random trigger files simultaneously
    if os.path.exists('recoptions.pkl'):
        # download random trigger files
        setJobStatus('Downloading Random Trigger')
        disableWatchdog()
        result = cmd(['bash', './gaudi_run.sh', bossVer])
        pd = Popen(['besdirac-dms-rantrg-get', '-j', 'options_rantrg.opts'], stdout=rantrgLogFile, stderr=rantrgErrFile)

    # run sim
    setJobStatus('Simulation')
    result = cmd(['bash', './boss_run.sh', bossVer])
    if result:
        setJobStatus('Simulation Error: %s' % result)
        return result

    if os.path.exists('recoptions.pkl'):
        setJobStatus('Waiting for Random Trigger')

        # download random trigger files over
        result = pd.wait()
        setRantrgSEJobStatus()
        if result:
            setJobStatus('Download Random Trigger Error: %s' % result)
            return result

        # run rec
        setJobStatus('Reconstruction')
        result = cmd(['bash', './boss_run.sh', bossVer, 'rec'])
        if result:
            setJobStatus('Reconstruction Error: %s' % result)
            return result

    # upload file and reg
    setJobStatus('Uploading Data')
    result = uploadData(lfn, se)
    if not result['OK']:
        setJobStatus('Upload Data Error')
        print >>errFile, 'Upload Data Error:\\n%s' % result
        return 72

    setJobStatus('Setting Metadata')
    metadata = {'jobId':       jobID,
                'eventNumber': eventNumber,
                'runL':        runL,
                'runH':        runH,
                'count':       1,
               }
    result = registerMetadata(lfn, metadata)
    if not result['OK']:
        setJobStatus('Set Metadata Error')
        print >>errFile, 'Set Metadata Error:\\n%s' % result
        removeData(lfn)
        return 73

    setJobStatus('Boss Job Finished Successfully')
    return 0


if __name__ == '__main__':
    sys.stdout = logFile
    sys.stderr = errFile

    # prepare
    cmd(['date'])
    cmd(['uname', '-a'])
    cmd(['ls', '-ltrA'])

    # before boss
    if os.path.exists('before_boss_job'):
        cmd(['./before_boss_job'])

    # main job process
    exitStatus = bossjob()

    # after boss
    if os.path.exists('after_boss_job'):
        cmd(['./after_boss_job'])

    # some ending job
    cmd(['ls', '-ltrA'])
    cmd(['date'])

    # upload log and reg
    setJobStatus('Uploading Log')
    result = uploadLog(loglfn, 'IHEPD-USER')
    if not result['OK']:
        setJobStatus('Upload Log Error')

    sys.exit(exitStatus)
"""

    return script_head + script_body

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
def boss_env_wrapper():   
    return """#!/bin/bash
exec 1>bosslog
exec 2>bosserr

touch DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK

echo -e -n 'export BESROOT=/cvmfs/boss.cern.ch
cd $BESROOT/slc5_amd64_gcc43/###BOSS_VERSION###/
pwd
ls
cat setup.sh
source setup.sh
source scripts/###BOSS_VERSION###/setup.sh
source dist/6.6.4.p01/TestRelease/*/cmt/setup.sh
cd $OLDPWD
' > bossenv.sh

echo "Node name: `uname -n`"
date
dirac-proxy-info

ls -al

./job-setappstatus -m 'Start Simulation'

cat /cvmfs/boss.cern.ch/slc5_amd64_gcc43/###BOSS_VERSION###/setup.sh

bash -c 'source bossenv.sh
gaudirun.py -n -v -o options.opts options.pkl data.py
boss.exe options.opts
result=$?
if [ $result != 0 ]; then 
   echo "ERROR: boss.exe on simulation job failed"
   exit $result
fi
gaudirun.py -n -o options.py options.pkl data.py
rawfile=`python -c "print eval(open('\\''options.py'\\'').read())['\\''RootCnvSvc'\\'']['\\''digiRootOutputFile'\\'']"`
if [ ! -f $rawfile ]; then
   echo "ERROR: $rawfile not generated"
   exit 2
fi
'

date

if [ -f "recoptions.pkl" ]; then
   ./job-setappstatus -m 'Start Random Trigger Downloading'
   
   ls -Al
   touch DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK
#   besdirac-dms-rantrg-get -j options.opts
   ls -Al
#   rm -f DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK

   ./job-setappstatus -m 'Start Reconstruction'

   bash -c 'source bossenv.sh
gaudirun.py -n -v -o recoptions.opts recoptions.pkl recdata.py
boss.exe recoptions.opts
result=$?
if [ $result != 0 ]; then
   echo "ERROR: boss.exe on reconstruction job failed"
   exit $result
fi
'

   ./job-setappstatus -m 'Reconstruction Successfully'
fi

date
"""

class GaudiDiracRTHandler(IRuntimeHandler):
    """The runtime handler to run Gaudi jobs on the Dirac backend"""

    def __init__(self):
        self._allRound = []

    def master_prepare(self,app,appconfig):
        app.extra.master_input_buffers['gaudi_run.sh'] = gaudi_run_wrapper()
        app.extra.master_input_buffers['boss_run.sh'] = boss_run_wrapper()
        sandbox = get_master_input_sandbox(app.getJobObject(),app.extra) 
        c = StandardJobConfig('',sandbox,[],[],None)
        return c

    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        if not app.extra.metadata['round'] in self._allRound:
            bdr = BDRegister(app.extra.metadata)
            dfcDir = bdr.createFileDir()
            bdr.createLogDir()
            app.add_output_dir(dfcDir)
            if app.createDataset:
                dataset_name = bdr.createDataset()
                app.add_dataset_name(dataset_name)
            self._allRound.append(app.extra.metadata['round'])

        # some extra lines for simulation job options on DIRAC site
        opts = 'DatabaseSvc.DbType = "sqlite";\n'
        opts += 'DatabaseSvc.SqliteDbPath = "/cvmfs/boss.cern.ch/slc5_amd64_gcc43/%s/database";\n' % app.version
        if app.version in ['6.6.2', '6.6.3.p01']:
            opts += 'MdcTunningSvc.Host = "202.122.33.120";'
        app.extra.input_buffers['data.opts'] += opts

        # extra lines for reconstruction and remove empty files
        if app.recoptsfile:
            app.extra.input_buffers['recdata.opts'] += opts
            opts = 'MixerAlg.UseNewDataDir = ".";\n'
            app.extra.input_buffers['recdata.opts'] += opts
        else:
            app.extra.input_buffers.pop('recdata.opts', None)
            app.extra.input_buffers.pop('recdata.py', None)


        if app.extra.inputdata and app.extra.inputdata.hasLFNs():        
            cat_opts = '\nFileCatalog().Catalogs = ' \
                       '["xmlcatalog_file:pool_xml_catalog.xml"]\n'
            app.extra.input_buffers['data.py'] += cat_opts

        #script = self._create_gaudi_script(app) # comment out by zhangxm
        script = self._create_boss_script(app)
#        script = self._create_bossold_script(app)
        sandbox = get_input_sandbox(app.extra)
        app.extra.outputsandbox += ['script.log', 'script.err', 'rantrg.log', 'rantrg.err', 'bosslog', 'bosserr', 'recbosslog', 'recbosserr']
        outputsandbox = app.extra.outputsandbox 
        c = StandardJobConfig(script,sandbox,[],outputsandbox,None)

        dirac_script = DiracScript()
        dirac_script.job_type = 'Job()'
        dirac_script.exe = DiracApplication(app,script)
        dirac_script.platform = app.platform
        dirac_script.output_sandbox = outputsandbox

        if app.extra.inputdata:
            dirac_script.inputdata = DiracInputData(app.extra.inputdata)
          
        if app.extra.outputdata:
            dirac_script.outputdata = app.extra.outputdata

        c.script = dirac_script
        return c

    def _create_gaudi_script(self,app):
        '''Creates the script that will be executed by DIRAC job. '''
        commandline = "'python ./gaudipython-wrapper.py'"
        if is_gaudi_child(app):
            commandline = "'boss.exe  jobOptions_sim.txt'"
        logger.debug('Command line: %s: ', commandline)
        wrapper = gaudi_dirac_wrapper(commandline)
        #logger.error("zhangxm log: gaudi-script.py: %s" % wrapper)
        j = app.getJobObject()
        script = "%s/gaudi-script.py" % j.getInputWorkspace().getPath()
        file = open(script,'w')
        file.write(wrapper)
        file.close()
        os.system('chmod +x %s' % script)
        return script

    def _create_boss_script(self,app):  
        '''Creates the script that will set the Boss environment on grid'''
        bdr = BDRegister(app.extra.metadata)
        app.extra.outputdata.location = bdr.getFileDirName()
        lfn = os.path.join(app.extra.outputdata.location, app.outputfile)
        loglfn = os.path.join(bdr.getLogDirName(), app.outputfile + '.log.tar.gz')
        wrapper = boss_script_wrapper(app.version, lfn, loglfn, eval(getConfig('Boss')['DiracOutputDataSE'])[0],
                                      app.eventNumber, app.runL, app.runH)
        j = app.getJobObject()
        script = os.path.join(j.getInputWorkspace().getPath(), "boss_dirac.py")
        file=open(script,'w')
        file.write(wrapper)
        file.close()
        os.system('chmod +x %s' % script)
        return script

    def _create_bossold_script(self,app):  
        '''Creates the script that will set the Boss environment on grid'''
        wrapper = boss_env_wrapper()
        wrapper=wrapper.replace('###BOSS_VERSION###',app.version)
        j = app.getJobObject()
        script = "%s/bossenv_dirac.sh" % j.getInputWorkspace().getPath()
        file=open(script,'w')
        file.write(wrapper)
        file.close()
        os.system('chmod +x %s' % script)
        return script

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
