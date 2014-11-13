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
extrapy=$3

besRoot=/cvmfs/boss.cern.ch
cd ${besRoot}/slc5_amd64_gcc43/${bossVer}
source setup.sh
source scripts/${bossVer}/setup.sh
source dist/${bossVer}/TestRelease/*/cmt/setup.sh
cd $OLDPWD

export LD_LIBRARY_PATH=`pwd`:`pwd`/custom_so_1:`pwd`/custom_so_2:`pwd`/custom_so_3:$LD_LIBRARY_PATH

gaudirun.py -n -v -o ${prefix}options.opts ${prefix}options.pkl ${prefix}data.py ${extrapy}
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

def boss_script_wrapper(bossVer, lfn, loglfn, se, eventNumber, runL, runH, useLocalRantrg):
    script_head = """#!/usr/bin/env python

import os, sys, shutil, re, time, random, socket
from subprocess import Popen, call

import DIRAC
from DIRAC import S_OK, S_ERROR, gConfig

from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = False )

from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport

from DIRAC.Interfaces.API.Dirac import Dirac
dirac = Dirac()

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
fccType = 'DataManagement/FileCatalog'
fcc = FileCatalogClient(fccType)

from DIRAC.Core.DISET.RPCClient                      import RPCClient

doReconstruction = os.path.exists('recoptions.pkl')
doAnalysis = os.path.exists('anaoptions.pkl')
delDisableWatchdog = False

logFile = open('script.log', 'w')
errFile = open('script.err', 'w')
rantrgLogFile = open('rantrg.log', 'w+')
rantrgErrFile = open('rantrg.err', 'w')

jobID = os.environ.get('DIRACJOBID', '0')
siteName = DIRAC.siteName()
hostname = socket.gethostname()

logJobInfo = False

bossVer = '%s'
lfn = '%s'
loglfn = '%s'
se = '%s'
eventNumber = %s
runL = %s
runH = %s
useLocalRantrg = %s

""" % (bossVer, lfn, loglfn, se, eventNumber, runL, runH, useLocalRantrg)

    script_body = """
def getRantrgInfo():
    roundNum = ''
    dateDir = ''
    filelist = []

    import sqlite3
    sql3File = '/cvmfs/boss.cern.ch/slc5_amd64_gcc43/%s/database/offlinedb.db' % bossVer
    try:
        conn = sqlite3.connect(sql3File)
        c = conn.cursor()
        c.execute("SELECT RunNo,FilePath,FileName FROM RanTrgData WHERE RunNo>=? AND RunNo<=?", (runL, runH))
        result = c.fetchall()
        conn.commit()
        conn.close()
    except sqlite3.Error as e:
        print >>errFile, 'Open sqlite3 file "%s" error: %s' % (sql3File, e)
        return roundNum, dateDir, filelist

    if result:
        # parse round number and date from directory name
        filePath = result[0][1]
        roundStart = filePath.rfind('round')
        if roundStart >= 0:
            roundNum = filePath[roundStart:]
            roundEnd = roundNum.find('/')
            if roundEnd >= 0:
                dateDir = roundNum[roundEnd:]
                roundNum = roundNum[:roundEnd]
                dateEnd = dateDir.find('/')
                if dateEnd >= 0:
                    dateDir = dateDir[:dateEnd]

        for line in result:
            filelist.append(line[2])

    return roundNum, dateDir, filelist

def getRantrgRoundInfo(roundNum):
    rantrgMethod = ''
    rantrgRoundPaths = []

    diracGridType, place, country = siteName.split('.')

    # search for available local random trigger data from the configuration server
    rantrgAvailable = gConfig.getValue('/Resources/Sites/%s/%s/Data/LocalRantrg/Available'%(diracGridType, siteName), [])
    if roundNum not in rantrgAvailable:
        print >>errFile, 'Local random trigger file not found: Round %s not available in configuration' % roundNum
        return rantrgMethod, rantrgRoundPaths

    # find if this round is configured individually
    result = gConfig.getSections('/Resources/Sites/%s/%s/Data/LocalRantrg'%(diracGridType, siteName))
    if not result['OK']:
        print >>errFile, result['Message']
        return rantrgMethod, rantrgRoundPaths
    individualRounds = result['Value']

    if roundNum in individualRounds:
        rantrgMethod = gConfig.getValue('/Resources/Sites/%s/%s/Data/LocalRantrg/%s/Method'%(diracGridType, siteName, roundNum), '')
        if rantrgMethod in ['New', 'Replace']:
            rantrgRoundPaths = gConfig.getValue('/Resources/Sites/%s/%s/Data/LocalRantrg/%s/Paths'%(diracGridType, siteName, roundNum), [])
    else:
        rantrgMethod = gConfig.getValue('/Resources/Sites/%s/%s/Data/LocalRantrg/Method'%(diracGridType, siteName), '')
        if rantrgMethod in ['New', 'Replace']:
            rantrgMainPath = gConfig.getValue('/Resources/Sites/%s/%s/Data/LocalRantrg/Path'%(diracGridType, siteName), '')
            if rantrgMainPath:
                rantrgRoundPaths = [os.path.join(rantrgMainPath, roundNum)]

    if rantrgMethod in ['New', 'Replace'] and not rantrgRoundPaths:
        print >>errFile, 'Local random trigger file not found: Round %s path not found in configuration' % roundNum

    return rantrgMethod, rantrgRoundPaths

def validateLocalRantrgPath(rantrgRoundPaths, dateDir, filelist):
    rantrgPath = ''

    for rantrgRoundPath in rantrgRoundPaths:
        if os.path.exists(os.path.join(rantrgRoundPath, filelist[0])):
            rantrgPath = rantrgRoundPath
            break
        if dateDir and os.path.exists(os.path.join(rantrgRoundPath, dateDir, filelist[0])):
            rantrgPath = os.path.join(rantrgRoundPath, dateDir)
            break

    if not rantrgPath:
        print >>errFile, 'Local random trigger file not in regular path: Try to find in %s' % rantrgRoundPaths

        for rantrgRoundPath in rantrgRoundPaths:
            for root,subdirs,files in os.walk(rantrgRoundPath):
                if filelist[0] in files:
                    rantrgPath = root
                    break
            if rantrgPath:
                break

    if not rantrgPath:
        print >>errFile, 'Local random trigger file not found: Random trigger file not found anywhere'

    return rantrgPath

def getLocalRantrgPath():
    roundNum, dateDir, filelist = getRantrgInfo()
    if not roundNum:
        print >>errFile, 'Local random trigger file not found: Run %s not in the database' % runL
        return ''

    rantrgMethod, rantrgRoundPaths = getRantrgRoundInfo(roundNum)

    rantrgPath = ''
    if rantrgMethod == 'New':
        rantrgPath = validateLocalRantrgPath(rantrgRoundPaths, dateDir, filelist)

    return rantrgMethod, rantrgPath

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

def launchPatch():
    for f in os.listdir('.'):
        if os.path.isfile(f) and f.endswith('.patch_for_gangaboss'):
            os.chmod(f, 0755)
            cmd([os.path.join('.', f)])

def setJobInfo(message):
    if logJobInfo and jobID != '0':
        info = RPCClient( 'Info/BadgerInfo' )
        result = info.addJobLog( int(jobID), siteName, hostname, eventNumber, message )
        if not result['OK']:
            print >>errFile, 'setJobInfo error: %s' % result

def setJobStatus(message):
    if jobID != '0':
        jobReport = JobReport(int(jobID), 'BossScript')
        result = jobReport.setApplicationStatus(message)
        if not result['OK']:
            print >>errFile, 'setJobStatus error: %s' % result

def setRantrgSEJobStatus():
    rantrgLogFile.seek(0)
    rantrgOutput = rantrgLogFile.read()
    m = re.search('^Determine SE: (.*)$', rantrgOutput, re.M)
    if m:
        rantrgSe = m.group(1)
    if not rantrgSe:
        rantrgSe = 'unknown'
    setJobStatus('Random Trigger Downloaded from: %s' % rantrgSe)

def uploadData(lfn, se):
    removeData(lfn)

    path = os.path.basename(lfn)
    for i in range(0, 5):
        result = dirac.addFile(lfn, path, se)
        if result['OK'] and result['Value']['Successful'] and result['Value']['Successful'].has_key(lfn):
            break
        time.sleep(random.randint(180, 600))
        print >>errFile, '- Upload to %s on SE %s failed, try again' % (lfn, se)
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
    result = fcc.isFile(lfn)
    if not (result['OK'] and lfn in result['Value']['Successful'] and result['Value']['Successful'][lfn]):
        return result

    for i in range(0, 16):
        result = dirac.removeFile(lfn)
        if result['OK'] and result['Value']['Successful'] and result['Value']['Successful'].has_key(lfn):
            break
        time.sleep(random.randint(6, 30))
        print >>errFile, '- Remove %s failed, try again' % lfn
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
        time.sleep(random.randint(6, 30))
        print >>errFile, '- Register metadata for %s failed, try again' % lfn
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
    cp('recbosserr', logdir)
    cp('anabosslog', logdir)
    cp('anabosserr', logdir)
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

def startRantrgDownload():
    # download random trigger files
    setJobStatus('Downloading Random Trigger')
    disableWatchdog()
    result = cmd(['./gaudi_run.sh', bossVer])
    return Popen(['besdirac-dms-rantrg-get', '-j', 'options_rantrg.opts'], stdout=rantrgLogFile, stderr=rantrgErrFile)

def startSimulation():
    setJobStatus('Simulation')

    startcmd = '%s\\n%s  Start Executing: %s' % ('='*80, '>'*16, ['./boss_run.sh', bossVer])
    print >>logFile, startcmd
    print >>errFile, startcmd
    logFile.flush()
    errFile.flush()

    return Popen(['./boss_run.sh', bossVer], stdout=logFile, stderr=errFile)

def endSimulation():
    endcmd = '%s  End Executing: %s\\n%s\\n' % ('<'*16, ['./boss_run.sh', bossVer], '='*80)
    print >>logFile, endcmd
    print >>errFile, endcmd
    logFile.flush()
    errFile.flush()

#    setJobStatus('Simulation Finished')

def generateLocalRantrgOpt(replacePath='', newPath=''):
    extraOptFile = open('extra.opts', 'w')
    extraOptFile.write('MixerAlg.ReplaceDataPath = "%s";\\n' % replacePath)
    extraOptFile.write('MixerAlg.UseNewDataDir = "%s";\\n' % newPath)
    extraOptFile.close()

def checkRantrgDownloadStatus():
    pass

def checkEnvironment():
    try:
        os.listdir('/cvmfs/boss.cern.ch')
    except OSError as e:
        print >>errFile, 'List directory "/cvmfs/boss.cern.ch" failed: %s' % e

    if not os.path.isdir('/cvmfs/boss.cern.ch'):
        setJobStatus('BOSS cvmfs not found')
        return False

    return True

def bossjob():
    disableWatchdog()

    setJobInfo('Start Job')

    # prepare for reconstruction
    rantrgMethod = ''
    localRantrgPath = ''
    if doReconstruction:
        if useLocalRantrg:
            rantrgMethod, localRantrgPath = getLocalRantrgPath()

            print >>logFile, 'rantrgMethod: %s' % rantrgMethod
            print >>logFile, 'localRantrgPath: %s' % localRantrgPath

            if rantrgMethod == 'New':
                print >>logFile, 'Use local random trigger path: %s' % localRantrgPath
                cmd(['ls', '-ld', localRantrgPath])
                generateLocalRantrgOpt(newPath=localRantrgPath)
            elif rantrgMethod == 'Replace':
                print >>logFile, 'Replace local random trigger path: %s' % localRantrgPath
                generateLocalRantrgOpt(replacePath=localRantrgPath)
            elif rantrgMethod == 'Default':
                print >>logFile, 'Use local random trigger with default path'
                generateLocalRantrgOpt()

        if not rantrgMethod:
            if runH > runL:
                print >>errFile, 'Too many runs to download random trigger file. %s - %s' % (runL, runH)
                setJobStatus('Can not do reconstruction on this site with split by event')
                return 71
            setJobInfo('Start Downloading Random Trigger')
            pdRantrg = startRantrgDownload()

    # run simulation
    setJobInfo('Start Simulation')
    pdSimulation = startSimulation()
    simRunning = True

    # 0: not started, 1: running, 2: finished ok, 3: finished with errors
    rantrgRunning = False

    retCode = 0

    if not doReconstruction or rantrgMethod:
        # only monitor simulation
        simRetCode = pdSimulation.wait()
        simRunning = False
        endSimulation()
        if simRetCode:
            setJobStatus('Simulation Error: %s' % simRetCode)
            setJobInfo('End Simulation with Error')
            retCode = simRetCode
        else:
            setJobStatus('Simulation Finished Successfully')
            setJobInfo('End Simulation')
    else:
        # monitor simulation and rantrg downloading
        rantrgRunning = True
        while True:
            simRetCode = pdSimulation.poll()
            rantrgRetCode = pdRantrg.poll()

            if simRunning and simRetCode != None:
                simRunning = False
                endSimulation()
                if simRetCode:
                    setJobStatus('Simulation Error: %s' % simRetCode)
                    setJobInfo('End Simulation with Error')
                    retCode = simRetCode
                    break
                else:
                    setJobStatus('Simulation Finished Successfully')
                    setJobInfo('End Simulation')
                    if rantrgRunning:
                        setJobStatus('Waiting for Random Trigger')

            if rantrgRunning and rantrgRetCode != None:
                rantrgRunning = False
                if rantrgRetCode:
                    setJobStatus('Download Random Trigger Error: %s' % rantrgRetCode)
                    setJobInfo('End Download Random Trigger with Error')
                    retCode = rantrgRetCode
                    break
                else:
                    setRantrgSEJobStatus()
#                    setJobStatus('Download Random Trigger Successfully')
                    setJobInfo('End Downloading Random Trigger')
                    if simRunning:
                        setJobStatus('Waiting for Simulation')

            if rantrgRetCode == None:
                checkRantrgDownloadStatus()

            if not (simRunning or rantrgRunning):
                break

            time.sleep(5)

    # teminate running process
    if simRunning:
        setJobStatus('Terminate Simulation')
        setJobInfo('End Simulation by Terminate')
        pdSimulation.terminate()
        simRunning = False
        endSimulation()
    if rantrgRunning:
        setJobStatus('Terminate Random Trigger Downloading')
        setJobInfo('End Random Trigger Downloading by Terminate')
        pdRantrg.terminate()
        rantrgRunning = False

    if retCode:
        setJobInfo('End Job with Error: %s' % retCode)
        return retCode

    # reconstruction
    if doReconstruction:
        # run rec
        setJobStatus('Reconstruction')
        setJobInfo('Start Reconstruction')
        if rantrgMethod:
            result = cmd(['./boss_run.sh', bossVer, 'rec', 'extra.opts'])
        else:
            result = cmd(['./boss_run.sh', bossVer, 'rec'])

        if result:
            setJobStatus('Reconstruction Error: %s' % result)
            setJobInfo('End Reconstruction with Error')
            setJobInfo('End Job with Error: %s' % result)
            return result

        setJobInfo('End Reconstruction')

    # analysis
    if doAnalysis:
        # run ana
        setJobStatus('Analysis')
        setJobInfo('Start Analysis')
        result = cmd(['./boss_run.sh', bossVer, 'ana'])

        if result:
            setJobStatus('Analysis Error: %s' % result)
            setJobInfo('End Analysis with Error')
            setJobInfo('End Job with Error: %s' % result)
            return result

        setJobInfo('End Analysis')


    # upload file and reg
    setJobStatus('Uploading Data')
    setJobInfo('Start Uploading Data')
    result = uploadData(lfn, se)
    if not result['OK']:
        print >>errFile, 'Upload Data Error:\\n%s' % result
        setJobStatus('Upload Data Error')
        setJobInfo('End Uploading Data with Error')
        setJobInfo('End Job with Error: %s' % 72)
        return 72
    setJobInfo('End Uploading Data')

    setJobStatus('Setting Metadata')
    setJobInfo('Start Setting Metadata')
    metadata = {'jobId':       jobID,
                'eventNumber': eventNumber,
                'runL':        runL,
                'runH':        runH,
                'count':       1,
               }
    result = registerMetadata(lfn, metadata)
    if not result['OK']:
        print >>errFile, 'Set Metadata Error:\\n%s' % result
        setJobStatus('Setting Metadata Error')
        setJobInfo('End Setting Metadata with Error')
        removeData(lfn)
        setJobInfo('End Job with Error: %s' % 73)
        return 73
    setJobInfo('End Set Metadata')

    setJobStatus('Boss Job Finished Successfully')
    setJobInfo('End Job')
    return 0


if __name__ == '__main__':
    sys.stdout = logFile
    sys.stderr = errFile

    # file executable
    os.chmod('boss_run.sh', 0755)
    os.chmod('gaudi_run.sh', 0755)

    # prepare
    cmd(['date'])
    cmd(['uname', '-a'])
    cmd(['ls', '-ltrA'])

    # patch
    launchPatch()

    # before boss
    if os.path.exists('before_boss_job') and os.path.isfile('before_boss_job'):
        os.chmod('before_boss_job', 0755)
        cmd(['./before_boss_job'])

    # check environment
    if checkEnvironment():
        # main job process
        exitStatus = bossjob()
    else:
        exitStatus = 70

    # after boss
    if os.path.exists('after_boss_job') and os.path.isfile('after_boss_job'):
        os.chmod('after_boss_job', 0755)
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

        self._boss_patch(app)
        self._create_patch_script(app)

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
        app.extra.input_buffers['data.opts'] += opts

        # extra lines for reconstruction and remove empty files
        if app.recoptsfile:
            app.extra.input_buffers['recdata.opts'] += opts
            extraopts = 'MixerAlg.UseNewDataDir = ".";\n'
            app.extra.input_buffers['recdata.opts'] += extraopts
        else:
            app.extra.input_buffers.pop('recdata.opts', None)
            app.extra.input_buffers.pop('recdata.py', None)

        # extra lines for analysis and remove empty files
        if app.anaoptsfile:
            app.extra.input_buffers['anadata.opts'] += opts
            app.extra.input_buffers['anadata.opts'] += opts
        else:
            app.extra.input_buffers.pop('anadata.opts', None)
            app.extra.input_buffers.pop('anadata.py', None)

        if app.extra.inputdata and app.extra.inputdata.hasLFNs():        
            cat_opts = '\nFileCatalog().Catalogs = ' \
                       '["xmlcatalog_file:pool_xml_catalog.xml"]\n'
            app.extra.input_buffers['data.py'] += cat_opts

        #script = self._create_gaudi_script(app) # comment out by zhangxm
        script = self._create_boss_script(app)
#        script = self._create_bossold_script(app)
        sandbox = get_input_sandbox(app.extra)
        app.extra.outputsandbox += ['script.log', 'script.err', 'rantrg.log', 'rantrg.err', 'bosslog', 'bosserr', 'recbosslog', 'recbosserr', 'anabosslog', 'anabosserr']
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

    def _boss_patch(self,app):
        if app.useBossPatch:
            boss_patch = Ganga.Utility.Config.getConfig('Boss')['BossPatch']
            if os.path.exists(boss_patch) and os.path.isfile(boss_patch):
                try:
                    f = open(boss_patch)
                    boss_conf = eval(f.read())
                    f.close()
                except Exception as e:
                    raise ApplicationConfigurationError(None, 'Invalid configuration file: %s. Error: %s' % (boss_patch, e))

                if app.version in boss_conf['Boss']:
                    logger.debug("Patch for Boss version %s: %s" % (app.version, boss_conf['Boss'][app.version]))
                    app.patch += boss_conf['Boss'][app.version]
            else:
                raise ApplicationConfigurationError(None, 'Cannot find the Boss patch configuration file: %s' % boss_patch)

    def _create_patch_script(self,app):
        patch_path = Ganga.Utility.Config.getConfig('Boss')['PatchPath']
        for pf in app.patch:
            local_patch_file = pf+'.patch'
            system_patch_file = os.path.join(patch_path, local_patch_file)
            if os.path.exists(local_patch_file) and os.path.isfile(local_patch_file):
                patch_file = local_patch_file
            elif os.path.exists(system_patch_file) and os.path.isfile(system_patch_file):
                patch_file = os.path.join(patch_path, local_patch_file)
            else:
                raise ApplicationConfigurationError(None, 'Cannot find the patch file: %s' % system_patch_file)

            f = open(patch_file)
            app.extra.master_input_buffers[pf+'.patch_for_gangaboss'] = f.read()
            f.close()

    def _create_gaudi_script(self,app):
        '''Creates the script that will be executed by DIRAC job. '''
        commandline = "'python ./gaudipython-wrapper.py'"
        if is_gaudi_child(app):
            commandline = "'boss.exe jobOptions_sim.txt'"
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
                                      app.eventNumber, app.runL, app.runH, app.localRantrg)
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
