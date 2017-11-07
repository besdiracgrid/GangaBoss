#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from Ganga.Utility.Config import getConfig
from DiracUtils import *
from DiracScript import *
from GangaBoss.Lib.Gaudi.RTHUtils import *
from GangaBoss.Lib.Dataset.BDRegister import BDRegister
from GangaBoss.Lib.Dataset.BDRegister import DfcOperation
from GangaBoss.Lib.DIRAC.DiracTask import gDiracTask
from Ganga.GPIDev.Lib.File import FileBuffer, File
from Ganga.Utility.Shell import Shell

from DIRAC import gConfig

from DIRAC.Core.Base import Script
Script.initialize()

from DIRAC.Interfaces.API.Dirac import Dirac
dirac = Dirac()

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
fcc = FileCatalogClient('DataManagement/FileCatalog')

from DIRAC.Core.Security.ProxyInfo                        import getProxyInfo

import shutil
import tarfile
import hashlib

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def boss_run_wrapper():
    return """#!/bin/bash

bossRepo=$1
bossVer=$2
prefix=$3
extraopts=$4

maxlogsize=$((32*1024*1024))

besRoot="/cvmfs/${bossRepo}"
cd ${besRoot}/*/${bossVer}
source setup.sh
source scripts/${bossVer}/setup.sh
source dist/${bossVer}/TestRelease/*/cmt/setup.sh
cd $OLDPWD

export LD_LIBRARY_PATH=`pwd`:`pwd`/custom_so_1:`pwd`/custom_so_2:`pwd`/custom_so_3:$LD_LIBRARY_PATH

echo "DatabaseSvc.SqliteDbPath = \\"/cvmfs/${bossRepo}/database\\";" >> ${prefix}data.opts

gaudirun.py -n -v -o ${prefix}final.opts ${prefix}options.opts ${prefix}data.opts ${extraopts}
(time boss.exe ${prefix}final.opts) 1> >(tail -c ${maxlogsize} > ${prefix}bosslog) 2> >(tail -c ${maxlogsize} > ${prefix}bosserr)
result=$?
if [ $result != 0 ]; then
    echo "ERROR: boss.exe ${prefix}final.opts failed with code $result" >&2
    exit $result
fi

sync
sleep 5

if ! ( grep -q 'Application Manager Finalized successfully' ${prefix}bosslog && grep -q 'INFO Application Manager Terminated successfully' ${prefix}bosslog ); then
    echo "ERROR: boss.exe ${prefix}final.opts does not finished successfully" >&2
    exit 2
fi
"""

def rantrg_get_wrapper():
    return """#!/bin/bash

dst=$(pwd)
max_retry=5
min_wait=60
max_wait=300

download() {
    v=$1

    for (( i=1; i <= $max_retry; ++i ))
    do
        fn=$(basename $v)
        echo "---- Start at $(date -u '+%Y-%m-%d %H:%M:%S.%N %Z')"
        (time globus-url-copy -sync ${v} file://$dst/$fn)
        result=$?
        echo "---- Finish at $(date -u '+%Y-%m-%d %H:%M:%S.%N %Z')"
        if [ $result = 0 ]; then
            break
        fi

        echo "---- Download (${i}) failed with exit code ${result}"
        echo ''

        if [ $i -ge $max_retry ]; then
            break
        fi

        echo "Wait for retry"
        sleep $((min_wait + RANDOM % (max_wait-min_wait)))
    done

    if [ $result = 0 ]; then
        echo "${v} downloaded successfully with ${i} attempt"
    else
        echo "ERROR: Download ${v} failed with code $result"
    fi
}

for var in "$@"
do
    echo '================================================================================' >>rantrg.log
    echo "Downloading ${var}" >>rantrg.log
    download $var >>rantrg.log 2>&1
    result=$?
    echo '' >>rantrg.log
    if [ $result != 0 ]; then
        exit $result
    fi
done
"""

def boss_script_wrapper(bossVer, lfns, loglfn, se, eventNumber, runL, runH, useLocalRantrg, autoDownload):
    script_head = """#!/usr/bin/env python

import os, sys, shutil, re, time, datetime, random, socket, tarfile
from subprocess import Popen, call, STDOUT

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

doReconstruction = os.path.exists('recoptions.opts')
doAnalysis = os.path.exists('anaoptions.opts')
delDisableWatchdog = False

logFile = open('script.log', 'w')

jobID = os.environ.get('DIRACJOBID', '0')
siteName = DIRAC.siteName()
hostname = socket.gethostname()

logJobInfo = False
bossRepo = 'boss.cern.ch'

bossVer = '%s'
lfns = %s
loglfn = '%s'
seInput = '%s'
eventNumber = %s
runL = %s
runH = %s
useLocalRantrg = %s
autoDownload = '%s'

if seInput.lower() == 'auto':
  bossUploadSEs = gConfig.getValue('/Resources/Applications/BossUpload/Data', [seInput])
else:
  bossUploadSEs = [seInput]
bossLogSE = gConfig.getValue('/Resources/Applications/BossUpload/Log', seInput)

""" % (bossVer, lfns, loglfn, se, eventNumber, runL, runH, useLocalRantrg, autoDownload)

    script_body = """
def getRantrgInfo():
    roundNum = ''
    dateDir = ''
    filelist = []

    import sqlite3
    sql3File = '/cvmfs/%s/database/offlinedb.db' % bossRepo
    try:
        conn = sqlite3.connect(sql3File)
        c = conn.cursor()
        c.execute("SELECT RunNo,FilePath,FileName FROM RanTrgData WHERE RunNo>=? AND RunNo<=?", (runL, runH))
        result = c.fetchall()
        conn.commit()
        conn.close()
    except sqlite3.Error as e:
        print 'Open sqlite3 file "%s" error: %s' % (sql3File, e)
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
                dateDir = dateDir.strip('/')

        for line in result:
            filelist.append(line[2])

    return roundNum, dateDir, filelist

def getRantrgRoundInfo(roundNum):
    rantrgMethod = ''
    rantrgRootPath = []

    configPrefix = '/Resources/Applications/RandomTrigger/Local'

    result = gConfig.getSections('%s/%s'%(configPrefix, siteName))
    if not result['OK']:
        print result['Message']
        return rantrgMethod, rantrgRootPath
    dataTypes = result['Value']

    dataTypeFound = ''
    for dt in dataTypes:
        # check if local rantrg is enabled
        rantrgEnabled = gConfig.getValue('%s/%s/%s/Enabled'%(configPrefix, siteName, dt), False)
        if not rantrgEnabled:
            continue

        rantrgAvailable = gConfig.getValue('%s/%s/%s/Available'%(configPrefix, siteName, dt), [])
        if roundNum not in rantrgAvailable:
            continue

        dataTypeFound = dt
        break

    if not dataTypeFound:
        print 'Local random trigger round %s: not available in configuration' % roundNum

    rantrgMethod = gConfig.getValue('%s/%s/%s/Method'%(configPrefix, siteName, dataTypeFound), '')
    rantrgMethod = rantrgMethod.lower()
    if rantrgMethod in ['new', 'replace']:
        rantrgRootPath = gConfig.getValue('%s/%s/%s/Path'%(configPrefix, siteName, dataTypeFound), '')
        if not rantrgRootPath:
            print 'Local random trigger round %s: path not found in configuration' % roundNum

    return rantrgMethod, rantrgRootPath

def validateLocalRantrgPath(rantrgRootPath, roundNum, dateDir, filelist):
    rantrgPath = ''

    testFilePath = os.path.join(rantrgRootPath, roundNum, filelist[0])
    if os.path.exists(testFilePath):
        rantrgPath = rantrgRootPath.rstrip('/')

    if not rantrgPath:
        print 'Local random trigger file not found. Test file %s not found' % testFilePath
        return ''

    return os.path.join(rantrgPath, roundNum)

def validateReplaceLocalRantrgPath(rantrgRootPath, roundNum, dateDir, filelist):
    rantrgPath = ''

    testFilePath = os.path.join(rantrgRootPath, roundNum, dateDir, filelist[0])
    if os.path.exists(testFilePath):
        rantrgPath = rantrgRootPath.rstrip('/') + '/'

    if not rantrgPath:
        print 'Local random trigger file not found. Test file %s not found' % testFilePath

    return rantrgPath

def getLocalRantrgPath():
    roundNum, dateDir, filelist = getRantrgInfo()
    print 'roundNum: "%s", dateDir: "%s"' % (roundNum, dateDir)
    if not roundNum:
        print 'Local random trigger file not found: Run %s not in the database' % runL
        return '', ''

    rantrgMethod, rantrgRootPath = getRantrgRoundInfo(roundNum)
    print 'rantrgMethod: %s, rantrgRootPath: %s' % (rantrgMethod, rantrgRootPath)

    rantrgPath = ''
    if rantrgMethod == 'new':
        rantrgPath = validateLocalRantrgPath(rantrgRootPath, roundNum, dateDir, filelist)
    elif rantrgMethod == 'replace':
        rantrgPath = validateReplaceLocalRantrgPath(rantrgRootPath, roundNum, dateDir, filelist)

    if not rantrgPath:
        rantrgMethod = ''

    return rantrgMethod, rantrgPath


def getRemoteRantrgPaths():
    roundNum = ''
    rantrgFilePaths = []

    import sqlite3
    sql3File = '/cvmfs/%s/database/offlinedb.db' % bossRepo
    try:
        conn = sqlite3.connect(sql3File)
        c = conn.cursor()
        c.execute("SELECT RunNo,FilePath,FileName FROM RanTrgData WHERE RunNo>=? AND RunNo<=?", (runL, runH))
        resultSqlite = c.fetchall()
        conn.commit()
        conn.close()
    except sqlite3.Error as e:
        print 'Open sqlite3 file "%s" error: %s' % (sql3File, e)
        return roundNum, dateDir, filelist

    if not resultSqlite:
        return rantrgFilePaths

    # parse round number and date from directory name
    filePath = resultSqlite[0][1]
    roundStart = filePath.rfind('round')
    if roundStart >= 0:
        roundNum = filePath[roundStart:]
        roundEnd = roundNum.find('/')
        if roundEnd >= 0:
            roundNum = roundNum[:roundEnd]


    configPrefix = '/Resources/Applications/RandomTrigger/Remote'

    result = gConfig.getSections(configPrefix)
    if not result['OK']:
        print result['Message']
        return rantrgFilePaths
    dataTypes = result['Value']

    dataTypeFound = ''
    for dt in dataTypes:
        rantrgAvailable = gConfig.getValue('%s/%s/Available'%(configPrefix, dt), [])
        if roundNum not in rantrgAvailable:
            continue

        dataTypeFound = dt
        break

    if not dataTypeFound:
        print 'Remote random trigger round %s: not found in configuration' % roundNum
        return rantrgFilePaths

    rantrgRootUrl = gConfig.getValue('%s/%s/Url'%(configPrefix, dataTypeFound), '')
    if not rantrgRootUrl:
        print 'Remote random trigger round %s: url not found in configuration' % roundNum
        return rantrgFilePaths

    rantrgFileStructure = gConfig.getValue('%s/%s/FileStructure'%(configPrefix, dataTypeFound), '')
    rantrgFileStructure = rantrgFileStructure.lower()

    for line in resultSqlite:
        if rantrgFileStructure == 'plain':
            rawFn = os.path.join(rantrgRootUrl, roundNum, line[2])
        else:
            roundStart = line[1].rfind('round')
            if roundStart < 0:
                print 'Remote random trigger path error: can not found round number in %s' % line[1]
                return rantrgFilePaths
            pathMiddle = line[1][roundStart:].rstrip('/')
            rawFn = os.path.join(rantrgRootUrl, pathMiddle, line[2])

        rantrgFilePaths.append(rawFn+'.idx')
        rantrgFilePaths.append(rawFn)

    return rantrgFilePaths


def cmd(args):
    startcmd = '%s\\n%s  Start Executing: %s' % ('='*80, '>'*16, args)
    print >>logFile, startcmd
    logFile.flush()

    result = -1
    try:
        result = call(args, stdout=logFile, stderr=STDOUT)
    except Exception as e:
        print 'Run command failed: %s' % e

    endcmd = '%s  End Executing: %s\\n%s\\n' % ('<'*16, args, '='*80)
    print >>logFile, endcmd
    logFile.flush()
    return result

def cp(src, dst):
    if os.path.exists(src) and os.path.exists(dst):
        shutil.copy(src, dst)

def launchInputDownload():
    if not autoDownload:
        return True

    print 'Downloading auto uploaded file %s ...' % autoDownload
    result = dirac.getFile(autoDownload)
    if not (result['OK'] and result['Value']['Successful']):
        print 'Auto uploaded files download failed: %s' % autoDownload
        return False

    tf = tarfile.open(os.path.basename(autoDownload))
    tf.extractall()
    tf.close()

    return True

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
            print 'setJobInfo error: %s' % result

def setJobStatus(message):
    if jobID != '0':
        jobReport = JobReport(int(jobID), 'BossScript')
        result = jobReport.setApplicationStatus(message)
        if not result['OK']:
            print 'setJobStatus error: %s' % result

def setRantrgSEJobStatus():
    rantrgLogFile = open('rantrg.log')
    rantrgOutput = rantrgLogFile.read()
    rantrgLogFile.close()
    m = re.search('^Determine SE: (.*)$', rantrgOutput, re.M)
    if m:
        rantrgSe = m.group(1)
    if not rantrgSe:
        rantrgSe = 'unknown'
    setJobStatus('Random Trigger Downloaded from: %s' % rantrgSe)

def uploadData(lfn, maxRetry=5, minWait=0, maxWait=60):
    fn = os.path.basename(lfn)
    path = os.path.join(os.getcwd(), fn)
    for i in range(maxRetry):
        result = cmd(['globus-url-copy', '-sync', 'file:///%s'%path, 'gsiftp://storm.ihep.ac.cn:2811%s'%lfn])
        if not result:
            break
        if i+1 >= maxRetry:
            break
        time.sleep(random.randint(minWait, maxWait))
        print '- Upload to %s failed, try again' % lfn
    if not result:
        print 'Successfully uploading %s. Retry %s' % (lfn, i+1)
    else:
        print 'Failed uploading %s. Retry %s' % (lfn, i+1)
    return result == 0

def uploadLog(loglfn):
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
    cp('rantrg.log', logdir)

    # tar path logdir
    tgzfile = path
    tf = tarfile.open(tgzfile,"w:gz")
    tf.add(logdir)
    tf.close()

    result = uploadData(loglfn, maxRetry=1)

    return result

def disableWatchdog():
    if not os.path.exists('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK'):
        open('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK', 'w').close()
        delDisableWatchdog = True

def enableWatchdog():
    if delDisableWatchdog:
        shutil.rmtree('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK', True)
        delDisableWatchdog = False

def startRantrgDownload(remoteRantrgPaths):
    # download random trigger files
    setJobStatus('Downloading Random Trigger')
    disableWatchdog()
    return Popen(['./rantrg_get.sh']+remoteRantrgPaths, stdout=logFile, stderr=STDOUT)

def startSimulation():
    setJobStatus('Simulation')

    startcmd = '%s\\n%s  Start Executing: %s' % ('='*80, '>'*16, ['./boss_run.sh', bossRepo, bossVer])
    print >>logFile, startcmd
    logFile.flush()

    return Popen(['./boss_run.sh', bossRepo, bossVer], stdout=logFile, stderr=STDOUT)

def endSimulation():
    endcmd = '%s  End Executing: %s\\n%s\\n' % ('<'*16, ['./boss_run.sh', bossRepo, bossVer], '='*80)
    print >>logFile, endcmd
    logFile.flush()

#    setJobStatus('Simulation Finished')

def generateLocalRantrgOpt(replacePath='', newPath=''):
    extraOptFile = open('extra.opts', 'w')
    extraOptFile.write('MixerAlg.ReplaceDataPath = "%s";\\n' % replacePath)
    extraOptFile.write('MixerAlg.UseNewDataDir = "%s";\\n' % newPath)
    extraOptFile.close()

def checkRantrgDownloadStatus():
    pass

def checkRepo(repo):
    repoDir = os.path.join('/cvmfs', repo)

    try:
        os.listdir(repoDir)
    except OSError as e:
        print 'List directory "%s" failed: %s' % (repoDir, e)

    if not os.path.isdir(repoDir):
        return False

    return True

def checkEnvironment():
    diracGridType, place, country = siteName.split('.')
    if country == 'cn':
        repos = ['bes.ihep.ac.cn', 'boss.cern.ch']
    else:
        repos = ['boss.cern.ch', 'bes.ihep.ac.cn']

    for repo in repos:
        if checkRepo(repo):
            global bossRepo
            bossRepo = repo
            setJobStatus('BOSS cvmfs repo: %s' % repo)
            return True

    setJobStatus('BOSS cvmfs not found')
    return False

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

            if rantrgMethod == 'new':
                print >>logFile, 'Use local random trigger path: %s' % localRantrgPath
                cmd(['ls', '-ld', localRantrgPath])
                generateLocalRantrgOpt(newPath=localRantrgPath)
            elif rantrgMethod == 'replace':
                print >>logFile, 'Replace local random trigger path: %s' % localRantrgPath
                cmd(['ls', '-ld', localRantrgPath])
                generateLocalRantrgOpt(replacePath=localRantrgPath)
            elif rantrgMethod == 'Default':
                print >>logFile, 'Use local random trigger with default path'

        if not rantrgMethod:
            if runH != runL:
                print 'Too many runs to download random trigger file. %s - %s' % (runL, runH)
                setJobStatus('Can not do reconstruction on this site with split by event')
                return 71
            setJobInfo('Start Downloading Random Trigger')
            remoteRantrgPaths = getRemoteRantrgPaths()
            if not remoteRantrgPaths:
                print 'No random trigger files found available'
                return 66
            pdRantrg = startRantrgDownload(remoteRantrgPaths)

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
                    retCode = 65
                    break
                else:
#                    setRantrgSEJobStatus()
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
            result = cmd(['./boss_run.sh', bossRepo, bossVer, 'rec', 'extra.opts'])
        else:
            result = cmd(['./boss_run.sh', bossRepo, bossVer, 'rec'])

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
        result = cmd(['./boss_run.sh', bossRepo, bossVer, 'ana'])

        if result:
            setJobStatus('Analysis Error: %s' % result)
            setJobInfo('End Analysis with Error')
            setJobInfo('End Job with Error: %s' % result)
            return result

        setJobInfo('End Analysis')


    # upload files and reg
    for lfn in lfns:
        setJobStatus('Uploading Data')
        setJobInfo('Start Uploading Data')
        result = S_ERROR('No SE specified')
        result = uploadData(lfn, maxRetry=5, minWait=60, maxWait=300)
        if not result:
            print 'Upload Data Error:\\n%s' % result
            setJobStatus('Upload Data Error')
            setJobInfo('End Uploading Data with Error')
            setJobInfo('End Job with Error: %s' % 72)
            return 72
        setJobStatus('Upload Data successfully')
        setJobInfo('End Uploading Data')

    setJobStatus('Boss Job Finished Successfully')
    setJobInfo('End Job')
    return 0


if __name__ == '__main__':
    sys.stdout = logFile
    sys.stderr = logFile

    # file executable
    os.chmod('boss_run.sh', 0755)
    os.chmod('rantrg_get.sh', 0755)

    # prepare
    cmd(['date'])
    cmd(['uname', '-a'])
    cmd(['ip', 'addr'])
    cmd(['/sbin/ifconfig'])
    cmd(['lsb_release', '-a'])
    cmd(['df', '-hT'])
    cmd(['pwd'])
    cmd(['df', '-T', '.'])
    cmd(['ls', '-ltrA'])

    # auto download
    if not launchInputDownload():
        sys.exit(74)

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
    cmd(['df', '-T', '.'])
    cmd(['date'])

    # upload log and reg
    setJobStatus('Uploading Log')
    result = uploadLog(loglfn)
    if not result:
        setJobStatus('Upload Log Error')

    sys.exit(exitStatus)
"""

    return script_head + script_body

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class GaudiDiracRTHandler(IRuntimeHandler):
    """The runtime handler to run Gaudi jobs on the Dirac backend"""

    def __init__(self):
        self._allRound = []
        self._autoDownload = ''

    def master_prepare(self,app,appconfig):
        bdr = BDRegister(app.extra.metadata)
        self._fullOutputDir = bdr.getFullOutputDir(app.output_dir)
        if 'streamId' in app.extra.metadata and app.extra.metadata['streamId']:
            self._fullOutputDir = os.path.join(self._fullOutputDir, app.extra.metadata['streamId'])
        app.add_output_dir(self._fullOutputDir)

        app.extra.master_input_buffers['boss_run.sh'] = boss_run_wrapper()
        app.extra.master_input_buffers['rantrg_get.sh'] = rantrg_get_wrapper()

        self._boss_auto_upload(app)
        self._boss_patch(app)
        self._create_patch_script(app)
        self._init_task(app)
        self._job_group(app)
        self._make_output_dir(app)

        sandbox = get_master_input_sandbox(app.getJobObject(),app.extra)
        c = StandardJobConfig('',sandbox,[],[],None)
        return c

    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        # some extra lines for simulation job options on DIRAC site
        opts = 'DatabaseSvc.DbType = "sqlite";\n'
        app.extra.input_buffers['data.opts'] += opts

        # extra lines for reconstruction and remove empty files
        if app.recoptsfile:
            app.extra.input_buffers['recdata.opts'] += opts
            extraopts = 'MixerAlg.UseNewDataDir = ".";\n'
            app.extra.input_buffers['recdata.opts'] += extraopts
        else:
            app.extra.input_buffers.pop('recdata.opts', None)

        # extra lines for analysis and remove empty files
        if app.anaoptsfile:
            app.extra.input_buffers['anadata.opts'] += opts
        else:
            app.extra.input_buffers.pop('anadata.opts', None)

        if app.extra.inputdata and app.extra.inputdata.hasLFNs():
            cat_opts = '\nFileCatalog().Catalogs = ' \
                       '["xmlcatalog_file:pool_xml_catalog.xml"]\n'
#            app.extra.input_buffers['data.py'] += cat_opts

        script = self._create_boss_script(app)
        sandbox = get_input_sandbox(app.extra)
        app.extra.outputsandbox += ['script.log', 'rantrg.log', 'bosslog', 'bosserr', 'recbosslog', 'recbosserr', 'anabosslog', 'anabosserr']
        outputsandbox = app.extra.outputsandbox
        c = StandardJobConfig(script,sandbox,[],outputsandbox,None)

        dirac_script = DiracScript()
        dirac_script.job_type = 'Job()'
        dirac_script.exe = DiracApplication(app,script)
        dirac_script.platform = app.platform
        dirac_script.output_sandbox = outputsandbox
        dirac_script.default_sites = gConfig.getValue('/Resources/Applications/DefaultSites/%s' % DfcOperation().getGroupName(), [])

        if app.extra.inputdata:
            dirac_script.inputdata = DiracInputData(app.extra.inputdata)

        if app.extra.outputdata:
            dirac_script.outputdata = app.extra.outputdata

        c.script = dirac_script
        return c

    def _upload_to_se(self, local_path, remote_path, se):
        result = fcc.isFile(remote_path)
        # File already exist
        if result['OK'] and remote_path in result['Value']['Successful'] and result['Value']['Successful'][remote_path]:
            logger.debug('File %s already exist' % remote_path)
            return True

        result = dirac.addFile(remote_path, local_path, se)
        if not (result['OK'] and result['Value']['Successful'] and result['Value']['Successful'].has_key(remote_path)):
            logger.error('Auto upload file failed to: %s' % remote_path)
            return False

        logger.debug('File auto uploaded to: %s' % remote_path)
        return True

    def _upload_file(self, local_path, remote_dir, se):
        hashfile = open(local_path, 'rb')
        hash_name = hashlib.sha256(hashfile.read()).hexdigest()
        hashfile.close()
        remote_path = os.path.join(remote_dir, hash_name)
        if not self._upload_to_se(local_path, remote_path, se):
            return ''
        return remote_path

    def _pack_auto_upload(self,app):
        j = app.getJobObject()
        auto_upload_tgz_dir = j.getInputWorkspace().getPath()
        auto_upload_tgz = os.path.join(auto_upload_tgz_dir, '_ganga_boss_auto_upload.tar.gz')

        tf = tarfile.open(auto_upload_tgz, 'w:gz')
        for auto_upload_file in app.auto_upload:
            if not os.path.isfile(auto_upload_file):
                raise ApplicationConfigurationError(None, 'No such file: %s' % auto_upload_file)
            shutil.copy(auto_upload_file, auto_upload_tgz_dir)
            auto_upload_basename = os.path.basename(auto_upload_file)
            auto_upload_fullname = os.path.join(auto_upload_tgz_dir, auto_upload_basename)
            tinfo = tf.gettarinfo(auto_upload_fullname, auto_upload_basename)
            fileobj = open(auto_upload_fullname)
            tf.addfile(tinfo, fileobj)
#            tf.add(os.path.join(os.path.basename(auto_upload_file), auto_upload_tgz_dir))
        tf.close()

        return auto_upload_tgz

    def _boss_auto_upload(self,app):
        if not app.auto_upload:
            return

        auto_upload_tgz = self._pack_auto_upload(app)

        bdr = BDRegister(app.extra.metadata)
        remote_dir = bdr.getUploadDirName()
        se = Ganga.Utility.Config.getConfig('Boss')['AutoUploadSE']
#  edit by yant. since sometimes upload will fail. so try 10 times
#        remote_path = self._upload_file(auto_upload_tgz, remote_dir, se)
        for repeatTime in range(10):
            remote_path = self._upload_file(auto_upload_tgz, remote_dir, se)
            if remote_path:
                break
            else:
                continue
        if not remote_path:
            raise ApplicationConfigurationError(None, 'Cannot upload file %s to %s' % (auto_upload_tgz, se))
        self._autoDownload = remote_path

    def _boss_patch(self,app):
        if app.use_boss_patch:
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

    def _init_task(self,app):
        j = app.getJobObject()
        if app.taskname:
            taskName = app.taskname
        elif 'JobGroup' in j.backend.settings:
            taskName = j.backend.settings['JobGroup']
        else:
            taskName = 'Boss'

        taskInfo = {}
        taskInfo['SE'] = eval(getConfig('Boss')['DiracOutputDataSE'])[0]
        taskInfo['OutputDirectory'] = self._fullOutputDir
        gDiracTask.updateTaskInfo(taskInfo)

        gDiracTask.setTaskName(taskName)

    def _job_group(self,app):
        # set up a unique job group name
        j = app.getJobObject()

        dfcOp = DfcOperation()
        jobGroupPrefix = 'prod' if dfcOp.getGroupName() == 'production' else dfcOp.getUserName()
        jobGroupPrefix += '_'

        jobGroup = j.backend.settings['JobGroup'] if 'JobGroup' in j.backend.settings else gDiracTask.getTaskName()
        if not jobGroup.startswith(jobGroupPrefix):
            jobGroup = jobGroupPrefix + jobGroup

        allJobGroups = gDiracTask.getAllJobGroups()
        if jobGroup not in allJobGroups:
            jobGroupTemp = jobGroup
        else:
            index = 2
            while True:
                jobGroupTemp = '%s_%d'%(jobGroup,index)
                if jobGroupTemp not in allJobGroups:
                    break
                index += 1
        gDiracTask.setJobGroup(jobGroupTemp)

    def _make_output_dir(self,app):
        dirs = app.output_step[:]
        dirs.append('log')
        for d in dirs:
            final_dir = os.path.join(self._fullOutputDir, d)
            if not os.path.exists(final_dir):
                os.makedirs(final_dir)


    def _create_boss_script(self,app):
        '''Creates the script that will set the Boss environment on grid'''
        app.extra.outputdata.location = self._fullOutputDir
        lfns = []
        for output_file in app.extra.output_files:
            lfns.append(os.path.join(app.extra.outputdata.location, output_file))
        loglfn = os.path.join(self._fullOutputDir, 'log', app.extra.output_name + '.log.tar.gz')
        wrapper = boss_script_wrapper(app.version, lfns, loglfn, eval(getConfig('Boss')['DiracOutputDataSE'])[0],
                                      app.eventNumber, app.runL, app.runH, app.local_rantrg, self._autoDownload)

        j = app.getJobObject()
        script = os.path.join(j.getInputWorkspace().getPath(), "boss_dirac.py")
        file=open(script,'w')
        file.write(wrapper)
        file.close()
        os.system('chmod +x %s' % script)

        jobInfo = {}
        jobInfo['EventNum'] = app.eventNumber
        jobInfo['RunL'] = app.runL
        jobInfo['RunH'] = app.runH
        jobInfo['Seed'] = app.seed
        jobInfo['GangaSubID'] = j.id
        jobInfo['OutputFileName'] = lfns
        jobInfo['OutputLogName'] = loglfn
        gDiracTask.appendJobInfo(jobInfo)

        return script

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
