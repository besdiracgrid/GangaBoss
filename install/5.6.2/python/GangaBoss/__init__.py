import os
import Ganga.Utility.logging
import Ganga.Utility.Config
configBoss=Ganga.Utility.Config.makeConfig('Boss','Parameters for Boss')
configDirac=Ganga.Utility.Config.makeConfig('DIRAC','Parameters for DIRAC')
logger=Ganga.Utility.logging.getLogger()


dscrpt = 'The place where OutputData should go.'
configBoss.addOption('DataOutput',os.environ['HOME'],dscrpt)
configBoss.addOption('DiracOutputDataSE','',dscrpt)
dscrpt = 'Automatically download sandbox for failed jobs?'
configBoss.addOption('failed_sandbox_download',True,dscrpt)

dscrpt = 'The command to used to create a directory in the locations of \
`DataOutput`'
configBoss.addOption('mkdir_cmd','/bin/mkdir',dscrpt)

dscrpt = 'The command used to copy out data to the `DataOutput` locations'
configBoss.addOption('cp_cmd','/bin/cp',dscrpt)

dscrpt = 'Files from these services will go to the output sandbox (unless \
overridden by the user in a specific job via the Job.outputdata field). Files \
from all other known handlers will go to output data (unless overridden by \
the user in a specific job via the Job.outputsandbox field).'
#configBoss.addOption('outputsandbox_types',
#                     ['NTupleSvc','HistogramPersistencySvc',
#                       'EvtTupleSvc'],dscrpt)
configBoss.addOption('outputsandbox_types',
                     ['HistogramPersistencySvc','EvtTupleSvc'],dscrpt)

dscrpt = 'The string that is added after the filename in the options to tell' \
         ' Gaudi how to read the data. This is the default value used if the '\
         'file name does not match any of the patterns in '\
         'datatype_string_patterns.'
configBoss.addOption('datatype_string_default',
                     """TYP='POOL_ROOTTREE' OPT='READ'""",dscrpt)
dscrpt = 'If a file matches one of these patterns, then the string here '\
         'overrides the datatype_string_default value.'
defval = {"SVC='LHCb::MDFSelector'" : ['*.rtaw','*.rec']}
configBoss.addOption('datatype_string_patterns',defval,dscrpt)
dscrpt = 'BES3 databases parameters'
defval = "production"
configBoss.addOption('dbuser',defval,dscrpt)
defval = "bes3mc"
configBoss.addOption('dbpass',defval,dscrpt)
defval = "bes3db1.ihep.ac.cn"
#defval = "202.122.33.121"
configBoss.addOption('dbhost',defval,dscrpt)

# RoundSearch path
dscrpt = 'The RoundSearch file path'
defval = '/afs/.ihep.ac.cn/bes3/offline/ExternalLib/gangadist/RoundSearch.txt'
configBoss.addOption('RoundSearchPath',defval,dscrpt)

# Patch path
dscrpt = 'The patch file path'
defval = '/afs/.ihep.ac.cn/bes3/offline/ExternalLib/gangadist/scripts'
configBoss.addOption('PatchPath',defval,dscrpt)

# Boss Patch
dscrpt = 'Boss patch conf'
defval = '/afs/.ihep.ac.cn/bes3/offline/ExternalLib/gangadist/scripts/Boss.conf'
configBoss.addOption('BossPatch',defval,dscrpt)


# Set default values for the Dirac section.
dscrpt = 'Display DIRAC API stdout to the screen in Ganga?'
configDirac.addOption('ShowDIRACstdout',False,dscrpt)
dscrpt = 'Global timeout (seconds) for Dirac commands'
configDirac.addOption('Timeout',1000,dscrpt)
dscrpt = 'Wait time (seconds) prior to first poll of Dirac child proc'
configDirac.addOption('StartUpWaitTime',3,dscrpt)

#set pbs
from Ganga.Utility.Config import getConfig
config=getConfig('PBS')
config.setUserValue('submit_res_pattern','^(?P<id>\\d*)\\.\\w\\s*')
config.setUserValue('kill_res_pattern','(^$)|(qdel: Unknown Job Id \\w\\s*)')


def getEnvironment( config = {} ):
   import sys
   import os.path
   import PACKAGE

   PACKAGE.standardSetup()
   return


def loadPlugins( config = {} ):
     import Lib.Gaudi
     import Lib.DIRAC
     import Lib.Dataset
     import Lib.DIRAC.DiracSplitter
