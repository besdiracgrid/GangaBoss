#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from Ganga.Utility.Config import getConfig
from DiracUtils import *
from DiracScript import *
from GangaBoss.Lib.Gaudi.RTHUtils import *
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
def boss_env_wrapper():   
    return """#!/bin/bash
export CURDIR=`pwd`
ls -al 
export BESROOT=/cvmfs/boss.cern.ch
cd $BESROOT/slc5_amd64_gcc43/###BOSS_VERSION###/
source setup.sh
source scripts/###BOSS_VERSION###/setup.sh
echo $CMTPATH
export BossDir=$HOME/boss/TestRelease
if [ ! -d "$BossDir" ]; then 
  mkdir -p $BossDir
fi
cp -r $BESROOT/slc5_amd64_gcc43/###BOSS_VERSION###/dist/###BOSS_VERSION###/TestRelease/* $BossDir/
cd $HOME/boss/TestRelease/*/cmt
source setup.sh
cd $CURDIR/
gaudirun.py -n -v -o options.opts options.pkl data.py
boss.exe  options.opts
if [ -f "recoptions.pkl" ]; then
   gaudirun.py -n -v -o recoptions.opts recoptions.pkl recdata.py
   boss.exe recoptions.opts
fi
"""

class GaudiDiracRTHandler(IRuntimeHandler):
    """The runtime handler to run Gaudi jobs on the Dirac backend"""

    def master_prepare(self,app,appconfig):
        sandbox = get_master_input_sandbox(app.getJobObject(),app.extra) 
        c = StandardJobConfig('',sandbox,[],[],None)
        return c

    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        if app.extra.inputdata and app.extra.inputdata.hasLFNs():        
            cat_opts = '\nFileCatalog().Catalogs = ' \
                       '["xmlcatalog_file:pool_xml_catalog.xml"]\n'
            app.extra.input_buffers['data.py'] += cat_opts

        #script = self._create_gaudi_script(app) # comment out by zhangxm
        script = self._create_boss_script(app)
        sandbox = get_input_sandbox(app.extra)
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
