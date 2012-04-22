#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
'''Parent for all Gaudi and GaudiPython applications in Boss.'''

import tempfile
import gzip
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Adapters.IApplication import IApplication
import CMTscript
from GangaBoss.Lib.Gaudi.CMTscript import parse_master_package
import Ganga.Utility.logging
from Ganga.Utility.files import expandfilename, fullpath
from GangaBoss.Lib.Dataset.BesDataset import BesDataset
from GangaBoss.Lib.Dataset.OutputData import OutputData
from GaudiUtils import *
from Ganga.GPIDev.Lib.File import File
from DIRAC.Interfaces.API.Badger import Badger
from Ganga.Core import ApplicationConfigurationError
import Ganga.Utility.Config

logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def get_common_gaudi_schema():
    schema = {}
    docstr = 'The version of the application (like "v19r2")'
    schema['version'] = SimpleItem(defvalue=None,
                                   typelist=['str','type(None)'],doc=docstr)
    docstr = 'run number'
    schema['runL'] = SimpleItem(defvalue=None,
                                   typelist=['int','type(None)'],doc=docstr)
    docstr = 'outputfile'
    schema['outputfile'] = SimpleItem(defvalue=None,
                                   typelist=['str','type(None)'],doc=docstr)
    docstr = 'The platform the application is configured for (e.g. ' \
             '"slc4_ia32_gcc34")'
    schema['platform'] = SimpleItem(defvalue=None,
                                    typelist=['str','type(None)'],doc=docstr)
    docstr = 'The package the application belongs to (e.g. "Sim", "Phys")'
    schema['package'] = SimpleItem(defvalue=None,
                                   typelist=['str','type(None)'],doc=docstr)
    docstr = 'The user path to be used. After assigning this'  \
             ' you can do j.application.getpack(\'Phys DaVinci v19r2\') to'  \
             ' check out into the new location. This variable is used to '  \
             'identify private user DLLs by parsing the output of "cmt '  \
             'show projects".'
    schema['user_release_area'] = SimpleItem(defvalue=None,
                                             typelist=['str','type(None)'],
                                             doc=docstr)
    docstr = 'The package where your top level requirements file is read '  \
             'from. Can be written either as a path '  \
             '\"Tutorial/Analysis/v6r0\" or in a CMT style notation '  \
             '\"Analysis v6r0 Tutorial\"'
    schema['masterpackage'] = SimpleItem(defvalue=None,
                                         typelist=['str','type(None)'],
                                         doc=docstr)
    return schema

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class Francesc(IApplication):
    '''Parent for all Gaudi and GaudiPython applications, should not be used
    directly.'''    
    _name = 'Francesc'
    _exportmethods = ['getenv','getpack', 'make', 'cmt', 'register']
    _schema = Schema(Version(1, 1), {})

    def get_gaudi_appname(self):
        '''Handles the (unfortunate legacy) difference between Gaudi and
        GaudiPython schemas wrt this attribute name.'''
        appname = ''
        try: appname = self.appname
        except AttributeError:
            appname = self.project
        return appname

    def _init(self,gaudi_app,set_ura):        
        if (not self.version): self.version = guess_version(gaudi_app)
        if (not self.platform): self.platform = get_user_platform()
        if (not self.package): self.package = available_packs(gaudi_app)
        if not set_ura: return
        if not self.user_release_area:
            expanded = os.path.expandvars("$User_release_area")
            if expanded == "$User_release_area": self.user_release_area = ""
            else:
                self.user_release_area = expanded.split(os.pathsep)[0]

    def _check_gaudi_inputs(self,optsfiles,appname):
        """Checks the validity of some of user's entries."""
        for fileitem in optsfiles:
            fileitem.name = os.path.expanduser(fileitem.name)
            fileitem.name = os.path.normpath(fileitem.name)
    
        if appname is None:
            msg = "The appname is not set. Cannot configure."
            logger.error(msg)
            raise ApplicationConfigurationError(None,msg)
    
        if appname not in available_apps():
            msg = "Unknown application %s. Cannot configure." % appname
            logger.error(msg)
            raise ApplicationConfigurationError(None,msg)
    
    def _getshell(self):
        appname = self.get_gaudi_appname()
        ver  = self.version

        fd = tempfile.NamedTemporaryFile()
        script = '#!/bin/sh\n'
        if self.user_release_area:
            script += 'User_release_area=%s; export User_release_area\n' % \
                      expandfilename(self.user_release_area)
        if self.platform:    
            script += 'export CMTCONFIG=%s\n' % self.platform
        useflag = ''
        if self.masterpackage:
            (mpack, malg, mver) = parse_master_package(self.masterpackage)
            useflag = '--use \"%s %s %s\"' % (malg, mver, mpack)
        #bossShell = os.environ["GANGABOSSENVIRONMENT"]
        gaudirunShell = os.environ["GAUDIRUNENV"]
        #cmd = 'source %s\n' % (bossShell)
        cmd = 'source %s' % (gaudirunShell)
        #cmd = '%s' % (gaudirunShell)
        script += '%s \n' % cmd
        fd.write(script)
        fd.flush()
        logger.error("zhangxm log: run boss env script:\n%s" % script)

        self.shell = Shell(setup=fd.name)
        #logger.error(pprint.pformat(self.shell.env))
        
        fd.close()
        app_ok = False
        ver_ok = False
        for var in self.shell.env:
            #logger.error("var:\n%s" % var)
            #if var.find(self.get_gaudi_appname()) >= 0: app_ok = True
            if self.shell.env[var].find(self.get_gaudi_appname()) >= 0: app_ok = True
            if self.shell.env[var].find(self.version) >= 0: ver_ok = True
        if not app_ok or not ver_ok:
            msg = 'Command "%s" failed to properly setup environment.' % cmd
            logger.error(msg)
            raise ApplicationConfigurationError(None,msg)

    def getenv(self):
        '''Returns a copy of the environment used to flatten the options, e.g.
        env = DaVinci().getenv(), then calls like env[\'DAVINCIROOT\'] return
        the values.
        
        Note: Editing this does not affect the options processing.
        '''
        try:
            job = self.getJobObject()
        except:
            self._getshell()
            return self.shell.env.copy()
        env_file_name = job.getInputWorkspace().getPath() + '/gaudi-env.py.gz'
        if not os.path.exists(env_file_name):
            self._getshell()
            return self.shell.env.copy()
        else:
            in_file = gzip.GzipFile(env_file_name,'rb')
            exec(in_file.read())
            in_file.close()
            return gaudi_env
    
    def getpack(self, options=''):
        """Execute a getpack command. If as an example dv is an object of
        type DaVinci, the following will check the Analysis package out in
        the cmt area pointed to by the dv object.

        dv.getpack('Tutorial/Analysis v6r2')
        """
        # Make sure cmt user area is there
        cmtpath = expandfilename(self.user_release_area)
        if cmtpath:
            if not os.path.exists(cmtpath):
                try:
                    os.makedirs(cmtpath)
                except Exception, e:
                    logger.error("Can not create cmt user directory: "+cmtpath)
                    return
                
        command = 'getpack ' + options + '\n'
        CMTscript.CMTscript(self,command)

    def make(self, argument=''):
        """Build the code in the release area the application object points
        to. The actual command executed is "cmt broadcast make <argument>"
        after the proper configuration has taken place."""
        #command = '###CMT### broadcast -global -select=%s cmt make ' \
        #          % self.user_release_area + argument
        config = Ganga.Utility.Config.getConfig('Boss')
        command = config['make_cmd']
        CMTscript.CMTscript(self,command)

    def register(self):
        """ register data file in File Catalog"""
        lfn = self.outputfile
        logger.error('zhangxm log:  the options file: %s %d', lfn, self.runL)
        entryDict = {'runL':self.runL}
        badger = Badger()
        result = badger.registerFileMetadata(lfn,entryDict)
        return result

    def cmt(self, command):
        """Execute a cmt command in the cmt user area pointed to by the
        application. Will execute the command "cmt <command>" after the
        proper configuration. Do not include the word "cmt" yourself."""
        command = '###CMT### ' + command
        CMTscript.CMTscript(self,command)

    def _master_configure(self):
        '''Handles all common master_configure actions.'''
        self.extra = GaudiExtras()
        self._getshell()
        
        job=self.getJobObject()                
        if job.inputdata: self.extra.inputdata = job.inputdata
        if job.outputdata: self.extra.outputdata = job.outputdata
                        
        if not self.user_release_area: return

        appname = self.get_gaudi_appname()
        dlls, pys, subpys = get_user_dlls(appname, self.version,
                                          self.user_release_area,self.platform,
                                          self.shell)

        self.extra.master_input_files += [File(f,subdir='lib') for f in dlls]
        for f in pys:
            tmp = f.split('InstallArea')[-1]
            subdir = 'InstallArea' + tmp[:tmp.rfind('/')+1]
            self.extra.master_input_files.append(File(f,subdir=subdir))
        for dir, files in subpys.iteritems():
            for f in files:
                tmp = f.split('InstallArea')[-1]
                subdir = 'InstallArea' + tmp[:tmp.rfind('/')+1]
                self.extra.master_input_files.append(File(f,subdir=subdir))

    def _configure(self):
        job=self.getJobObject()
        txt_str = self.extra.inputdata.optionsString()
        data_str = txt_str
        if txt_str:
           self.extra.input_buffers['data.opts'] += txt_str
           data_str = '\nfrom Gaudi.Configuration import * \n'
           data_str += 'importOptions("data.opts")\n'
           logger.error("zhangxm log: data files: %s", data_str)
        self.extra.input_buffers['data.py'] += data_str


    #def postprocess(self):
        #from Ganga.GPIDev.Adapters.IApplication import PostprocessStatusUpdate
        #job = self.getJobObject()
        #if job:
        #raise PostprocessStatusUpdate("failed")
        
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class GaudiExtras:
    '''Used to pass extra info from Gaudi apps to the RT-handler.'''
    _name = "GaudiExtras"
    _category = "extras"

    def __init__(self):
        self.master_input_buffers = {}
        self.master_input_files = []
        self.input_buffers = {}
        self.input_files = []
        self.inputdata = BesDataset()
        self.outputsandbox = []
        self.outputdata = OutputData()
        self.input_buffers['data.py'] = ''
        self.input_buffers['recdata.py'] = ''
        self.input_buffers['data.opts'] = ''
        self.input_buffers['recdata.opts'] = ''

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
