#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
'''Application handler for Gaudi applications in Boss.'''
import os
import re
import tempfile
import gzip
from Ganga.GPIDev.Schema import *
from Ganga.Core import ApplicationConfigurationError
import Ganga.Utility.logging
from GaudiUtils import *
from GaudiRunTimeHandler import * 
from PythonOptionsParser import PythonOptionsParser
from Francesc import *
from GangaBoss.Lib.Dataset.BDRegister import BDRegister
from Ganga.Utility.util import unique
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
from Ganga.Utility.Shell import Shell

logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def GaudiDocString(appname):
    "Provide the documentation string for each of the Gaudi based applications"
    
    doc="""The Gaudi Application handler

    The Gaudi application handler is for running  GAUDI framework
    jobs. For its configuration it needs to know the version of the application
    and what options file to use. More detailed configuration options are
    described in the schema below.

    An example of submitting a Gaudi job to Dirac could be:

    app = Gaudi(version='v99r0')

    # Give absolute path to options file. If several files are given, they are
    # just appended to each other.
    app.optsfile = ['/afs/...../myopts.opts']

    # Append two extra lines to the python options file
    app.extraopts=\"\"\"
    ApplicationMgr.HistogramPersistency ="ROOT"
    ApplicationMgr.EvtMax = 100
    \"\"\"

    # Define dataset
    ds = BesDataset(['LFN:foo','LFN:bar'])

    # Construct and submit job object
    j=Job(application=app,backend=Dirac())
    j.submit()

    """
    return doc.replace( "Gaudi", appname )
 

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
class Gaudi(Francesc):
    
    _name = 'Gaudi'
    __doc__ = GaudiDocString(_name)
    _category = 'applications'
    _exportmethods = ['getenv','getpack', 'make', 'cmt', 'register', 'add_output_dir', 'get_output_dir', 'add_dataset_name', 'get_dataset_name']

    schema = get_common_gaudi_schema()
    docstr = 'The name of the optionsfile. Import statements in the file ' \
             'will be expanded at submission time and a full copy made'
    schema['optsfile'] =  FileItem(sequence=1,strict_sequence=0,defvalue=[],
                                   doc=docstr)
    docstr = 'The name of the rec optionsfile. '
    schema['recoptsfile'] =  FileItem(sequence=1,strict_sequence=0,defvalue=[],
                                   doc=docstr)
    docstr = 'The name of the ana optionsfile. '
    schema['anaoptsfile'] =  FileItem(sequence=1,strict_sequence=0,defvalue=[],
                                   doc=docstr)
    docstr = 'The name of the Gaudi application (e.g. "DaVinci", "Gauss"...)'
    schema['appname'] = SimpleItem(defvalue=None,typelist=['str','type(None)'],
                                   hidden=1,doc=docstr)
    schema['configured'] = SimpleItem(defvalue=None,hidden=0,copyable=0,
                                      typelist=['str','type(None)']) 
    docstr = 'A python configurable string that will be appended to the '  \
             'end of the options file. Can be multiline by using a '  \
             'notation like \nHistogramPersistencySvc().OutputFile = '  \
             '\"myPlots.root"\\nEventSelector().PrintFreq = 100\n or by '  \
             'using triple quotes around a multiline string.'
    schema['extraopts'] = SimpleItem(defvalue=None,
                                     typelist=['str','type(None)'],doc=docstr) 
    docstr = 'User metadata'
    schema['metadata'] = SimpleItem(defvalue={},doc=docstr) 
    _schema = Schema(Version(2, 1), schema)
    docstr = 'Long idle job'
    schema['long_idle'] = SimpleItem(defvalue=False,doc=docstr)
    docstr = 'Create dataset'
    schema['create_dataset'] = SimpleItem(defvalue=True,doc=docstr)
    docstr = 'Use local random trigger files'
    schema['local_rantrg'] = SimpleItem(defvalue=True,doc=docstr)
    docstr = 'Patch files'
    schema['patch'] = SimpleItem(defvalue=[],doc=docstr)
    docstr = 'Use patch for BOSS'
    schema['use_boss_patch'] = SimpleItem(defvalue=True,doc=docstr)
    docstr = 'Auto upload files'
    schema['auto_upload'] = SimpleItem(defvalue=[],doc=docstr)
    docstr = 'User workarea'
    schema['user_workarea'] = SimpleItem(defvalue='',doc=docstr)
    docstr = 'Use custom packages'
    schema['use_custom_package'] = SimpleItem(defvalue=False,doc=docstr)
    docstr = 'Output root directory'
    schema['output_rootdir'] = SimpleItem(defvalue='',doc=docstr)

    def _auto__init__(self):
        """bootstrap Gaudi applications. If called via a subclass
        set up some basic structure like version platform..."""
        if not self.appname: return 
        self._init(self.appname,True)
       
            
    def master_configure(self):
        self._validate_version()

        job = self.getJobObject()
        self._master_configure()
        inputs = self._check_inputs()         
        optsfiles = [fileitem.name for fileitem in self.optsfile]
        recoptsfiles = [fileitem.name for fileitem in self.recoptsfile]
        anaoptsfiles = [fileitem.name for fileitem in self.anaoptsfile]
        try:
            parser = PythonOptionsParser(optsfiles,self.extraopts,self.shell)
            if recoptsfiles:
                recparser = PythonOptionsParser(recoptsfiles,self.extraopts,self.shell)
                if anaoptsfiles:
                    anaparser = PythonOptionsParser(anaoptsfiles,self.extraopts,self.shell)
        except ApplicationConfigurationError, e:
            debug_dir = job.getDebugWorkspace().getPath()
            f = open(debug_dir + '/gaudirun.stdout','w')
            f.write(e.message)
            f.close()
            msg = 'Unable to parse job options! Please check options ' \
                  'files and extraopts. The output from gaudyrun.py can be ' \
                  'found in %s. You can also view this from within ganga '\
                  'by doing job.peek(\'../debug/gaudirun.stdout\').' % f.name
            #logger.error(msg)
            raise ApplicationConfigurationError(None,msg)

        self.extra.master_input_buffers['options.pkl'] = parser.opts_pkl_str
        script = "%s/options.pkl" % job.getInputWorkspace().getPath()
        file_pkl=open(script,'w')
        file_pkl.write(parser.opts_pkl_str)
        file_pkl.close()
        if recoptsfiles:
            self.extra.master_input_buffers['recoptions.pkl'] = recparser.opts_pkl_str
            recscript = "%s/recoptions.pkl" % job.getInputWorkspace().getPath()
            file_recpkl=open(recscript,'w')
            file_recpkl.write(recparser.opts_pkl_str)
            file_recpkl.close()
            if anaoptsfiles:
                self.extra.master_input_buffers['anaoptions.pkl'] = anaparser.opts_pkl_str
                anascript = "%s/anaoptions.pkl" % job.getInputWorkspace().getPath()
                file_anapkl=open(anascript,'w')
                file_anapkl.write(anaparser.opts_pkl_str)
                file_anapkl.close()
        inputdata = parser.get_input_data()
  
        # If user specified a dataset, ignore optsfile data but warn the user.
        if len(inputdata.files) > 0:
            if job.inputdata:
                msg = 'A dataset was specified for this job but one was ' \
                      'also defined in the options file. Data in the options '\
                      'file will be ignored...hopefully this is OK.' 
                logger.warning(msg)            
            else:
                logger.info('Using the inputdata defined in the options file.')
                self.extra.inputdata = inputdata
        
        if anaoptsfiles:
           self.extra.outputsandbox,outputdata = anaparser.get_output(job)
        elif recoptsfiles:
           self.extra.outputsandbox,outputdata = recparser.get_output(job)
        else:
           self.extra.outputsandbox,outputdata = parser.get_output(job)
        self.extra.outputdata.files += outputdata
        self.extra.outputdata.files = unique(self.extra.outputdata.files)

        self._validate_input()
        self._custom_package()
        self._auto_upload_workarea()

        if self.output_rootdir:
            bdr = BDRegister(self.extra.metadata)
            bdr.setRootDir(self.output_rootdir)

        if anaoptsfiles:
            dataType = 'root'
        elif recoptsfiles:
            dataType = 'dst'
        else:
            dataType = 'rtraw'
        self._prepare_metadata(parser, dataType)

        # write env into input dir
        input_dir = job.getInputWorkspace().getPath()
        file = gzip.GzipFile(input_dir + '/gaudi-env.py.gz','wb')
        file.write('gaudi_env = %s' % str(self.shell.env))
        file.close()
        
        return (inputs, self.extra) # return (changed, extra)

    def configure(self,master_appconfig):
        self._configure()
        return (None,self.extra)

    _output_dir = []

    def add_output_dir(self,outputdir):
        Gaudi._output_dir.append(outputdir)

    def get_output_dir(self):
        return Gaudi._output_dir

    _dataset_name = []

    def add_dataset_name(self,dataset_name):
        Gaudi._dataset_name.append(dataset_name)

    def get_dataset_name(self):
        return Gaudi._dataset_name

    def _validate_version(self):
        if not re.match('^\d+\.\d+\.\d+(\.p\d+)?$', self.version):
            msg = 'The BOSS version format is not correct: %s. It should be like "6.6.3" or "6.6.4.p01"' % self.version
            raise ApplicationConfigurationError(None,msg)

    def _validate_input(self):
        if self.metadata.has_key('streamId') and not re.match('^stream(?!0+$)\d+$', self.metadata['streamId']):
            msg = 'The streamId format is not correct: %s. It should be like "stream001" but can not be "stream000"' % self.metadata['streamId']
            raise ApplicationConfigurationError(None,msg)

    def _custom_package(self):
        if self.user_workarea or not self.use_custom_package:
            return

        if 'CMTPATH' not in os.environ:
            raise ApplicationConfigurationError(None, 'Can not guess the user workarea')
        self.user_workarea = os.environ['CMTPATH'].split(':')[0]

    def _auto_upload_workarea(self):
        if not self.user_workarea:
            return

        platform = get_user_platform()
        lib_dir = os.path.join(self.user_workarea, 'InstallArea', platform, 'lib')
        lib_list = os.listdir(lib_dir)
        for lib_file in lib_list:
            if lib_file.endswith('.so'):
                lib_fullname = os.path.join(lib_dir, lib_file)
                lib_realname = os.readlink(lib_fullname)
                if os.path.isfile(lib_realname):
                    self.auto_upload.append(lib_realname)

    def _prepare_metadata(self, parser, dataType):
        # deal with some metadata
        self.extra.metadata = self.metadata.copy()
        self.extra.metadata['bossVer'] = self.version.replace('.', '')
#        self.extra.metadata['round'] = parser.get_round_num()  # the round could vary with different runs
        self.extra.metadata['dataType'] = dataType

        # the joboption and decay card
        self.extra.run_ranges = parser.get_run_range()

        # the main round
        self.extra.metadata['round'] = get_round_nums(self.extra.run_ranges)[0]

        # automatically get the stream ID
        if not self.extra.metadata.has_key('streamId'):
            bdr = BDRegister(self.extra.metadata)
            self.extra.metadata['streamId'] = bdr.getUnusedStream()

        # the joboption and decay card
        if self.optsfile:
            f = open(self.optsfile[0].name)
            self.extra.metadata['jobOptions'] = f.read()
            f.close()
            decaycard = parser.get_decay_card()
            if decaycard:
                decaycard_path = decaycard if decaycard.startswith('/') else os.path.join(os.path.dirname(self.optsfile[0].name), decaycard)
                f = open(decaycard_path)
                self.extra.metadata['decayCard'] = f.read()
                f.close()
                self.extra.master_input_buffers[decaycard] = self.extra.metadata['decayCard']

    def _check_inputs(self):
        """Checks the validity of some of user's entries for Gaudi schema"""

        self._check_gaudi_inputs(self.optsfile,self.appname)        
        if self.package is None:
            msg = "The 'package' attribute must be set for application. "
            raise ApplicationConfigurationError(None,msg)

        inputs = None
        if len(self.optsfile)==0:
            logger.warning("The 'optsfile' is not set. I hope this is OK!")
            packagedir = self.shell.env[self.appname.upper()+'ROOT']
            opts = os.path.expandvars(os.path.join(packagedir,'options',
                                                   self.appname + '.py'))
            if opts: self.optsfile.append(opts)
            else:
                logger.error('Cannot find the default opts file for ' % \
                             self.appname + os.sep + self.version)
            inputs = ['optsfile']
            
        return inputs

    def readInputData(self,optsfiles,extraopts=False):
        """Returns a BesDataSet object from a list of options files. The
        optional argument extraopts will decide if the extraopts string inside
        the application is considered or not. 
        
        Usage examples:
        # Create an BesDataset object with the data found in the optionsfile
        l=DaVinci(version='v22r0p2').readInputData([\"~/cmtuser/\" \
        \"DaVinci_v22r0p2/Tutorial/Analysis/options/Bs2JpsiPhi2008.py\"]) 
        # Get the data from an options file and assign it to the jobs inputdata
        field
        j.inputdata = j.application.readInputData([\"~/cmtuser/\" \
        \"DaVinci_v22r0p2/Tutorial/Analysis/options/Bs2JpsiPhi2008.py\"])
        
        # Assuming you have data in your extraopts, you can use the extraopts.
        # In this case your extraopts need to be fully parseable by gaudirun.py
        # So you must make sure that you have the proper import statements.
        # e.g.
        from Gaudi.Configuration import * 
        # If you mix optionsfiles and extraopts, as usual extraopts may
        # overwright your options
        # 
        # Use this to create a new job with data from extraopts of an old job
        j=Job(inputdata=jobs[-1].application.readInputData([],True))
        """
        
        def dummyfile():
            temp_fd,temp_filename=tempfile.mkstemp(text=True,suffix='.py')
            os.write(temp_fd,"#Dummy file to keep the Optionsparser happy")
            os.close(temp_fd)
            return temp_filename

        if type(optsfiles)!=type([]): optsfiles=[optsfiles]

        # use a dummy file to keep the parser happy
        if len(optsfiles)==0: optsfiles.append(dummyfile())

        self._getshell()
        inputs = self._check_inputs() 
        if extraopts: extraopts=self.extraopts
        else: extraopts=""
            
        try:
            parser = PythonOptionsParser(optsfiles,extraopts,self.shell)
        except Exception, e:
            msg = 'Unable to parse the job options. Please check options ' \
                  'files and extraopts.'
            raise ApplicationConfigurationError(None,msg)

        return GPIProxyObjectFactory(parser.get_input_data())
   
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

# Individual Gaudi applications. These are thin wrappers around the Gaudi base 
# class. The appname property is read protected and it tries to guess all the
# properties except the optsfile.

myschema = Gaudi._schema.inherit_copy()
myschema['appname']._meta['protected'] = 1

# getpack,... methods added b/c of bug in exportmethods dealing w/ grandchild
class_str = """
class ###CLASS###(Gaudi):
    _name = '###CLASS###'
    __doc__ = GaudiDocString(_name)
    _schema = myschema.inherit_copy()
    _exportmethods = ['getenv','getpack', 'make', 'cmt', 'register', 'add_output_dir', 'get_output_dir', 'add_dataset_name', 'get_dataset_name']

    def __init__(self):
        super(###CLASS###, self).__init__()
        self.appname = '###CLASS###'
        ###SETLHCBRA###

    def getenv(self,options=''):
        return super(###CLASS###,self).getenv()
        
    def getpack(self,options=''):
        return super(###CLASS###,self).getpack(options)

    def make(self,argument=''):
        return super(###CLASS###,self).make(argument)

    def register(self):
        return super(###CLASS###,self).register()

    def cmt(self,command):
        return super(###CLASS###,self).cmt(command)

    def add_output_dir(self,outputdir):
        return super(###CLASS###,self).add_output_dir(outputdir)

    def get_output_dir(self):
        return super(###CLASS###,self).get_output_dir()

    def add_dataset_name(self,dataset_name):
        return super(###CLASS###,self).add_dataset_name(dataset_name)

    def get_dataset_name(self):
        return super(###CLASS###,self).get_dataset_name()

    for method in ['getenv','getpack','make','cmt','register','add_output_dir','get_output_dir','add_dataset_name','get_dataset_name']:
        setattr(eval(method), \"__doc__\", getattr(Gaudi, method).__doc__)

"""

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from GangaBoss.Lib.Gaudi.GaudiRunTimeHandler import GaudiRunTimeHandler
from GangaBoss.Lib.DIRAC.GaudiDiracRTHandler import GaudiDiracRTHandler

for app in available_apps():
    exec_str = class_str.replace('###CLASS###', app)
    if app is 'Vetra':
        lhcbra = os.path.expandvars("$Vetra_release_area")
        exec_str = exec_str.replace('###SETLHCBRA###',
                                    'self.lhcb_release_area = "%s"' % lhcbra)
    else:
        exec_str = exec_str.replace('###SETLHCBRA###', '')
    if app is not 'Gaudi':
        exec(exec_str)

    for backend in ['LSF','Interactive','PBS','SGE','Local','Condor','Remote']:
        allHandlers.add(app, backend, GaudiRunTimeHandler)
    allHandlers.add(app, 'Dirac', GaudiDiracRTHandler)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

