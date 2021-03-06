#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
'''Application handler for GaudiPython applications in Boss.'''
import os
from os.path import split,join
import inspect
from Ganga.GPIDev.Schema import *
import Ganga.Utility.logging
from Ganga.GPIDev.Lib.File import  File
from Francesc import *
from Ganga.Utility.util import unique

logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class GaudiPython(Francesc):
    """The GaudiPython Application handler
    
    The GaudiPython application handler is for running Boss GaudiPython
    jobs. This means running scripts where you are in control of the events
    loop etc. If you are usually running jobs using the gaudirun script
    this is *not* the application handler you should use. Instead use the
    DaVinci, Gauss, ... handlers.

    For its configuration it needs to know what application and version to
    use for setting up the environment. More detailed configuration options
    are described in the schema below.
    
    An example of submitting a GaudiPython job to Dirac could be:
    
    app = GaudiPython(project='DaVinci', version='v19r14')

    # Give absolute path to the python file to be executed. 
    # If several files are given the subsequent ones will go into the
    # sandbox but it is the users responsibility to include them
    app.script = ['/afs/...../myscript.py']

    # Define dataset
    ds = BossDataset(['LFN:spam','LFN:eggs'])

    # Construct and submit job object
    j=Job(application=app,backend=Dirac(),inputdata=ds)
    j.submit()

"""
    _name = 'GaudiPython'
    _category = 'applications'

    schema = get_common_gaudi_schema()
    docstr = 'The name of the script to execute. A copy will be made ' + \
             'at submission time'
    schema['script'] = FileItem(sequence=1,strict_sequence=0,defvalue=[],
                                doc=docstr)
    docstr = "List of arguments for the script"
    schema['args'] =  SimpleItem(defvalue=[],typelist=['str'],
                                 sequence=1,doc=docstr)
    docstr = 'The name of the Gaudi application (e.g. "DaVinci", "Gauss"...)'
    schema['project'] = SimpleItem(defvalue=None,
                                   typelist=['str','type(None)'],
                                   doc=docstr)
    _schema = Schema(Version(1, 2), schema)                                    

    def _auto__init__(self):
        if (not self.project): self.project = 'DaVinci'
        self._init(self.project,False)
        
    def master_configure(self):
        self._master_configure()
        self._check_inputs()
        self.extra.master_input_files += self.script[:]
        return (None,self.extra)

    def configure(self,master_appconfig):
        self._configure()
        name = join('.',self.script[0].subdir,split(self.script[0].name)[-1])
        script =  "from Gaudi.Configuration import *\n"
        if self.args:
            script += 'import sys\nsys.argv += %s\n' % str(self.args)
        script += "importOptions('data.py')\n"
        script += "execfile(\'%s\')\n" % name
        self.extra.input_buffers['gaudipython-wrapper.py'] = script
        outsb = self.getJobObject().outputsandbox
        self.extra.outputsandbox = unique(outsb)
        return (None,self.extra)
            
    def _check_inputs(self):
        """Checks the validity of user's entries for GaudiPython schema"""
        self._check_gaudi_inputs(self.script,self.project)
        if len(self.script)==0:
            logger.warning("No script defined. Will use a default " \
                           'script which is probably not what you want.')
            self.script = [File(os.path.join(
                os.path.dirname(inspect.getsourcefile(GaudiPython)),
                'options/GaudiPythonExample.py'))]
        return

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

# Associate the correct run-time handlers to GaudiPython for various backends.

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from GangaBoss.Lib.Gaudi.GaudiRunTimeHandler import GaudiRunTimeHandler
from GangaBoss.Lib.DIRAC.GaudiDiracRTHandler import GaudiDiracRTHandler

for backend in ['LSF','Interactive','PBS','SGE','Local','Condor','Remote']:
    allHandlers.add('GaudiPython', backend, GaudiRunTimeHandler)
allHandlers.add('GaudiPython', 'Dirac', GaudiDiracRTHandler)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
