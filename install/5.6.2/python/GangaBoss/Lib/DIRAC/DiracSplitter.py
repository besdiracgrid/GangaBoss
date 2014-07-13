#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
''' Splitter for DIRAC jobs. '''

from GangaBoss.Lib.Dataset.BesDataset import *
from GangaBoss.Lib.Dataset.DatasetUtils import *
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Adapters.ISplitter import SplittingError
import Ganga.Utility.logging
from GangaBoss.Lib.Gaudi.Splitters import SplitByFiles
from Dirac import Dirac
from DiracUtils import *

logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class DiracSplitter(SplitByFiles):
    """Query the LFC, via Dirac, to find optimal data file grouping.

    This Splitter will query the Logical File Catalog (LFC) to find
    at which sites a particular file is stored. Subjobs will be created
    so that all the data required for each subjob is stored in
    at least one common location. This prevents the submission of jobs that
    are unrunnable due to data availability.
    """

    _name = 'DiracSplitter'
    _schema = Schema(Version(1,0),{
        'filesPerJob' : SimpleItem(defvalue=10,
                                   doc='Number of files per subjob'),
        'maxFiles':SimpleItem(defvalue=-1,
                              doc='Maximum number of files to use in ' \
                              'a masterjob. A value of "-1" means all files'),
        'ignoremissing' : SimpleItem(defvalue=False,
                                     doc='Skip LFNs if they are not found ' \
                                     'in the LFC.')
        })
    
    def _splitFiles(self, inputs):
        from GangaBoss.Lib.Dataset import LogicalFile
        files = []
        for f in inputs.files:
            if isLFN(f): 
               files.append(f.name)
               print f.name
            if self.maxFiles > 0 and len(files) >= self.maxFiles: break
        cmd = 'result = DiracCommands.splitInputData(%s,%d)' \
              % (files,self.filesPerJob)
        print cmd
        result = Dirac.execAPI(cmd)
        if not result_ok(result):
            logger.error('Error splitting files: %s' % str(result))
            raise SplittingError('Error splitting files.')
        split_files = result.get('Value',[])
        if len(split_files) == 0:
            raise SplittingError('An unknown error occured.')
        datasets = []
        # check that all files were available on the grid
        big_list = []
        for l in split_files: big_list.extend(l)
        diff = set(inputs.getFileNames()).difference(big_list)
        if len(diff) > 0:            
            for f in diff: logger.warning('Ignored file: %s' % f)
            if not self.ignoremissing:
                raise SplittingError('Some files not found!')
        
        for l in split_files:
            dataset = BesDataset()
            dataset.depth = inputs.depth
            for file in l:
                dataset.files.append(LogicalFile(file))
            datasets.append(dataset)
        return datasets
        
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
