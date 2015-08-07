from Ganga.Core import GangaException

from DIRAC.Core.DISET.RPCClient                      import RPCClient

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

class DiracTask:

    def __init__(self):
        self.__task = RPCClient('WorkloadManagement/TaskManager')
        self.reset()

    def reset(self):
        self.__taskID = 0
        self.__taskName = 'UNKNOWN'
        self.__taskInfo = {}
        self.__jobInfos = {}
        self.__jobInfoList = []
        self.__jobGroup = 'unknown'

    def updateTaskInfo(self, taskInfo):
        self.__taskInfo.update(taskInfo)

    def appendJobInfo(self, jobInfo):
        self.__jobInfoList.append(jobInfo)

    def setTaskName(self, taskName):
        self.__taskName = taskName

    def setJobGroup(self, jobGroup):
        self.__jobGroup = jobGroup

    def addTaskJob(self, jobID, subID):
        self.__jobInfos[jobID] = self.__jobInfoList[subID]
        if subID + 1 == len(self.__jobInfoList):
            self.__createTask()

    def __createTask(self):
        result = self.__task.createTask(self.__taskName, self.__taskInfo, self.__jobInfos)
        if not result['OK']:
            logger.warning('Create task failed: %s' % result['Message'])
            return
        self.__taskID = result['Value']
        print('')
        print('The DIRAC task ID   of the submitted jobs is : %s' % self.__taskID)
        print('The DIRAC task name of the submitted jobs is : %s' % self.__taskName)
        print('')

    def getTaskID(self):
        return self.__taskID

    def getTaskName(self):
        return self.__taskName

    def getAllJobGroups(self):
        allJobGroups = []

        rpcClient = RPCClient( "WorkloadManagement/JobMonitoring" )
        result = rpcClient.getProductionIds()
        if result['OK']:
            allJobGroups += result['Value']

        rpcClient = RPCClient( "Accounting/ReportGenerator" )
        result = rpcClient.listUniqueKeyValues( 'Job' )
        if result['OK']:
            allJobGroups += result['Value']['JobGroup']

        return allJobGroups

    def getJobGroup(self):
        return self.__jobGroup


gDiracTask = DiracTask()
