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
        self.__jobInfos = []

    def updateTaskInfo(self, taskInfo):
        self.__taskInfo.update(taskInfo)

    def appendJobInfo(self, jobInfo):
        self.__jobInfos.append(jobInfo)

    def createTask(self, taskName):
        self.__taskName = taskName
        result = self.__task.createTask(taskName, self.__taskInfo)
        if not result['OK']:
            logger.warning('Create task failed: %s' % result['Message'])
            return
        taskID = result['Value']
        self.__taskID = taskID

    def addTaskJob(self, jobID, subID):
        if self.__taskID == 0:
            return
        result = self.__task.addTaskJob(self.__taskID, jobID, self.__jobInfos[subID])
        if not result['OK']:
            logger.warning('Add task job failed: %s' % result['Message'])
            return
        if subID + 1 == len(self.__jobInfos):
            self.__task.activateTask(self.__taskID)

    def refreshTaskInfo(self):
        if self.__taskID == 0:
            return
        self.__task.updateTaskInfo(self.__taskID, self.__taskInfo)

    def getTaskID(self):
        return self.__taskID

    def getTaskName(self):
        return self.__taskName


gDiracTask = DiracTask()
