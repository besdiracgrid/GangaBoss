from Ganga.Core import GangaException

from DIRAC.Core.DISET.RPCClient                      import RPCClient

#logger = Ganga.Utility.logging.getLogger()

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
            raise GangaException('Create task failed: %s' % result['Message'])
        taskID = result['Value']
        self.__taskID = taskID

    def addTaskJob(self, jobID, subID):
        result = self.__task.addTaskJob(self.__taskID, jobID, self.__jobInfos[subID])
        if not result['OK']:
            raise GangaException('Add task job failed: %s' % result['Message'])
        if subID + 1 == len(self.__jobInfos):
            self.__task.activateTask(self.__taskID)

    def refreshTaskInfo(self):
        self.__task.updateTaskInfo(self.__taskID, self.__taskInfo)

    def getTaskID(self):
        return self.__taskID

    def getTaskName(self):
        return self.__taskName


gDiracTask = DiracTask()
