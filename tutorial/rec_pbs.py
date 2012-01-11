app = Boss(version='6.6.0',optsfile='/ihepbatch/cc/zhangxm/BESIII_64/6.6.0/TestRelease/TestRelease-00-00-75/run/jobOptions_rec.txt')
j=Job(application=app, splitter=(eventsPerJob=1, numberOfJobs=2))
#j=Job(application=app, splitter=DiracSplitter(filesPerJob=1, maxFiles=2))
#j=Job(application=app)
#j.inputdata = ["rhopi.rtraw"]
bkq = BKQuery(path = "/bes/user/z/zhangxm", type = "Production");
ds = bkq.getDataset();
#ds = BesDataset(["PFN:/ihepbatch/cc/zhangxm/BESIII_64/6.6.0/TestRelease/TestRelease-00-00-75/run/rhopi_1.rtraw"])
#ds = BesDataset(["LFN:/bes/user/z/zhangxm/0/400/rhopi.rtraw"])
j.inputdata = ds
j.backend=Dirac()
j.backend.settings['BannedSites'] = ['BES.IHEP-PBS.cn']
#j.backend=PBS(queue="gridtbq")
#j.outputdata = ["rhopi.dst"]
j.submit()
