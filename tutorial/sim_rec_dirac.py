app = Boss(version='6.6.0',optsfile='/ihepbatch/cc/zhangxm/BESIII_64/6.6.0/TestRelease/TestRelease-00-00-75/run/jobOptions_sim_psip.txt',recoptsfile='/ihepbatch/cc/zhangxm/BESIII_64/6.6.0/TestRelease/TestRelease-00-00-75/run/jobOptions_rec.txt')
#j=Job(application=app, splitter=BossSplitter(evtMaxPerJob = 5000))
j=Job(application=app, splitter=BossSplitter(evtMaxPerJob = 50))
j.backend=Dirac()
j.backend.settings['BannedSites'] = ['BES.IHEP-PBS.cn']
j.inputsandbox.append("/ihepbatch/cc/zhangxm/BESIII_64/6.6.0/TestRelease/TestRelease-00-00-75/run/psip.dec")
j.inputsandbox.append("/ihepbatch/cc/zhangxm/BESIII_64/6.6.0/TestRelease/TestRelease-00-00-75/run/pdt.table")
j.submit()
