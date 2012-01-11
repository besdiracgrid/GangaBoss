app = Boss(version='6.6.0',optsfile='/ihepbatch/cc/zhangxm/BESIII_64/6.6.0/TestRelease/TestRelease-00-00-75/run/jobOptions_sim_psip.txt')
#j=Job(application=app, splitter=BossSplitter(evtMaxPerJob = 50))
j=Job(application=app)
j.backend=PBS(queue="gridtbq")
j.inputsandbox.append("/ihepbatch/cc/zhangxm/BESIII_64/6.6.0/TestRelease/TestRelease-00-00-75/run/psip.dec")
j.inputsandbox.append("/ihepbatch/cc/zhangxm/BESIII_64/6.6.0/TestRelease/TestRelease-00-00-75/run/pdt.table")
#j.outputdata = ["rhopi.rtraw"]
j.submit()
