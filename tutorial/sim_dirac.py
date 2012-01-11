app = Boss(version='6.6.0',optsfile='/ihepbatch/cc/zhangxm/BESIII_64/6.6.0/TestRelease/TestRelease-00-00-75/run/jobOptions_sim_psip.txt')
#app.extraopts="""ApplicationMgr().EvtMax = 2"""
j=Job(application=app)
j.backend=Dirac()
#j.backend.settings['BannedSites'] = ['BES.IHEP-PBS.cn','BES.GUCAS.cn']
j.backend.settings['BannedSites'] = ['BES.IHEP-PBS.cn']
j.inputsandbox.append("/ihepbatch/cc/zhangxm/BESIII_64/6.6.0/TestRelease/TestRelease-00-00-75/run/psip.dec")
j.inputsandbox.append("/ihepbatch/cc/zhangxm/BESIII_64/6.6.0/TestRelease/TestRelease-00-00-75/run/pdt.table")
#j.outputdata = ["rhopi.rtraw"]
j.submit()
