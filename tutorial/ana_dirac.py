app = Boss(version='6.6.0',user_release_area='/home/cc/zhangxm/BESIII_64/6.6.0/',optsfile='/ihepbatch/cc/zhangxm/BESIII_64/6.6.0/TestRelease/TestRelease-00-00-75/run/jobOptions_ana_pipijpsi.txt')
j=Job(application=app, splitter=DiracSplitter(filesPerJob=1, maxFiles=2))
#bkq = BKQuery(path = "/bes/user/z/zhangxm", type = "Production");
#ds = bkq.getDataset();
ds = BesDataset(["LFN:/bes/user/z/zhangxm/0/138/655_psip_stream001_run8093_file0004.dst","LFN:/bes/user/z/zhangxm/0/139/655_psip_stream001_run8093_file0005.dst"])
j.inputdata = ds
j.backend=Dirac()
j.backend.settings['BannedSites'] = ['BES.IHEP-PBS.cn']
#j.outputdata = ['pipijpsi_ana.root']
j.submit()
