app = Boss(version='6.6.0',user_release_area='/home/cc/zhangxm/BESIII_64/6.6.0/',optsfile='/ihepbatch/cc/zhangxm/BESIII_64/6.6.0/TestRelease/TestRelease-00-00-75/run/jobOptions_ana_pipijpsi.txt')
j=Job(application=app, splitter=SplitByFiles(filesPerJob=1, maxFiles=2))
#bkq = BKQuery(path = "/bes/user/z/zhangxm", type = "Production");
#ds = bkq.getDataset();
ds = BesDataset(["PFN:/home/cms/zhangxm/gangadir/259/0/outputdata/MC1000_round001_run8106_file0001.dst","PFN:/home/cms/zhangxm/gangadir/260/0/outputdata/MC1000_round001_run8106_file0001.dst"])
j.inputdata = ds
j.backend=PBS(queue="gridtbq")
#j.outputdata = ["pipijpsi_ana.root"]
j.submit()
