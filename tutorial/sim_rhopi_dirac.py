app = Boss(version='6.6.4',optsfile='jobOptions_sim.txt')

j=Job(application=app)
j.backend=Dirac()
j.inputsandbox.append("rhopi.dec")
j.backend.settings['BannedSites'] = ['BES.NSCCSZ.cn', 'BES.IHEP-LCG.cn']
#j.backend.settings['Destination'] = ['BES.USTC.cn']

j.submit()
