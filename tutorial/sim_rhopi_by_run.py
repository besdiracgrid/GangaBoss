# ganga job file
bossVersion = '6.6.4.p01'
optionsFile = 'jobOptions.txt'
decayCard = 'rhopi.dec'
jobGroup = 'yourname_tst_001'
metadata = {'resonance':'jpsi', 
		'eventType':'rhopi',
		'streamId':'stream001',
		'bossVer':bossVersion,
		'optionsFile':optionsFile,
		'decayCard':decayCard,
		'JobGroup':jobGroup,
		}
userSplitter = UserSplitterByRun(evtMaxPerJob = 100, evtTotal = 500,
		metadata=metadata)
app = Boss(version=bossVersion,optsfile=optionsFile)
j=Job(application=app, splitter=userSplitter)

j.backend=Dirac()
j.backend.settings['JobGroup'] = jobGroup
j.backend.settings['BannedSites'] = ['BES.NSCCSZ.cn', 'BES.IHEP-LCG.cn']
j.inputsandbox.append(decayCard)
# add more input files if needed
#j.inputsandbox.append("mypdt.table")
j.submit()

print '\nOutput data will be written to the following directory:'
print 'FC:',
print userSplitter.getFcDir()
print ' '
