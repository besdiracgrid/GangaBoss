# ganga job for simulation and reconstruction
bossVersion = '6.6.4.p01'
optionsFile = 'jobOptions_sim_bhabha.txt'
recOptionsFile = 'jobOptions_rec_bhabha.txt'
jobGroup = 'simrec_bhabha_140512'
metadata = {'resonance': '4360', 'eventType': 'bhabha'}
splitter = UserSplitterByRun(evtMaxPerJob = 100, evtTotal = 100*10)

app = Boss(version=bossVersion, optsfile=optionsFile, recoptsfile=recOptionsFile, metadata=metadata)
j = Job(application=app, splitter=splitter)

j.backend = Dirac()
j.backend.settings['JobGroup'] = jobGroup
j.submit()

print '\nThe DFC path for output data is:'
print app.get_output_dir()
print '\nThe dataset name for output data is:'
print app.get_dataset_name()
