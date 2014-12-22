# ganga job for simulation and reconstruction
bossVersion = '6.6.4.p02'
optionsFile = 'jobOptions_sim_rhopi.txt'
recOptionsFile = 'jobOptions_rec_rhopi.txt'
anaOptionsFile = 'jobOptions_ana_rhopi.txt'
jobGroup = 'simrecana_rhopi_140512'
metadata = {'resonance': 'jpsi', 'eventType': 'rhopi'}
splitter = UserSplitterByRun(evtMaxPerJob = 100, evtTotal = 100*10)

app = Boss(version=bossVersion, optsfile=optionsFile, recoptsfile=recOptionsFile, anaoptsfile=anaOptionsFile, metadata=metadata)
j = Job(application=app, splitter=splitter)

j.backend = Dirac()
j.backend.settings['JobGroup'] = jobGroup
# add lib files needed for the analysis
j.inputsandbox.append('/ihepbatch/bes/zhaoxh/boss/664p02/workarea/InstallArea/x86_64-slc5-gcc43-opt/lib/libRhopiAlg.so')
j.submit()

print '\nThe DFC path for output data is:'
print app.get_output_dir()
print '\nThe dataset name for output data is:'
print app.get_dataset_name()
