GangaBoss CHANGELOG
===================

v1.0.7.2 (2015-02-11)
---------------------

### New Feature
- Support SL6 and BOSS 6.6.5
- Use seperated directory for Gaudi and copy gaudirun.py script there
- Change the directory structure of Ganga

### Bug
- Fix bug of "Replace" method of local random trigger

v1.0.7.1 (2015-01-26)
---------------------

### New Feature
- Support output ".rec" file of reconstruction
- Support output multi-files if needed
- User can specify the output file to get event number of each job

v1.0.7 (2015-01-14)
-------------------

### New Feature
- Support using local random trigger file for reconstruction on specified site
- Support using user's workarea
- Support simulation + reconstruction + analysis job
- Automatically upload specified files to SE and download while running
- Use patch script to do some more prepare work before running BOSS
- Automatically apply patch for different BOSS version
- Check envirionment (cvmfs) in the beginning of job execution
- Add dataset fullpath to fit new DIRAC version
- User can set random seed in the splitter
- Add a FakeSplitterByRun for debug. Each run has same event number
