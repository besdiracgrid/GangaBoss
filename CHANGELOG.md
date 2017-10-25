GangaBoss CHANGELOG
===================

v1.2.1 (2017-10-25)
-------------------

### Bug
- Fix user output_dir empty

v1.2.0 (2017-10-25)
-------------------

### New Feature
- Upload user output to local lustre

v1.1.1 (2017-09-06)
-------------------

### Bug
- Quick fix bug when using BOSS 7.0.2.p02

v1.1.0 (2016-08-03)
-------------------

### Change
- "addDataset" arguments changed in the new DIRAC version
- "auto" SE will choose from the CS

v1.0.9.3 (2016-01-05)
---------------------

### New Feature
- Add "SeedStart" in task information
- Select multiple SEs from the DIRAC CS

v1.0.9.2 (2015-11-08)
---------------------

### Bug
- Add the line about sqlite path in the job option

v1.0.9.1 (2015-11-02)
---------------------

### Change
- Change the sqlite database file path

v1.0.9 (2015-08-08)
---------------------

### New Feature
- Use the CS to get default sites list
- Output the task ID after submission

### Change
- Use setPlatform instead of setSystemConfig for new DIRAC version

v1.0.8 (2015-06-08)
---------------------

### New Feature
- Add DIRAC task manager support
- Use opts instead of pkl format for job options
- Add bes CVMFS repository and choose the best one automatically
- Restrict boss log size

### Bug
- Fix bug of analysis jobs when not "FILE1"

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
- Support using user workarea
- Support simulation + reconstruction + analysis job
- Automatically upload specified files to SE and download while running
- Use patch script to do some more prepare work before running BOSS
- Automatically apply patch for different BOSS version
- Check envirionment (cvmfs) in the beginning of job execution
- Add dataset fullpath to fit new DIRAC version
- User can set random seed in the splitter
- Add a FakeSplitterByRun for debug. Each run has same event number
