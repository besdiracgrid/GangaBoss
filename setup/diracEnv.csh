setenv LD_LIBRARY_PATH $DIRAC_INSTALL_PATH/Linux_x86_64_glibc-2.5/lib:$DIRAC_INSTALL_PATH/Linux_x86_64_glibc-2.5/lib/mysql:$LD_LIBRARY_PATH
setenv PATH $DIRAC_INSTALL_PATH/scripts:$DIRAC_INSTALL_PATH/Linux_x86_64_glibc-2.5/bin:/opt/glite/bin:/opt/glite/externals/bin:/opt/lcg/bin:/opt/lcg/sbin:/opt/globus/sbin:/opt/globus/bin:/opt/gpt/sbin:/opt/d-cache//srm/bin:/opt/d-cache//dcap/bin:$PATH
setenv PYTHONPATH ${DIRAC_INSTALL_PATH}:$DIRAC_INSTALL_PATH/Linux_x86_64_glibc-2.5/lib/python2.6/site-packages:$PYTHONPATH
