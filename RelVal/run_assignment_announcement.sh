source /cvmfs/grid.cern.ch/emi-ui-3.7.3-1_sl6v2/etc/profile.d/setup-emi3-ui-example.sh
export X509_USER_PROXY=/tmp/x509up_u13536
python2.6 -u input_dset_checker.py 2>&1 | tee input_dset_checker_log.dat >& /dev/null &
python2.6 -u assignment_loop.py 2>&1 | tee assignment_loop_log.dat >& /dev/null &
python2.6 -u announcement_loop.py 2>&1 | tee announcement_loop_log.dat >& /dev/null &
