source set_environment.sh
python2.6 -u input_dset_checker.py 2>&1 | tee input_dset_checker_log.dat >& /dev/null &
python2.6 -u assignment_loop.py 2>&1 | tee assignment_loop_log.dat >& /dev/null &
python2.6 -u announcement_loop.py 2>&1 | tee announcement_loop_log.dat >& /dev/null &
python2.6 -u renew_kerberos_voms_loop.py 2>&1 | tee renew_kerberos_voms_loop_log.dat >& /dev/null &
