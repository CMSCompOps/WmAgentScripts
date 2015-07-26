#make sure that only one instance of this script is running at a time
if [ -f /home/relval/run_assignment_announcement_is_running ]; then
    exit
fi

touch /home/relval/run_assignment_announcement_is_running;
source /home/relval/WmAgentScripts/RelVal/set_environment.sh;
python2.6 -u /home/relval/WmAgentScripts/RelVal/batch_killor.py 2>&1 | tee >> /home/relval/WmAgentScripts/RelVal/batch_killor_log.dat;
python2.6 -u /home/relval/WmAgentScripts/RelVal/batch_clonor.py 2>&1 | tee >> /home/relval/WmAgentScripts/RelVal/batch_clonor_log.dat;
python2.6 -u /home/relval/WmAgentScripts/RelVal/renew_kerberos_voms_loop.py 2>&1 | tee >> /home/relval/WmAgentScripts/RelVal/renew_kerberos_voms_loop_log.dat;
python2.6 -u /home/relval/WmAgentScripts/RelVal/input_dset_checker.py 2>&1 | tee >> /home/relval/WmAgentScripts/RelVal/input_dset_checker_log.dat;
python2.6 -u /home/relval/WmAgentScripts/RelVal/assignment_loop.py 2>&1 | tee >> /home/relval/WmAgentScripts/RelVal/assignment_loop_log.dat;
python2.6 -u /home/relval/WmAgentScripts/RelVal/announcement_loop.py 2>&1 | tee >> /home/relval/WmAgentScripts/RelVal/announcement_loop_log.dat;
rm /home/relval/run_assignment_announcement_is_running;
