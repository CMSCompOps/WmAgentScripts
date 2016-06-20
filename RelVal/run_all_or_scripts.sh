#make sure that only one instance of this script is running at a time
if [ -f /home/relval/run_all_or_scripts_is_running ]; then
    exit
fi

touch /home/relval/run_all_or_scripts_is_running
source /home/relval/WmAgentScripts/RelVal/set_environment.sh;
python2.6 -u /home/relval/WmAgentScripts/RelVal/batch_killor.py 2>&1 | tee >> /home/relval/WmAgentScripts/RelVal/batch_killor_log.dat;
python2.6 -u /home/relval/WmAgentScripts/RelVal/batch_clonor.py 2>&1 | tee >> /home/relval/WmAgentScripts/RelVal/batch_clonor_log.dat;
python2.6 -u /home/relval/WmAgentScripts/RelVal/kerberos_voms_renewor.py 2>&1 | tee >> /home/relval/WmAgentScripts/RelVal/kerberos_voms_renewor_log.dat;
python2.6 -u /home/relval/WmAgentScripts/RelVal/input_dset_checkor.py 2>&1 | tee >> /home/relval/WmAgentScripts/RelVal/input_dset_checkor_log.dat;
python2.6 -u /home/relval/WmAgentScripts/RelVal/assignor.py 2>&1 | tee >> /home/relval/WmAgentScripts/RelVal/assignor_log.dat;
#do this multiple times because often there are temporary errors interacting with cmsweb
python2.6 -u /home/relval/WmAgentScripts/RelVal/announcor.py 2>&1 | tee >> /home/relval/WmAgentScripts/RelVal/announcor_log.dat;
python2.6 -u /home/relval/WmAgentScripts/RelVal/announcor.py 2>&1 | tee >> /home/relval/WmAgentScripts/RelVal/announcor_log.dat;
python2.6 -u /home/relval/WmAgentScripts/RelVal/announcor.py 2>&1 | tee >> /home/relval/WmAgentScripts/RelVal/announcor_log.dat;
rm /home/relval/run_all_or_scripts_is_running
