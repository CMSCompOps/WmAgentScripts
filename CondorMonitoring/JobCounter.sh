#!/bin/sh

# Script run in acrontab cmst1
# 5,20,35,50 * * * * lxplus ssh vocms202 /afs/cern.ch/user/c/cmst1/CondorMonitoring/JobCounter.sh &> /dev/null
# outputfile CondorMonitoring.json CondorJobs_Workflows.json Running*.txt Pending*.txt per Type of job 
# outputdir /afs/cern.ch/user/c/cmst1/www/CondorMonitoring/

location="/afs/cern.ch/user/c/cmst1/CondorMonitoring"
outputdir="/afs/cern.ch/user/c/cmst1/www/CondorMonitoring/"

#Set environment 
source /data/admin/wmagent/env.sh
source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh
cd $location

#Email if things are running slowly
if [ -f scriptRunning.run ];
then
    echo "Last JobCounter.sh is currently running. Will send an email to the admin."
    SUBJECT="[Monitoring] CondorMonitoring running slowly"
    EMAIL="luis89@fnal.gov"
    touch ./emailmessage.txt
    echo "Hi, Condor monitoring script is running slowly at:" > ./emailmessage.txt
    echo $location >> ./emailmessage.txt
    /bin/mail -s "$SUBJECT" "$EMAIL" < ./emailmessage.txt
    rm ./emailmessage.txt
else
    echo "JobCounter.sh started succesfully"
    touch scriptRunning.run
fi

#Run the script
python JobCounter.py &> JobCounter.log
exitstatus="$?"
echo "JobCounter.py exit status: $exitstatus"
cp *.json $outputdir
cp *.txt $outputdir
rm scriptRunning.run