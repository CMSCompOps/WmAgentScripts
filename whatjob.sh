date 

#echo "################ QUOTA #######################"
#eos quota ls -u cmsvoc /eos/cms/store/unified

for each in 0268 0269 0272 0273 0274 0275; do
    echo
    echo "#############################################" 
    echo "Set on node" $each 
    echo "################ CRON #######################"
    acrontab -l | grep $each 
    echo "############## UPTIME #######################"
    ssh  vocms$each "uptime"
    echo "############## RUNNING ######################"
    ssh  vocms$each "ps -e -f | grep Uni| grep -v grep"

    ssh vocms$each python /data/unified/WmAgentScripts/Unified/deadlock.py
done
