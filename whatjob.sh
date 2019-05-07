date 

#echo "################ QUOTA #######################"
#eos quota ls -u cmsvoc /eos/cms/store/unified
nodes="0268 0269 0272 0273 0274 0275"
if [ ! -z $1 ] ; then
    nodes=$1
fi

for each in $nodes; do
    echo
    echo "#############################################" 
    echo "Set on node" $each 
    echo "################ CRON #######################"
    acrontab -l | grep $each 
    echo "############## UPTIME #######################"
    ssh  vocms$each "uptime"
    echo "############## RUNNING ######################"
    ssh  vocms$each "ps -e -f | grep Uni| grep -v grep"

    ssh vocms$each "cd /data/unified/WmAgentScripts ; source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh ; python /data/unified/WmAgentScripts/Unified/deadlock.py"
    
    #ssh vocms$each "sudo chmod g+rw -R /data/unified/"
    #ssh vocms$each "sudo chmod g+rw -R /data/unified-cache/"
    #ssh vocms$each "sudo chmod g+rw -R /data/unified/WmAgentScripts/"
    #ssh vocms$each "sudo chown -R :zh /data/unified/"
    #ssh vocms$each "sudo chown -R :zh /data/unified-cache/"
    ## relevant
    #ssh vocms$each "sudo setfacl -R -m group:zh:rwx /data/unified/"
    #ssh vocms$each "sudo setfacl -R -m group:zh:rwx /data/unified-cache/"
done
