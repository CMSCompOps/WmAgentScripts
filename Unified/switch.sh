#! /bin/bash 

if [ $1 == 'kill' ]
then
        acrontab -r
elif [ $1 == 'reset' ]
then
        curl https://raw.githubusercontent.com/CMSCompOps/WmAgentScripts/master/acrontab.list | acrontab
fi
