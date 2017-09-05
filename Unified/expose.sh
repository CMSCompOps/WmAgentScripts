#!/bin/bash

host=`echo $HOSTNAME | cut -d "." -f 1`
echo "looking for condor log on" $host
taskName=$1
wmbsID=$2
code=$3
cluster=${wmbsID:0:3}
loc=$4
short=$5

#end_dir=/afs/cern.ch/user/c/cmst1/www/JobLogs/$taskName/$cluster/$host\_$wmbsID
#end_dir=/afs/cern.ch/user/c/cmst2/www/unified/condorlogs/$taskName/$code/$cluster/$host\_$wmbsID
end_dir=$loc/condorlogs/$taskName/$code/$short/$cluster/$host\_$wmbsID

echo '==================================== Condor log retrieval ============================================='
echo 'LISTING: ' /data/srv/wmagent/current/install/wmagent/JobCreator/JobCache/$taskName/*/job_$wmbsID/
dir=`ls -d /data/srv/wmagent/current/install/wmagent/JobCreator/JobCache/$taskName/*/job_$wmbsID/`
if [ -z $dir ]; then
    echo "no on-going directory"
else
    echo "     Found some logs in" $dir
    echo "Will put it in " $end_dir    
    mkdir -p $end_dir
    cp -r $dir/* $end_dir/.
fi
echo '==================================== Condor log retrieval ============================================='
echo 'LISTING: ' /data/srv/wmagent/current/install/wmagent/JobCreator/JobCache/$taskName/*/*/job_$wmbsID/
dir=`ls -d /data/srv/wmagent/current/install/wmagent/JobCreator/JobCache/$taskName/*/*/job_$wmbsID/`
if [ -z $dir ]; then
    echo "no on-going directory"
else
    echo "      Found some logs in" $dir
    echo "Will put it in " $end_dir    
    mkdir -p $end_dir
    cp -r $dir/* $end_dir/.
fi

echo '==================================== Condor log retrieval ============================================='
echo 'LISTING: ' /data/srv/wmagent/current/install/wmagent/JobArchiver/logDir/${taskName:0:1}/$taskName/JobCluster_$cluster/Job_$wmbsID.tar.bz2
file=`ls /data/srv/wmagent/current/install/wmagent/JobArchiver/logDir/${taskName:0:1}/$taskName/JobCluster_$cluster/Job_$wmbsID.tar.bz2`
if [ -z $file ]; then
    echo "no archived file"
else
    echo "      Found some logs in" $file
    echo "Will put it in " $end_dir    
    mkdir -p  $end_dir
    cd $end_dir
    tar xvf $file
fi

echo '==================================== Condor log retrieval ============================================='
cluster=${wmbsID:0:4}
echo 'LISTING: ' /data/srv/wmagent/current/install/wmagent/JobArchiver/logDir/${taskName:0:1}/$taskName/JobCluster_$cluster/Job_$wmbsID.tar.bz2
file=`ls /data/srv/wmagent/current/install/wmagent/JobArchiver/logDir/${taskName:0:1}/$taskName/JobCluster_$cluster/Job_$wmbsID.tar.bz2`
if [ -z $file ]; then
    echo "no archived file"
else
    echo "      Found some logs in" $file
    echo "Will put it in " $end_dir    
    mkdir -p  $end_dir
    cd $end_dir
    tar xvf $file
fi