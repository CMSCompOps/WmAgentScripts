#get all of the log files for a given workflow

tarfile=$1
targzfile=$2

found_log_dir=false

if ! nsls $tarfile >& /dev/null
    then
    echo "tar file does not exist on castor, exiting"
    exit
fi

xrdcp root://castorcms/${tarfile} .

tar xpf `echo ${tarfile} | awk -F/ '{print $12}'`

if ! ls WMTaskSpace/logCollect1/${targzfile} >& /dev/null
    then 
    echo "tar.gz file does not exist, exiting"
    exit
fi

cp WMTaskSpace/logCollect1/${targzfile} ~/webpage/log_tar_gz_files/
