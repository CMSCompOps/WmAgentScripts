#get a specific tar.gz file given the .tar castor path name and the .tar.gz filename

tarfile=$1
targzfile=$2

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
