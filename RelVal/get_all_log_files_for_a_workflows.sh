#get all of the log files for a given workflow

wfname=$1

found_log_dir=false

mkdir ~/webpage/log_tar_files/${wfname}/

declare -a year_month_dirs

echo "looking for the workflow directory on castor"

for year in `nsls /castor/cern.ch/cms/store/logs/prod/`;
  do
  for month in `nsls /castor/cern.ch/cms/store/logs/prod/${year}/`;
    do
    if nsls /castor/cern.ch/cms/store/logs/prod/${year}/${month}/WMAgent/${wfname}/ >& /dev/null
	then
	year_month_dirs+=(${year}/${month}/)
	found_log_dir=true
    fi
  done
done

if ! $found_log_dir
    then 
    echo "    no directories for the workflow $wfname were found, exiting"
    exit
fi

echo "the workflow was found in the following directories:"

for year_month in ${year_month_dirs[@]}
  do
  echo "   "/castor/cern.ch/cms/store/logs/prod/${year_month}WMAgent/${wfname}/
done

echo "staging the tar files from castor" 

for year_month in ${year_month_dirs[@]}
  do
  for tarball in `nsls /castor/cern.ch/cms/store/logs/prod/${year_month}WMAgent/${wfname}/` 
    do 
    echo /castor/cern.ch/cms/store/logs/prod/${year_month}WMAgent/${wfname}/${tarball}
    xrdcp root://castorcms//castor/cern.ch/cms/store/logs/prod/${year_month}WMAgent/${wfname}/${tarball} .
    mv ${tarball} ~/webpage/log_tar_files/${wfname}/
  done
done
