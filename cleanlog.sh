for each in `ls /afs/cern.ch/user/c/cmst2/www/unified/logs/*/*.log` ; do
    age=$((($(date +%s)-$(date +%s -r $each))/86400))
    if [ $age -gt 10 ] ; then
	echo remove $each
	rm -f $each
    fi
done