BASE_DIR=/data/unified/WmAgentScripts/
# shellcheck disable=SC2034
HTML_DIR=/var/www/html/unified/

lock_name=$1


if [ -r "$lock_name" ] ; then
    echo "lock file $lock_name is present"
    echo current id is $$
    lock_id=$(tail -1 "$lock_name")
    echo looking for "$lock_id"
    # shellcheck disable=SC2009
    lock_running=$(ps -e -f | grep " $lock_id " | grep -c -v grep)
    # shellcheck disable=SC2009
    ps -e -f | grep " $lock_id " | grep -v grep
    echo "$lock_running"
    if [ "$lock_running" == "0" ] ; then
	echo "The cycle is locked but $lock_id is not running. Lifting the lock"
    # shellcheck disable=SC2009
	ps -e -f | grep Unified
    # shellcheck disable=SC2002
	cat "$lock_name" | mail -s "[Ops] Emergency On Cycle Lock. Unified isn't running." cmsunified@cern.ch
	rm -f "$lock_name"
    else
	echo "cycle is locked"
	echo "$lock_id, is running"
    # shellcheck disable=SC2009
	ps -e -f | grep Unified
	exit
    fi
else
    echo "no lock file $lock_name, cycle can run"
fi

if [ ! -r $BASE_DIR/credentials.sh ] ; then
    echo "Cannot read simple files" | mail -s "[Ops] read permission" cmsunified@cern.ch
    exit
fi

echo "$lock_name" > "$lock_name"
# shellcheck disable=SC2005,SC2129
echo "$(date)" >> "$lock_name"
echo "$HOSTNAME" >> "$lock_name"
echo $$ >> "$lock_name"

## set sso cookie and grid proxy locations: not creating them
# shellcheck disable=SC1090
source $BASE_DIR/credentials.sh


