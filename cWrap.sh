BASE_DIR=/data/unified/WmAgentScripts/

HTML_DIR=/data/unified/www
FINAL_HTML_DIR=/eos/cms/store/unified/www/

if [ ! -d $FINAL_HTML_DIR ] ; then 
    echo "Cannot read the log destination",$FINAL_HTML_DIR
    exit
fi
cd $BASE_DIR||exit

modulename=$(echo "$1" | sed 's/\.py//' | sed 's/Unified\///')
mkdir -p $HTML_DIR/logs/"$modulename"/
env EOS_MGM_URL=root://eoscms.cern.ch eos mkdir -p $FINAL_HTML_DIR/logs/"$modulename"/

last_log=$HTML_DIR/logs/$modulename/last.log
s_dated_log=$modulename/$(date +%F_%T).log
dated_log=$HTML_DIR/logs/$s_dated_log
log=$dated_log

# shellcheck disable=SC2005
echo "$(date)" > "$log"
echo $$ >> "$log"

if [ -r unified_drain ] ; then
    echo "System is locally draining" >> "$log"
    cp "$log" "$last_log"
    env EOS_MGM_URL=root://eoscms.cern.ch eos cp "$log" $FINAL_HTML_DIR/logs/"$modulename"/.
    env EOS_MGM_URL=root://eoscms.cern.ch eos cp "$log" $FINAL_HTML_DIR/logs/"$modulename"/last.log
    exit
fi
if [ -r /eos/cms/store/unified/unified_drain ] ; then
    echo "System is globally draining" >> "$log"
    cp "$log" "$last_log"
    env EOS_MGM_URL=root://eoscms.cern.ch eos cp "$log" $FINAL_HTML_DIR/logs/"$modulename"/.
    env EOS_MGM_URL=root://eoscms.cern.ch eos cp "$log" $FINAL_HTML_DIR/logs/"$modulename"/last.log
    exit
fi
# shellcheck disable=SC2129
echo "$USER" >> "$log"
echo "$HOSTNAME" >> "$log"
echo module "$modulename">> "$log" 

# shellcheck disable=SC1091
source ./set.sh

echo >> "$log"

start=$(date +%s)
python ssi.py "$modulename" "$start"

python "$@" &>> "$log"

# shellcheck disable=SC2181
if [ $? == 0 ]; then
    echo "finished" >> "$log"
else
    echo -e "\nAbnormal termination with exit code $?" >> "$log"
    top -n1  -o %MEM -c >> "$log"
    
    emaillog=$log.txt
    failed_pid=$!
    echo "Abnormal termination, check $log" > "$emaillog"
    # shellcheck disable=SC2129
    echo "https://cms-unified.web.cern.ch/cms-unified/logs/$s_dated_log" >> "$emaillog"
    echo $failed_pid >> "$emaillog"
    echo "$USER" >> "$emaillog"
    echo "$HOSTNAME" >> "$emaillog"
    echo -e "module $modulename \n" >> "$emaillog" 
    tail "$log" >> "$emaillog"
    # shellcheck disable=SC2002
    cat "$emaillog" | mail -s "[Ops] module $modulename failed" cmsunified@cern.ch
fi

stop=$(date +%s)
python ssi.py "$modulename $start $stop"
# shellcheck disable=SC2005
echo "$(date)" >> "$log"

#cp $log $dated_log
cp "$log" "$last_log"
env EOS_MGM_URL=root://eoscms.cern.ch eos cp "$log" $FINAL_HTML_DIR/logs/"$modulename"/.
env EOS_MGM_URL=root://eoscms.cern.ch eos cp "$log" $FINAL_HTML_DIR/logs/"$modulename"/last.log

# rm $log
