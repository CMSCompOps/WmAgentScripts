HTML_DIR=/eos/cms/store/unified/www/
LOCAL_DIR=/data/unified/www/
CACHE_DIR=/data/unified-cache/

echo "cleaning .log files in local dir"
find $LOCAL_DIR/logs/ -type f -name '*.log' -mtime +15 -exec rm {} \;

echo "cleaning .json files in local dir"
find $LOCAL_DIR/logs/ -type f -name '*.json' -mtime +15 -exec rm {} \;

echo "cleaning files in cache dir"
find $CACHE_DIR/ -type f -mtime +15 -exec rm {} \;

echo "cleaning .log files in html dir"
find $HTML_DIR/logs/ -type f -name '*.log' -mtime +15 -exec rm {} \;

echo "cleaning .json files in html dir"
find $HTML_DIR/logs/ -type f -name '*.json' -mtime +15 -exec rm {} \;

