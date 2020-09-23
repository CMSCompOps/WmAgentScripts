#!/bin/bash

module=$1
# shellcheck disable=SC2012
filename=$(ls -tr ../www/logs/"$module"/*.log | tail -1)
# echo $module
echo "$filename"
tail   "$filename"
