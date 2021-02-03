#!/bin/bash

module=$1
filename=`ls -tr ../www/logs/$module/*.log | tail -1`
#echo $module
echo $filename
tail   $filename
