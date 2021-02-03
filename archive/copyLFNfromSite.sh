#!/bin/bash
if [[ "$1" == T1* ]]; then 	
	CURL_URL="https://cmsweb.cern.ch/phedex/datasvc/xml/prod/lfn2pfn?node=$1_Buffer&protocol=srmv2&lfn=$2"
else
	CURL_URL="https://cmsweb.cern.ch/phedex/datasvc/xml/prod/lfn2pfn?node=$1&protocol=srmv2&lfn=$2"
fi
echo $CURL_URL
file=$(curl -s --insecure -X GET $CURL_URL | awk -F"'" '{print $32}')
echo $file
lcg-cp -v -n 1 $file file:////`pwd`/$3
