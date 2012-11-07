#!/bin/bash

CURL_URL="https://cmsweb.cern.ch/phedex/datasvc/xml/prod/lfn2pfn?node=$1_Buffer&protocol=srmv2&lfn=$2"

file=$(curl -s --insecure -X GET $CURL_URL | awk -F"'" '{print $32}')
lcg-cp -v $file file:////tmp/delete.me
