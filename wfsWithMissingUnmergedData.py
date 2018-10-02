#!/usr/bin/env python
"""
Script used for querying all the active workflows in wmstats and matching their
output LFNBases against a list of unmerged files (that were lost somewhere).
It will print out the request name, its status and the LFN base path to the
files missing.

Note: you need to setup your grid proxy (X509_USER_PROXY) before running it
"""
from __future__ import print_function

import argparse
import os
import sys

import requests


def getData():
    """
    Query wmstats REST API for all active requests, their statuses and their
    list of LFNBases
    """
    wmstatsurl = "https://cmsweb.cern.ch/wmstatsserver/data/filtered_requests"
    query = "?mask=OutputModulesLFNBases&mask=RequestStatus"
    r = requests.get("%s%s" % (wmstatsurl, query),
                     cert=os.getenv('X509_USER_PROXY'),
                     verify=False,
                     headers={"Accept": "application/json"})
    data = r.json()['result']
    return data


def getUniqueLFNBases(filesMissing):
    """
    Given a list of unmerged files, truncate their absolute path and return
    a unique list of common LFN bases. E.g.:
    """
    uniqueLFNs = set()
    for line in filesMissing:
        a = line.split('/')
        b = '/'.join(a[:-2])
        uniqueLFNs.add(b)
    return uniqueLFNs


def getFiles(filePath):
    """"
    Read an input file and create a unique list of unmerged files
    """
    listFiles = set()
    with open(filePath) as fp:
        for line in fp.readlines():
            listFiles.add(line.rstrip('\n'))
    return listFiles


def findWF(data, lfn, out):
    """
    Scans the wmstats active data against the LFN base provided,
    if found, print out the request name, status and lfn.
    """
    for info in data:
        if info["OutputModulesLFNBases"]:
            if lfn in info.get("OutputModulesLFNBases", []):
                print(info["RequestName"], info["RequestStatus"])
                out.write("%s %s  %s\n" % (info["RequestName"], info["RequestStatus"], lfn))


def main():
    parser = argparse.ArgumentParser(description="Map unmerged files to workflows and their statuses")
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-u', '--unmerged', help='A single unmerged file name')
    group.add_argument('-i', '--inputFile', help='Plain text file containing unmerged file names (one per line)')
    args = parser.parse_args()

    if args.unmerged:
        listOfFiles = [args.unmerged]
    elif args.inputFile:
        listOfFiles = getFiles(args.inputFile)
    else:
        parser.error("You must provide either an unmerged or an input file name.")
        sys.exit(1)

    if not os.getenv('X509_USER_PROXY'):
        print("You must create a user grid proxy and set the X509_USER_PROXY env var")
        sys.exit(2)

    wmstatsData = getData()
    print("Found a total of %i active workflows in the system" % len(wmstatsData))

    print("List of missing files contain %i files" % len(listOfFiles))
    uniqueLFNs = getUniqueLFNBases(listOfFiles)
    print("Unique missing LFNBases: %s" % len(uniqueLFNs))

    with open("wfs_with_missing_files.txt", "w") as out:
        for lfn in uniqueLFNs:
            findWF(wmstatsData, lfn, out)


if __name__ == '__main__':
    sys.exit(main())
