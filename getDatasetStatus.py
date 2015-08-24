#!/usr/bin/env python
"""
 Gets dataset status (access type) PRODUCTION, VALID, INVALID, DEPRECATED or 
 DELETED
""" 
import sys
import optparse
import dbs3Client as dbs3


def main():
    parser = optparse.OptionParser("pyton %prog [DSET1 DSET2 ... | -f FILE]")
    parser.add_option('-f', '--file', help='Text file', dest='file')
    (options, args) = parser.parse_args()

    if options.file:
        datasets = [l.strip() for l in open(options.file) if l.strip()]
    elif len(args) >= 1:
        datasets = args
    else:
        parser.error("A file name or dataset is required")
        sys.exit(0)
    for dataset in datasets:
        status = dbs3.getDatasetStatus(dataset)
        print dataset, status

if __name__ == "__main__":
    main()
