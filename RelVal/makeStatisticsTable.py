#!/usr/bin/env python
import sys

if len(sys.argv) != 3:
     print "Usage:"
     print "python2.6 makeStatisticsTable.py input_file_name output_file_name"
     sys.exit()

#get the maximum length of a dataset name
max = -1

output_file = open(sys.argv[2], 'w') 

for line in open(sys.argv[1]):
     parts = line.split('\t')
     if len(parts[0]) > max:
          max = len(parts[0])

long_dash_string = "------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------"

long_empty_string = "                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               "


output_file.write(long_dash_string[1:max + 15] + '\n')
output_file.write("| Datasets" + long_empty_string[1:max -6] + "|  Events |" + '\n')
output_file.write(long_dash_string[1:max + 15] + '\n')

if len(long_empty_string) <= max:
     print "ERROR: len(long_empty_string) >= max"
     exit()

L = []
     
for line in open(sys.argv[1]):
    parts = line.split('\t')
    if len(parts[1]) > 9:
           print "ERROR: len(parts[1]) > 9"
           exit()
    found = False       
    for x in L:
        if x == parts[0]:
             found = True
             print "duplicate dataset: " + parts[0]
             
    if not found:
        L.append(parts[0])
        output_file.write("|"+parts[0] + long_empty_string[1:max - len(parts[0])+3] + "|" + long_empty_string[0:9-len(parts[1])]+ parts[1][0:-1] + " |"+ '\n')

output_file.write(long_dash_string[1:max + 15]+ '\n')


