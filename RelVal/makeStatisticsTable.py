#!/usr/bin/env python
import sys

#if len(sys.argv) != 3:
#     print "Usage:"
#     print "python2.6 makeStatisticsTable.py input_file_name output_file_name"
#     sys.exit()


def makeStatisticsTable(dsets_nevents_list,output_file_name):

    #get the maximum length of a dataset name
    max = -1

    output_file = open(output_file_name, 'w') 

    for dset_nevents in dsets_nevents_list:
        #parts = line.split('\t')
        if len(dset_nevents[0]) > max:
            max = len(dset_nevents[0])

    long_dash_string = "------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------"

    long_empty_string = "                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               "


    output_file.write(long_dash_string[1:max + 15] + '\n')
    output_file.write("| Datasets" + long_empty_string[1:max -6] + "|  Events |" + '\n')
    output_file.write(long_dash_string[1:max + 15] + '\n')

    if len(long_empty_string) <= max:
        print "ERROR: len(long_empty_string) >= max"
        exit()

    L = []
     
    for dset_nevents in dsets_nevents_list:
        #parts = line.split('\t')
        if len(str(dset_nevents[1])) > 9:
            print "ERROR: len(parts[1]) > 9"
            exit()
        found = False       
        for x in L:
            if x == dset_nevents[0]:
                found = True
                print "duplicate dataset: " + dset_nevents[0]
             
        if not found:
            L.append(dset_nevents[0])
            output_file.write("|"+dset_nevents[0] + long_empty_string[1:max - len(dset_nevents[0])+3] + "|" + long_empty_string[0:8-len(str(dset_nevents[1]))]+ str(dset_nevents[1]) + " |"+ '\n')

    output_file.write(long_dash_string[1:max + 15]+ '\n')


