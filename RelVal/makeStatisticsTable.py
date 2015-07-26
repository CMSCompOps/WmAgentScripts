#!/usr/bin/env python
import sys
import optparse

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

def main():

    parser = optparse.OptionParser()
    (options,args) = parser.parse_args()

    if not len(args)==1:
        print "usage: python2.6 makeStatisticsTable.py <inputFile_containing_list_of_datasets_and_number_of_events>"

    dsets_nevents_list = []    
        
    inputFile=args[0]    
    f = open(inputFile, 'r')
    for line in f:
        line = line.rstrip('\n')
        dsets_nevents_list.append([line.split(' ')[0],line.split(' ')[1]])

    makeStatisticsTable(dsets_nevents_list,"delete_this_statistics.txt")    

if __name__ == "__main__":
    main()

