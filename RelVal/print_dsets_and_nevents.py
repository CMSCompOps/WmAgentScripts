#!/usr/bin/env python
import sys
import optparse
import os

def print_dsets_and_nevents(dsets_nevents_list,output_file_name):

    #get the maximum length of a dataset name
    max_dset_length = -1

    output_file = open(output_file_name, 'w') 

    for dset_nevents in dsets_nevents_list:
        if len(dset_nevents[0]) > max_dset_length:
            max_dset_length = len(dset_nevents[0])

    output_file.write(('-'*(max_dset_length + 14)) + '\n')
    output_file.write("| Datasets" + (' '*(max_dset_length -7)) + "|  Events |" + '\n')
    output_file.write(('-'*(max_dset_length + 14)) + '\n')

    L = []
     
    for dset_nevents in dsets_nevents_list:
        if len(str(dset_nevents[1])) > 9:
            os.system('echo '+dset_nevents[0]+' | mail -s \"print_dsets_and_nevents.py error 2\" andrew.m.levin@vanderbilt.edu')
            sys.exit(1)
        found = False
        for x in L:
            if x == dset_nevents[0]:
                found = True
                os.system('echo '+dset_nevents[0]+' | mail -s \"print_dsets_and_nevents.py error 1\" andrew.m.levin@vanderbilt.edu')
                sys.exit(1)
             
        if not found:
            L.append(dset_nevents[0])
            output_file.write("|"+dset_nevents[0] + (' '*(max_dset_length - len(dset_nevents[0])+2)) + "|" + (' '*(8-len(str(dset_nevents[1])))) + str(dset_nevents[1]) + " |"+ '\n')

    output_file.write(('-'*(max_dset_length + 14))+ '\n')

def main():

    parser = optparse.OptionParser()
    (options,args) = parser.parse_args()

    if not len(args)==1:
        print "usage: python2.6 print_dsets_and_nevents.py <inputFile_containing_list_of_datasets_and_number_of_events>"

    dsets_nevents_list = []    
        
    inputFile=args[0]    
    f = open(inputFile, 'r')
    for line in f:
        line = line.rstrip('\n')
        dsets_nevents_list.append([line.split(' ')[0],line.split(' ')[1]])

    print_dsets_and_nevents(dsets_nevents_list,"delete_this_statistics.txt")    

if __name__ == "__main__":
    main()
