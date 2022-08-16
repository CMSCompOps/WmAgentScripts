""""
getACDC_list.py 

Returns the list of ACDCs submitted from the output of the runner.py from autoACDC.
The clean list of ACDC workflows can be very useful to do mass action on them,
especially with other scripts (e.g. abortWorkflows.py, rejectWorkflows.py)

Author: Luca Lavezzo
Date: August 2022
"""

import os, sys
import argparse

def main():
    
	#Create option parser
	parser = argparse.ArgumentParser(description='Famous Submitter')
	parser.add_argument('-f', '--file', help='Text file output of runner.py', required=True)
	parser.add_argument('-o', '--out', help='Output text file with a list of workflows', required=True)
	options = parser.parse_args()

	wfs = [l.strip().split(', ')[1] for l in open(options.file) if l.strip()]

	with open(options.out, 'a') as f: f.write("\n".join(wfs)) 

if __name__ == "__main__":
    main()