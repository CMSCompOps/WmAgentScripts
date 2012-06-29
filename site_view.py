#!/usr/bin/env python
#
# Sara Alderweireldt (sara.alderweireldt@cern.ch)
# 2012-06-21
#
# parsing site_view table from cms dashboard

import os

tmpdir='/tmp/cmst1'

# helper function for parsing entries
def json_read(jstring):
	jin = jstring.strip('{').strip('}')
	jarr = jin.split(', ')
	d1 = {}
	for j in jarr:
		key,value = j.split(': ')
		d1[key.strip('\'')] = value.strip('\'')
	return d1

# get file
if os.path.exists(os.path.join(tmpdir,'site_view.txt')):
	os.remove(os.path.join(tmpdir,'site_view.txt'))
os.system('curl -H \'Accept:text/csv\' http://dashb-ssb.cern.ch/dashboard/request.py/getall > '+os.path.join(tmpdir,'site_view.txt'))
os.chmod(os.path.join(tmpdir,'site_view.txt'),0755)

# parse file
fin = open(os.path.join(tmpdir,'site_view.txt'),'r+')
entries = []
for lno,l in enumerate(fin):
	entry = {}
	if lno == 0:
		headers = l.strip().split(',')
	else:
		fields = l.strip().split(',"')
		for i in range(1,len(fields)):
			fields[i] = '"'+fields[i]	
		for i,h in enumerate(headers):
			entry[h] = fields[i]
		entries.append(entry)
fin.close()

headers.remove(headers[15])
widths = [20,11,11,8,8,11,11,7,7,12,12,10,12,12,12,12,200]

# write summary
fout = open(os.path.join(tmpdir,'site_view.summary'),'w+')

for hno,h in enumerate(headers):
	fout.write( ' %-*s |' % (widths[hno],h[0:12]), )
fout.write( ' %-*s |' % (widths[hno],"Savannah URL"), )
fout.write( '\n' )
for w in widths:
	fout.write( '-'*w+'-+', )
fout.write( '\n' )

for lno in range(len(entries)):
	for hno,h in enumerate(headers):
		if hno == 0:
			fout.write( ' %-*s |' % (widths[hno],entries[lno][h]), )
		else:
			dic1 = json_read(entries[lno][h][1:-1])
			fout.write( ' %-*s |' % (widths[hno],dic1['Status']), )
			if h == "Savannah CMS":
				dicsave = dic1
	fout.write( ' %-*s |' % (widths[hno+1],dicsave['URL']), )
	fout.write( '\n' )

fout.close()

# cleaning
os.remove(os.path.join(tmpdir,'site_view.txt'))
