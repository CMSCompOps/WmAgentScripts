"""
File       : SearchTools.py
Author     : Hasan Ozturk <haozturk AT cern dot com>

Description: Class which contains helper functions for searching in general

"""

# Search the given dict for the given key in maximum depth 2. Return values for the matches
def findKeys(key, dictionary):
	values = set()
	for k, v in dictionary.iteritems():
		if type(v) is dict:
			for k2, v2 in v.iteritems():
				if k2 == key:
					values.add(v2)
		elif k == key:
			values.add(v)

	return list(values)


