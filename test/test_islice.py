from itertools import islice
a = [x for x in  range(101)]

def slicedIterator(sourceList, sliceSize):
    start = 0
    end = 0

    while len(sourceList) > end:
        end = start + sliceSize
        yield sourceList[start: end]
        start = end

for b in slicedIterator(a, 200):
    print b