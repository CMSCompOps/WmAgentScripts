import sys
from utils import StartStopInfo

ssi = StartStopInfo()
if 'purge' in sys.argv:
    since = int(sys.argv[2])
    ssi.purge(since)
else:
    try:
        component, start, stop = sys.argv[1:]
    except:
        stop = None
        component, start = sys.argv[1:]
    ssi.pushStartStopTime(component, start, stop)
