import glob
import os
import socket

print("cleaning unified locks")
from utils import UnifiedLock
UL = UnifiedLock(acquire=False)
UL.deadlock()

print("cleaning module locks")
## get rid of deadlock in mongodb
from utils import moduleLock
mlock = moduleLock(component='deadlock')
mlock.check()

print("purging cache info")
from utils import cacheInfo
cache = cacheInfo()
cache.purge()
