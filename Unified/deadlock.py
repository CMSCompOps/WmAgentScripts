import glob
import os
import socket

from utils import UnifiedLock
UL = UnifiedLock(acquire=False)
UL.deadlock()

## get rid of deadlock in mongodb
from utils import moduleLock
mlock = moduleLock(component='deadlock')
mlock.check()
