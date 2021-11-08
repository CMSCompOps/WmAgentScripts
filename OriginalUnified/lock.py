#!/usr/bin/env python
import sys
from utils import lockInfo

LI = lockInfo( andwrite = False )
item = sys.argv[1]
reason = sys.argv[2] if  len(sys.argv)>2 else ''

LI.lock( item, reason = reason )
