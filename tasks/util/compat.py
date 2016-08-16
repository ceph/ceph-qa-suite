"""
Compatibility module for supporting Python 2 and Python 3 in Ceph QA Suite
"""

import sys

PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3

if PY2:
    from itertools import izip, imap
    from cStringIO import StringIO

    string = basestring
    str = unicode
    zip = izip
    range = xrange
    map = imap
    cmp = cmp


elif PY3:
    from io import StringIO

    string = str
    str = str
    zip = zip
    range = range
    map = map
    cmp = lambda a, b: (a > b) - (a < b)
