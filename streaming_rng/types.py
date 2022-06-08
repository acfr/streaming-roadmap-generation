# Py 2/3 compat imports
from __future__ import print_function, division, absolute_import
from future.builtins import dict, list, object, range, str, bytes, filter, map, zip, ascii, chr, hex, input, next, oct, open, pow, round, super
from future.standard_library import install_aliases; install_aliases()
# This file is compatible with Python 2 and Python 3, please keep it so

__author__ = "Konstantin Seiler, The University of Sydney"
__copyright__ = "Copyright Technological Resources Pty Ltd (a Rio Tinto Group company), 2019"

from .advanced_namedtuple import namedtuple


GpsPoint = namedtuple('GpsPoint',[
        # Important: some calculations are performed by accessing the fields
        # as tuple instead of their name. For this it is important, that the first
        # three fields correspond to x, y, orientation. Don't change this unless
        # you know what you are doing.
        # (this is partly for performance reasons and partly for historic reasons)
        'x', # x must be the first entry
        'y', # y must be the second entry
        'orientation', # orientation must be the third entry
        'z',
        'speed',
        'timestamp',
    ])


class TagType(object):
    UNKNOWN = 0
    LOAD_LOCATION = 1
    DUMP_LOCATION = 2
