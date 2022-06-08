# Streaming Roadmap Generation. Progressive online mapping of roads in real-time from vehicle location data. Copyright (C) 2022 Konstantin Seiler.
# 
# This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
#  
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License along with this program.  If not, see https://www.gnu.org/licenses/.
# 
# Further information about this program can be obtained from:
#
# - Konstantin Seiler (konstantin.seiler@sydney.edu.au)
# - Thomas Albrecht (thomas.albrecht@riotinto.com, Rio Tinto, Central Park, Lvl 4, 152-158 St Georges Tce, Perth, 6000, Western Australia)

# Py 2/3 compat imports
from __future__ import print_function, division, absolute_import
from future.builtins import dict, list, object, range, str, bytes, filter, map, zip, ascii, chr, hex, input, next, oct, open, pow, round, super
from future.standard_library import install_aliases; install_aliases()
# This file is compatible with Python 2 and Python 3, please keep it so


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
