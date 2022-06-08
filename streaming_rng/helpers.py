# Py 2/3 compat imports
from __future__ import print_function, division, absolute_import
from future.builtins import dict, list, object, range, str, bytes, filter, map, zip, ascii, chr, hex, input, next, oct, open, pow, round, super
from future.standard_library import install_aliases; install_aliases()
# This file is compatible with Python 2 and Python 3, please keep it so

__author__ = "Konstantin Seiler, The University of Sydney"
__copyright__ = "Copyright Technological Resources Pty Ltd (a Rio Tinto Group company), 2019"

import math
import numpy as np
import datetime
import pytz

import dubins

from .advanced_namedtuple import namedtuple

INF = float('inf')
NAN = float('nan')
EPSILON = 0.00001
EPOCH = datetime.datetime.fromtimestamp(0,pytz.UTC).replace(tzinfo=None)



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
        'id',
        'timestamp',
    ])

GpsPointMean = namedtuple('GpsPointMean', [
        'x',
        'y',
        'orientation',
        'z',
        'z_variance',
        'count',
    ])

def center_angle(angle):
    return ((angle + 180) % 360) - 180


def point_distance2d(p1, p2):
    deltax = p2[0] - p1[0]
    deltay = p2[1] - p1[1]
    return math.sqrt(deltax*deltax + deltay*deltay)


def point_distance(p1, p2, angular_weight):
    delta = np.subtract(p2[:3], p1[:3])
    return math.sqrt(delta[0]*delta[0] + delta[1]*delta[1] + (angular_weight*center_angle(delta[2]))**2)

def point_difference(p1, p2):
   return (p1[0]-p2[0], p1[1]-p2[1], center_angle(p1[2]-p2[2]))


def dubins_distance(source, waypoint, dubins_turning_radius):

    dubins_distance = dubins.shortest_path(
            (source[0],source[1],math.radians(source[2])),
            (waypoint[0],waypoint[1],math.radians(waypoint[2])),
            dubins_turning_radius,
        ).path_length()

    return dubins_distance



def point_mean(points):
    if not isinstance(points, (list, tuple)):
        points = tuple(points)
    result_x = sum(p['x'] for p in points) / len(points)
    result_y = sum(p['y'] for p in points) / len(points)
    result_orientation = math.atan2(sum(math.sin(math.radians(p['orientation'])) for p in points), sum(math.cos(math.radians(p['orientation'])) for p in points))
    result_orientation = center_angle(math.degrees(result_orientation))
    result_z = sum(p['z'] for p in points) / len(points)
    result_z_variance = np.var(tuple(p['z'] for p in points))

    return GpsPointMean(
            x=result_x,
            y=result_y,
            z=result_z,
            orientation=result_orientation,
            z_variance=result_z_variance,
            count=len(points),
        )



def vehicle_is_reversing(first, second):
    """returns True if the vehicle is reversing
    to get from the first point to the second.
    """

    if first['speed'] >= 0 and second['speed'] >= 0:
        return False

    if first['speed'] <= 0 and second['speed'] <= 0:
        return True

    # ok, we can't figure it out from the speed. Use a heuristic base on the heading instead.

    delta_x = second['x'] - first['x']
    delta_y = second['y'] - first['y']

    direction = math.degrees(math.atan2(delta_y, delta_x))

    first_relative_heading = center_angle(direction - first['orientation'])
    second_relative_heading = center_angle(direction - second['orientation'])

    if (-90 <= first_relative_heading <= 90) and (second_relative_heading <= -90 or 90 <= second_relative_heading):
        return False

    if (first_relative_heading <= -90 or 90 <= first_relative_heading) and (-90 <= second_relative_heading <= 90):
        return True


    # ok, the spacial analysis is inconclusive, too.

    # Let's simply take the average speed as fall back. It's weird data anyway...
    if first['speed'] + second['speed'] >= 0:
        return False
    else:
        return True



