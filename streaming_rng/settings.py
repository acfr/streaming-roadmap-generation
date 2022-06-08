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
from .helpers import INF


StreamingRngSettings = namedtuple("StreamingRngSettings", [
        ('cluster_radius', 30),
        ('cluster_move_threshold', 3), # set to cluster_radius / 10
        ('angular_threshold', 30),
        ('cluster_merge_radius', 15),
        ('intermediate_point_method', 2),
        ('chord_angular_tolerance', 30),
        ('dubins_turning_radius', 10),
        ('event_backtrack_time', 5*60),
        ('event_backtrack_distance', 100),
        ('bucket_radius', 10),
        ('minimum_point_age_for_removal_by_bucket',1800),
        ('minimum_prune_age', 12*3600),
        ('maximum_prune_age', 7*24*3600),
        ('assume_truck_every_when_pruning', 1*24*3600),
        ('maximum_connection_count', 30),
        ('assign_points_every', 3000),
        ('update_connections_every', INF), # should be multiple of assign_points_every or INF
        ('prune_points_every', 30000), # should be multiple of update_connections_every (if not INF) and multiple of assign_points_every
        ('disable_point_pruning', False),
        ('minimum_zone_votes', 2),
        ('improve_multipath', True),
        ('multipath_search_max_path_hops', 4),
        ('multipath_search_ignore_zones', True),
        ('intersection_search_radius', 60), # set to 2*cluster_radius
        ('intersection_search_angular_threshold', 45),
        ('roadnetwork_driving_side', 'left'),
        ('intersection_search_max_path_hops', 4),
        ('max_allowed_pair_distance_threshold', 200),
        ('max_allowed_pair_time_threshold', 30),
        ('max_allowed_pair_speed', 80/3.6),
        ('auto_tag_gps_points_based_on_equipment_proximity', False),
        ('minimum_edge_weight', 2),
        ('minimum_road_network_component_size', 10),
        ('maximum_road_network_component_count', INF),
        ('protected_hop_count_when_pruning', 2),
    ])

StreamingRngSettings.CHORD_ANGLE = 1
StreamingRngSettings.DUBINS_METHOD = 2
StreamingRngSettings.GRAVITY_METHOD = 3
