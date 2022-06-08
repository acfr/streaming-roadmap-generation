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


import csv
from .advanced_namedtuple import namedtuple
from dxfwrite import DXFEngine as dxf
import igraph
from shapely.wkt import dumps
from shapely.geometry import Polygon, LineString
from six import string_types
import pickle
import copy
import math

class RoadNetworkAreaSource(object):
    UNKNOWN=0 # the type/source of area is unknown.
    INTERSECTION=1 # the area was created when searching for intersections.
    TAG=2 # the area was created by tagging GPS points.


RoadNetworkArea = namedtuple("RoadNetworkArea", [
        "polygons",
        ("name", None),
        ("source", RoadNetworkAreaSource.UNKNOWN),
    ],
    doc='''
        Represents an area in a road network.

        polygon:
        A polygon is a list of 2d coordinates, represented as a tuple with two entries.
        An area can contain multiple polygons, so the field "polygons" contains a list
        of lists of 2d tuples.

        name: the name of the area as string or None if no name is available.

        source: the source of the polygon. This indicates how the polygon was created.
        The value is one of the constants defined in RoadNetworkAreaSource.
    '''
    )


class RoadNetworkPointOfInterestType(object):
    DIGGER_POSITION = 1
    TRUCK_DIG_POSITION = 2
    DUMP_POSITION = 4
    TRUCK_DUMP_POSITION = 3

    @classmethod
    def to_string(cls, point_of_interest_type):
        mapping = {value:key for key,value in cls.__dict__.items() if not key.startswith('_') and key.upper() == key}
        return mapping[point_of_interest_type]


RoadNetworkPointOfInterest = namedtuple("RoadNetworkPointOfInterest", [
        "type",
        "position",
        "last_seen_timestamp",
        "name",
        ("equipment", None),
    ],
    doc='''
        Represents a point of interest in the road network.

        Points of interest have no direct connection to the road network graph. They are spacial features
        which may provide useful context.

        type: one of the constants defined in RoadNetworkPointOfInterestType
        position: (x, y, z) or (x, y) position of the point of interest.
        name: the name of the point of interest.
        equipment: (optional) The name of a piece of equipment associated with the point of interest.
    '''
    )


RoadNetwork = namedtuple("RoadNetwork", [
        "graph",
        "areas",
        "points_of_interest",
        "metadata",
    ],
    doc='''
        graph: an igraph object that holds the graph structure of the road network.
        The graph is directed.

        The vertices hold the following attributes:
            cluster: int. The internal id of the cluster that created the vertex.
                This field has little practical use, but might be useful for debugging.
            position: tuple. A 3-tuple containing the x,y,z coordinates of the vertex.
            orientation: float. The orientation (heading) of the vertex. The orientation
                is provided in degrees and measured from x towards y.
                Thus, if x represents Easting and y represents Northing, the angle is measured
                counterclockwise with East being zero degrees and North being 90 degrees.
            z_variance: float. The variance of the points making up the z-values in position.
            areas: list or None. If the vertex is part of any zones, the indices of the zones
                are listed here. The indices are with respect to the order of the areas in the
                'areas' field of the RoadNework.
            weight: the number of GPS points that are represented by this vertex.

        The edges hold the following attributes:
            weight: the number of GPS traversals that are represented by this edge.
            last_traversal: the timestamp of the last recorded traversal of this edge.
                Note: if weight == 0, the last_traversal may be nan.


        areas: A list of RoadNetworkArea structures that represent the areas.

        points_of_interest: A list of RoadNetworkPointOfInterest structures that represent points of interest.

        metadata: A dict with information about the road network.
    '''
    )


def shift_road_network(road_network, delta_x, delta_y, delta_z=0):
    """
    Add delta_x and delta_y to all coordinates to shift the road network to match
    a new coordinate system.
    """

    road_network = copy.deepcopy(road_network)

    road_network.graph.vs['position'] = [(p[0]+delta_x, p[1]+delta_y, p[2]+delta_z) for p in road_network.graph.vs['position']]
    for index in range(len(road_network.areas)):
        area = road_network.areas[index]
        polygons = tuple(tuple((p[0]+delta_x, p[1]+delta_y) for p in polygon) for polygon in area.polygons)
        road_network.areas[index] = area._replace(polygons=polygons)

    return road_network





def follow_edges(graph, first_edge, direction):
    """returns all edges from a given edge until the end of the road segment (fork or end of path)"""

    def next_vertex(edge):
        vertex_idx = edge.source if direction == igraph.IN else edge.target
        return graph.vs[vertex_idx]
    def first_next_edge(vertex):
        edge_idx = graph.incident(vertex, direction)[0]
        return graph.es[edge_idx]

    results = []
    vertex = next_vertex(first_edge)

    # keep going until we find an end-of-road or branch
    while vertex.indegree() == vertex.outdegree() == 1:
        edge = first_next_edge(vertex)
        if edge == first_edge:
            # loop detected!
            break
        results.append(edge)
        vertex = next_vertex(results[-1])

    return results


def find_roads(graph):
    """returns a list of lists-of-edges representing the segments in a road"""

    results = []

    # keep the indices of all unexplored edges
    unexplored_edge_idxs = set(range(graph.ecount()))

    while unexplored_edge_idxs:
        edge = graph.es[unexplored_edge_idxs.pop()]
        road_prefix = follow_edges(graph, edge, igraph.IN)
        road_suffix = follow_edges(graph, edge, igraph.OUT)
        road = list(reversed(road_prefix)) + [edge] + road_suffix

        results.append(road)

        # remove the new road's edges from the 'unexplored edges' list
        road_edge_idxs = {e.index for e in road}
        unexplored_edge_idxs.difference_update(road_edge_idxs)

    return results


def make_road(graph, road):
    """returns a dict containing the shapely geometry for the given road
    and the source/target areas from the specified graph"""

    v_idxs = [e.source for e in road] + [road[-1].target]
    coords = [graph.vs['position'][idx] for idx in v_idxs]

    z_variance = sum(graph.vs['z_variance'][idx]*graph.vs['weight'][idx] for idx in v_idxs) / sum(graph.vs['weight'][idx] for idx in v_idxs)

    weight = math.floor(sum(e['weight'] for e in graph.es if e.source in v_idxs and e.target in v_idxs)/(len(v_idxs)-1))

    counter = 0
    traversal_sum = 0

    for e in graph.es:
        if e.source in v_idxs and e.target in v_idxs and not math.isnan(e['last_traversal']):
            traversal_sum = traversal_sum + e['last_traversal']
            counter = counter + 1

    if traversal_sum:
        last_traversal = math.floor(traversal_sum/counter)
    else:
        last_traversal = float('nan')

    return {
        'shape': LineString(coordinates=coords),
        'source': graph.vs['cluster'][road[0].source],
        'target': graph.vs['cluster'][road[-1].target],
        'last_traversal': last_traversal,
        'weight': weight,
        'z_variance': z_variance,
        }


def write_csv(road_network, csv_file):
    """writes roads and areas from the specified road network into a CSV file

    Example:
    geometry, type, name
    LINESTRING(....), road, LOCATION_A->LOCATION_B
    POLYGON(...), area, LOCATION
    """

    if isinstance(csv_file, string_types):
        csv_file = open(csv_file, 'wt')
        close_file = True
    else:
        close_file = False

    try:

        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['geometry', 'type', 'name', 'z_variance'])

        # write roads
        roads = [make_road(road_network.graph, x) for x in find_roads(road_network.graph)]
        for road in roads:
            csv_writer.writerow([dumps(road['shape']),'aa_road','{}->{}'.format(road['source'], road['target']), str(road['z_variance'])])


        # write areas as polygons
        for area in road_network.areas:
            for polygon in area.polygons:
                if area.source == RoadNetworkAreaSource.INTERSECTION:
                    type_name = 'intersection'
                else:
                    type_name = 'area'

                csv_writer.writerow([dumps(Polygon(polygon)), type_name, area.name, ''])


    finally:
        if close_file:
            csv_file.close()


def write_dxf(road_network, file_name):

    drawing = dxf.drawing(file_name)

    # area layer
    drawing.add_layer('areas')
    for area in road_network.areas:
        for polygon in area.polygons:
            drawing.add(dxf.polyline(polygon, color=140, layer='areas'))

    # road layer
    drawing.add_layer('roads')
    roads = [make_road(road_network.graph, x) for x in find_roads(road_network.graph)]
    for road in roads:
        drawing.add(dxf.polyline(road['shape'].coords, color=1, layer='roads'))

    drawing.save()

