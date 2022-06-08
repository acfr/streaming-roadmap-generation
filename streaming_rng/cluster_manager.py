# Py 2/3 compat imports
from __future__ import print_function, division, absolute_import
from future.builtins import dict, list, object, range, str, bytes, filter, map, zip, ascii, chr, hex, input, next, oct, open, pow, round, super
from future.standard_library import install_aliases; install_aliases()
# This file is compatible with Python 2 and Python 3, please keep it so

__author__ = "Konstantin Seiler, The University of Sydney"
__copyright__ = "Copyright Technological Resources Pty Ltd (a Rio Tinto Group company), 2020"

import math
import numpy as np
import rtree
import sklearn
import sklearn.neighbors
import collections
import shapely
import shapely.geometry
import shapely.ops
import igraph
import datetime
import struct
import json
import psutil
import os
from six import string_types

from . import road_network as road_network

from .advanced_namedtuple import namedtuple
from .helpers import INF, NAN, EPSILON, EPOCH, center_angle, vehicle_is_reversing, dubins_distance, point_distance, point_distance2d, point_mean
from .priodict import priorityDictionary
from .settings import StreamingRngSettings
from .types import GpsPoint, TagType

from .cluster import Cluster
from .cluster_connection import ClusterConnection



_TagPropagationData = namedtuple('_TagPropagationData', [
        'tag',
        'propagate_backwards',
        'propagate_forwards',
        'backwards_time_horizon',
        'forwards_time_horizon',
        'backwards_distance',
        'forwards_distance',
    ])

_PointPruneData = namedtuple('_PointPruneData', [
        'points_flagged_for_deletion',
        'connection_weight_adjustment',
    ])


POINT_MATCH_EPSILON = 0.0001

class ClusterManager(object):
    """
    The ClusterManager is the main object for the map creation algorithm.
    Data is incrementally fed into it and different processing steps can be executed
    after a batch of data has been added to update the map.
    """

    _point_row_template = (
            ('x', float, NAN),
            ('y', float, NAN),
            ('z', float, NAN),
            ('orientation', float, NAN),
            ('speed', float, NAN),
            ('timestamp', int, 0),
            ('previous', int, -1),
            ('next', int, -1),
            ('tag', int, -1),
            ('edge_tag', int, -1),
            ('cluster_id', int, -1),
        )

    def __init__(self, settings, logger):
        """The logger provides two methods of recording logs in console or generator log file"""
        self.logger = logger

        self.next_cluster_id = 0
        """Clusters have a unique id. This is the next free id to use."""

        self.next_connection_id = 0
        """Cluster connections have a unique id. This is the next free id to use."""

        self.latest_timestamp = -INF
        """The timestamp of the most recent data that was received (a high water mark)"""

        self.settings = settings
        """The configuration settings to use."""

        self.angular_weight = (float)(self.settings.cluster_radius) / (float)(self.settings.angular_threshold)
        """For convenience, the weight of the angular difference to use in distance calculations."""

        self.points = np.array([tuple(i[2] for i in self._point_row_template)], dtype=[(i[0],i[1]) for i in self._point_row_template])
        """The GPS points as memory efficient table"""

        self.free_points = set([0])
        """Indices of self.points which are currently unused."""

        self.clusters = dict()
        """The Cluster objects keyed by their id."""

        tree_property = rtree.index.Property()
        tree_property.dimension = 2
        self.cluster_index = rtree.index.Index(interleaved=True, property=tree_property)
        """A spacial index that allows to quickly look up clusters that are within a certain area."""

        self.cluster_connections_by_source_and_target = dict()
        """Cluster connections keyed by (source, target) tuples. Source and target are given as Cluster object."""

        self.cluster_connections = dict()
        """The cluster connection objects keyed by their id."""

        tree_property = rtree.index.Property()
        tree_property.dimension = 2
        self.connection_index = rtree.index.Index(interleaved=True, property=tree_property)
        """A spacial index that allows to quickly look up cluster connections that are afected by a certain area."""

        self.cluster_connections_to_update = set()
        """A set of cluster connections that must be updated in the next round of updating cluster connections."""

        self.points_to_recluster = set()
        """A set of all GpsPoint objects that are not part of a cluster. Each point known to the
        algorithm must be either in here or in points_to_clusters."""

        tree_property = rtree.index.Property()
        tree_property.dimension = 2
        self.point_index = rtree.index.Index(interleaved=True, property=tree_property)
        """A spacial index with the GpsPoints."""

        self.tag_names = []
        """If a gps point has a tag associated with it, it is stored as an id with the point. This list
        allows to match the id to a string representation."""

        self.latest_tags = dict()
        """The latest instance a tag was seen.
        Tuples of form ((x,y,z), timestamp) keyed by (tag_id, tag_type)"""

        self.pending_edge_tags_to_propagate = dict()
        """When a point is tagged, the tag is propagated along neighbouring points.
        While back-propagation can be done immediately (under the assumption that points are stored in chronological order,
        forward propagation is delayed, because the points might not be there yet.
        Here, _TagPropagationData objects are stored, keyed by the corresponding GpsPoint.
        Whenever an outgoing edge is added to a gps point, the data in here is checked and propagated if needed.
        """

        self.digger_positions = dict()
        """The current position of each digger. ((x,y,z), timestamp) tuples keyed by digger name."""

        self.dump_positions = dict()
        """The current position of each dump. ((x,y,z), timestamp) tuples keyed by dump name."""

        self.equipment_locations = dict()
        """The name of a location a piece of equipment is currently at.
        This information is used to automatically tag Gps points when they are
        close to a digger or dump.
        (location_name, timestamp) tuples keyed by equipment name.
        """


    # we simply use the snapshot mechanism for pickling
    def __getstate__(self):
        raise NotImplementedError("This class can't be pickled.")

    def __setstate__(self, state):
        raise NotImplementedError("This class can't be pickled.")


    def point_to_string(self, point):
        if isinstance(point, int):
            point = self.points[point]

        result = " ".join("%s: %s" % (t[0],v) for v,t in zip(point, self._point_row_template))
        result += " datetime: %s" % ((EPOCH + datetime.timedelta(seconds=int(point['timestamp']))).strftime('%Y-%m-%d %H:%M:%S'))
        return "[" + result + "]"





    SNAPSHOT_MARKER = b'RNG_SNAPSHOT_v2.1' + b'\0'

    def _get_snapshot_point_format(self):

        point_format = b'<'
        for name, data_type, _ in self._point_row_template:
            if data_type is float:
                point_format += b'd'
            elif data_type is int:
                point_format += b'q'
            elif data_type is bool:
                point_format += b'?'
            else:
                raise NotImplementedError("Don't know how to encode data type %s" % (data_type,))

        return point_format



    def save_snapshot_to_File(self, file):

        file.write(self.SNAPSHOT_MARKER)


        point_count =  len(self.points)-len(self.free_points)
        cluster_count = len(self.clusters)
        connection_count = len(self.cluster_connections)


        pending_edge_tags_to_propagate = list()
        for point_index, data in self.pending_edge_tags_to_propagate.items():
            data = dict(data._asdict())
            for field in ['tag', 'backwards_time_horizon', 'forwards_time_horizon']:
                data[field] = int(data[field])
            pending_edge_tags_to_propagate.append((int(point_index), data))


        latest_tags = [((int(t),int(tt)), (((float(x), float(y), float(z)), int(ts)))) for (t,tt), ((x,y,z), ts) in self.latest_tags.items()]

        json_data = {
                'settings': dict(self.settings._asdict()),
                'pending_edge_tags_to_propagate': pending_edge_tags_to_propagate,
                'digger_positions': list(self.digger_positions.items()),
                'dump_positions': list(self.dump_positions.items()),
                'equipment_locations': list(self.equipment_locations.items()),
                'tag_names': list(self.tag_names),
                'latest_tags': latest_tags,
                'point_count': point_count,
                'cluster_count': cluster_count,
                'connection_count': connection_count,
            }


        json_data = json.dumps(json_data).encode('utf8')

        file.write(b'JSON')
        file.write(struct.pack('<q', len(json_data)))
        file.write(json_data)



        file.write(b'POINTS')

        file.write((','.join(i[0] for i in self._point_row_template)).encode('utf8'))

        written_point_count = 0
        point_struct = struct.Struct(self._get_snapshot_point_format() + b'q?')
        for index, point in enumerate(self.points):
            if index not in self.free_points:
                is_in_to_recluster = index in self.points_to_recluster
                file.write(point_struct.pack( *(tuple(point) + (index, is_in_to_recluster,)) ))
                written_point_count += 1

        assert point_count == written_point_count, "The count of written points is wrong. This shouldn't happen. Looks like the internal data structures are corrupted."


        file.write(b'CLUSTERS')

        written_cluster_count = 0
        cluster_struct = struct.Struct(b'<qddd')
        for cluster in self.clusters.values():
            assert len(cluster.position_at_last_update) == 3, "The cluster's position_at_last_update doesn't have the expected length. %s" % (cluster.position_at_last_update,)
            file.write(cluster_struct.pack(cluster.id, *tuple(cluster.position_at_last_update)))
            written_cluster_count += 1

        assert cluster_count == written_cluster_count, "The count of written clusters is wrong. This shouldn't happen. Looks like the internal data structures are corrupted."

        file.write(b'CONNECTIONS')

        written_connection_count = 0
        for connection in self.cluster_connections.values():
            sequence = tuple(c.id for c in connection.cluster_sequence)
            file.write(
                struct.pack(
                    b'<qq?q'+(b'q'*len(sequence)),
                    connection.source.id,
                    connection.target.id,
                    connection in self.cluster_connections_to_update,
                    len(sequence),
                    *sequence
                ))

            written_connection_count += 1

        assert connection_count == written_connection_count, "The count of written clusters is wrong. This shouldn't happen. Looks like the internal data structures are corrupted."

        file.write(b'END')



    def _check_snapshot_marker(self, file, marker):
        data = file.read(len(marker))
        assert data == marker, 'The snapshot file seems to be corrupt. Expected to find %s, but found %s.' % (marker, data)


    def load_snapshot_from_file(self, file, load_settings_from_snapshot=False):
        """
        Load the state as it was when the snapshot was created. All data that
        is currently in the cluster manager is discarted.
        """

        old_settings = self.settings
        old_logger = self.logger
        self.__dict__ = {}

        try:

            self._check_snapshot_marker(file, self.SNAPSHOT_MARKER)

            self._check_snapshot_marker(file, b'JSON')

            length = struct.unpack( '<q', file.read(struct.calcsize(b'<q')) )[0]
            json_data = json.loads(file.read(length).decode('utf8'))

            if load_settings_from_snapshot:
                settings = StreamingRngSettings(**json_data['settings'])
            else:
                settings = old_settings

            self.__init__(settings=settings, logger=old_logger)

            self.logger.debug('loading points')


            #allocate the memory for the points
            assert len(self.points) > 0, "The points should never have zero size."
            new_size = len(self.points)
            while new_size < json_data['point_count']:
                new_size *= 2
            self.points = np.resize(self.points, new_size)
            self.points[:] = tuple(i[2] for i in self._point_row_template)
            self.free_points = set(range(new_size))

            point_id_translator = dict()
            cluster_id_translator = dict()
            tag_id_translator = dict()

            def translate_point_id(index):
                if index < 0:
                    return index
                if index not in point_id_translator:
                    point_id_translator[index] = len(point_id_translator)
                return point_id_translator[index]

            def translate_cluster_id(index):
                if index < 0:
                    return index
                if index not in cluster_id_translator:
                    cluster_id_translator[index] = len(cluster_id_translator)
                return cluster_id_translator[index]

            def translate_tag_id(index):
                if index < 0:
                    return index
                if index not in tag_id_translator:
                    tag_id_translator[index] = len(tag_id_translator)
                return tag_id_translator[index]


            self._check_snapshot_marker(file, b'POINTS')
            self._check_snapshot_marker(file, (','.join(i[0] for i in self._point_row_template)).encode('utf8'))

            earliest_timestamp = INF

            point_struct = struct.Struct(self._get_snapshot_point_format() + b'q?')
            for _ in range(json_data['point_count']):
                data = point_struct.unpack(file.read(point_struct.size))
                index = translate_point_id(data[-2])
                is_in_to_recluster = data[-1]

                self.points[index] = data[:-2]
                self.points[index]['next'] = translate_point_id(self.points[index]['next'])
                self.points[index]['previous'] = translate_point_id(self.points[index]['previous'])
                self.points[index]['cluster_id'] = translate_cluster_id(self.points[index]['cluster_id'])
                self.points[index]['tag'] = translate_tag_id(self.points[index]['tag'])
                self.points[index]['edge_tag'] = translate_tag_id(self.points[index]['edge_tag'])

                self.free_points.remove(index)

                if is_in_to_recluster:
                    self.points_to_recluster.add(index)

                if self.latest_timestamp < self.points[index]['timestamp']:
                    self.latest_timestamp = int(self.points[index]['timestamp'])

                if self.points[index]['timestamp'] < earliest_timestamp:
                    earliest_timestamp = int(self.points[index]['timestamp'])


            self.logger.info('Snapshot contains data from %s to %s.' %
                (
                    (EPOCH+datetime.timedelta(seconds=int(earliest_timestamp))).strftime("%Y-%m-%d %H:%M:%S"),
                    (EPOCH+datetime.timedelta(seconds=int(self.latest_timestamp))).strftime("%Y-%m-%d %H:%M:%S")
                )
            )
            self.logger.debug('creating point index')

            # We load all points at once into a new index using the iterator method
            # This is much faster than individual insert() calls for each point.
            def tree_loader():
                for index, point in enumerate(self.points):
                    if index not in self.free_points:
                        yield (index, (point['x'], point['y'], point['x'], point['y']), None)

            assert self.point_index.interleaved, "The point index isn't set up using the expected interleaved convention."
            self.point_index = rtree.index.Index(tree_loader(), interleaved=self.point_index.interleaved, property=self.point_index.properties)


            self.logger.debug('setting up tags')
            self.tag_names = ['UNKNOWN']*len(tag_id_translator)
            for old_index, new_index in tag_id_translator.items():
                self.tag_names[new_index] = str(json_data['tag_names'][old_index])

            self.logger.debug('loading latest tag information')
            self.latest_tags = dict()
            for (old_tag_id, tag_type), (position, timestamp) in json_data['latest_tags']:
                if timestamp >= self.latest_timestamp - (int)(self.settings.maximum_prune_age):
                    if old_tag_id in tag_id_translator:
                        self.latest_tags[tag_id_translator[old_tag_id], tag_type] = (position, timestamp)

            self.logger.debug('loading tag propagation data')

            for point_index, data in json_data['pending_edge_tags_to_propagate']:
                point_index = int(point_index)
                if point_index in point_id_translator:
                    propagation_data = _TagPropagationData(**data)
                    if propagation_data.forwards_time_horizon >= self.latest_timestamp - 600:
                        self.pending_edge_tags_to_propagate[point_id_translator[point_index]] = propagation_data


            self.logger.debug('loading other position data')

            self.digger_positions = {str(k):(tuple(p), int(t)) for k,(p,t) in json_data['digger_positions']}
            self.dump_positions = {str(k):(tuple(p), int(t)) for k,(p,t) in json_data['dump_positions']}
            self.equipment_locations = {str(k):(str(n), int(t)) for k,(n,t) in json_data['equipment_locations']}


            # the easy data is all loaded now.

            # Instantiate the clusters
            self.logger.debug('instantiating clusters')

            points_in_clusters = collections.defaultdict(list)
            for index, point in enumerate(self.points):
                if index not in self.free_points:
                    if point['cluster_id'] >= 0:
                        points_in_clusters[point['cluster_id']].append(index)

            self._check_snapshot_marker(file, b'CLUSTERS')

            cluster_struct = struct.Struct(b'<qddd')
            for _ in range(json_data['cluster_count']):
                data = cluster_struct.unpack(file.read(cluster_struct.size))
                index = translate_cluster_id(data[0])
                position_at_last_update = data[1:4]

                Cluster.make_new_when_loading_from_snapshot(
                        cluster_manager=self,
                        id=index,
                        points=points_in_clusters[index],
                        position_at_last_update=position_at_last_update,
                    )

            # Make sure the id counter is correct
            self.next_cluster_id = (max(c.id for c in self.clusters.values())+1) if self.clusters else 0

            self.logger.debug('instantiating connections')


            self._check_snapshot_marker(file, b'CONNECTIONS')

            connecting_sequences = dict()
            cluster_connections_to_update = set()

            connection_struct = struct.Struct(b'<qq?q')
            for _ in range(json_data['connection_count']):
                source_id, target_id, in_cluster_connections_to_update, sequence_len = connection_struct.unpack(file.read(connection_struct.size))
                sequence_struct = struct.Struct( b'<'+(b'q'*sequence_len) )
                sequence = sequence_struct.unpack(file.read(sequence_struct.size))

                source_id = cluster_id_translator[source_id]
                target_id = cluster_id_translator[target_id]
                sequence = tuple(cluster_id_translator[i] for i in sequence)

                connecting_sequences[sequence[0], sequence[-1]] = sequence

                if in_cluster_connections_to_update:
                    cluster_connections_to_update.add((source_id, target_id))

            # Instantiate the cluster connections
            for cluster in self.clusters.values():
                for target, counter in cluster.desired_connections.items():
                    if counter.count > 0:

                        if (cluster.id, target.id) in connecting_sequences:
                            cluster_sequence = [self.clusters[i] for i in connecting_sequences[cluster.id, target.id]]
                        else:
                            cluster_sequence = None

                        connection = ClusterConnection(
                                cluster_manager=self,
                                source=cluster,
                                target=target,
                                cluster_sequence=cluster_sequence,
                            )
                        cluster.connections[target] = connection

                        if (connection.source.id, connection.target.id) in cluster_connections_to_update:
                            self.cluster_connections_to_update.add(connection)


            self.logger.debug("setting connection tags")

            for source_index, source in enumerate(self.points):
                if source_index not in self.free_points:
                    if source['next'] >= 0 and source['cluster_id'] >= 0:
                        target_index = source['next']
                        target = self.points[target_index]
                        source_cluster = self.clusters[source['cluster_id']]
                        if target['cluster_id'] >= 0:
                            target_cluster = self.clusters[target['cluster_id']]
                            if target_cluster is not source_cluster:
                                tag = source['edge_tag']
                                if not vehicle_is_reversing(source, target):
                                    source_cluster.connections[target_cluster].add_tag(tag)
                                else:
                                    target_cluster.connections[source_cluster].add_tag(tag)


            self._check_snapshot_marker(file, b'END')

            self.logger.debug('running sanity checks')

            for index, point in enumerate(self.points):
                if index not in self.free_points:
                    if point['next'] >= 0:
                        assert point['next'] < len(self.points) and point['next'] not in self.free_points, "A point points towards a next point that doesn't exist"
                    if point['previous'] >= 0:
                        assert point['previous'] < len(self.points) and point['previous'] not in self.free_points, "A point points towards a previous point that doesn't exist."
                    if point['cluster_id'] >= 0:
                        assert point['cluster_id'] in self.clusters, "The point is in a cluster that doesn't exist"
                        assert index in self.clusters[point['cluster_id']].points, "the point is not part of the cluster it is supposedly a part of."
                    if point['tag'] >= 0:
                        assert point['tag'] < len(self.tag_names), "The point points to a tag that doesn't exist"
                    if point['edge_tag'] >= 0:
                        assert point['next'] >= 0, "The point has an edge tag but no edge (no next point)."
                        assert point['edge_tag'] < len(self.tag_names), "The point points to an edge tag that doesn't exist"

            for tag in self.tag_names:
                assert isinstance(tag, string_types), "The tag loaded from snapshot isn't of string type. Found %s with type %s." % (tag, type(tag))

            for digger in self.digger_positions:
                assert isinstance(digger, string_types), "The digger name loaded from snapshot isn't of string type. Found %s with type %s." % (digger, type(digger))

            for dump in self.dump_positions:
                assert isinstance(dump, string_types), "The dump name loaded from snapshot isn't of string type. Found %s with type %s." % (dump, type(dump))

            for equipment, (location, timestamp) in self.equipment_locations.items():
                assert isinstance(equipment, string_types), "The equipment name loaded from snapshot isn't of string type. Found %s with type %s." % (equipment, type(equipment))
                assert isinstance(location, string_types), "The equipment location name loaded from snapshot isn't of string type. Found %s with type %s." % (location, type(location))
                assert isinstance(timestamp, int), "The equipment location timestamp loaded from snapshot isn't of int type. Found %s with type %s." % (timestamp, type(timestamp))

            self.logger.info('snapshot loaded')

        except Exception as e:
            old_logger.exception('Error loading snapshot: {} ({})'.format(str(e), type(e)))
            old_logger.info('Loading of snapshot failed. Restoring cluster manager to an empty state.')
            self.__init__(settings=old_settings, logger=old_logger)
            raise e




    def set_digger_position(self, timestamp, digger, point):
        """Set the position of a digger.

        timestamp: the timestamp of the recording
        digger: the name of the digger
        point: the GPS position given as (x, y, z).

        The orientation may be None.
        """

        if isinstance(timestamp,float):
            timestamp = int(math.trunc(timestamp))
        assert isinstance(timestamp, int), "Unexpected timestamp: %s of type %s" % (timestamp, type(timestamp))

        if type(point) is not tuple or len(point) != 3:
            point = (point[0], point[1], point[2])

        if digger not in self.digger_positions or self.digger_positions[digger][1] <= timestamp:
            self.digger_positions[digger] = (point, timestamp)

    def add_bucket_position(self, digger, point):
        """Set the position of a bucket that was scooped.

        digger: the name of the digger that scooped it.
        point: the position given as (x, y)
        """

        if not type(point) is tuple and len(point) == 2:
            point = (point[0], point[1])

        self.remove_points_in_circle(
                center=point,
                radius=self.settings.bucket_radius,
                max_timestamp=self.latest_timestamp-self.settings.minimum_point_age_for_removal_by_bucket,
            )


    def set_dump_position(self, timestamp, dump, point):
        """Set the position of a dump.

        timestamp: the timestamp of the recording
        dump: the name of the dump
        point: the GPS position given as (x, y, orientation).

        The orientation may be None.
        """

        if isinstance(timestamp,float):
            timestamp = int(math.trunc(timestamp))
        assert isinstance(timestamp, int), "Unexpected timestamp: %s of type %s" % (timestamp, type(timestamp))

        if type(point) is not tuple or len(point) != 3:
            point = (point[0], point[1], point[2])

        if dump not in self.dump_positions or self.dump_positions[dump][1] <= timestamp:
            self.dump_positions[dump] = (point, timestamp)


    def set_equipment_location(self, timestamp, equipment, location):
        """Set the location name of a piece of equipment (digger or dump).

        timestamp: the timestamp of the recording
        equipment: the name of the equipment.
        location: the name of the location it is at.
        """

        if isinstance(timestamp,float):
            timestamp = int(math.trunc(timestamp))
        assert isinstance(timestamp, int), "Unexpected timestamp: %s of type %s" % (timestamp, type(timestamp))

        if equipment not in self.equipment_locations or self.equipment_locations[equipment][1] <= timestamp:
            self.equipment_locations[equipment] = (location, timestamp)


    def add_point(self, point):
        """Add a GpsPoint to the algorithm.

        point: GpsPoint object.
        """

        assert -180 <= point.orientation and point.orientation < 180, "Point %s has an orientation of %s which is not an angle that is normalised to be in the range [-180, 180). Use center_angle() to pre-process the data." % (point, point.orientation)

        if not isinstance(point.timestamp, int):
            point = point._replace(timestamp=math.trunc(point.timestamp))

        # check if the point already exists
        candidate_ids = self.point_index.intersection(
                (point.x-POINT_MATCH_EPSILON, point.y-POINT_MATCH_EPSILON, point.x+POINT_MATCH_EPSILON, point.y+POINT_MATCH_EPSILON),
                objects=False,
            )

        for index in candidate_ids:
            candidate = self.points[index]
            if (candidate['x']-POINT_MATCH_EPSILON <= point.x <= candidate['x']+POINT_MATCH_EPSILON
                    and candidate['y'] - POINT_MATCH_EPSILON <= point.y <= candidate['y'] + POINT_MATCH_EPSILON
                    and candidate['z'] - POINT_MATCH_EPSILON <= point.z <= candidate['z'] + POINT_MATCH_EPSILON
                    and candidate['timestamp'] == point.timestamp
                    and candidate['speed'] - POINT_MATCH_EPSILON <= point.speed <= candidate['speed'] + POINT_MATCH_EPSILON
                    and candidate['orientation'] - POINT_MATCH_EPSILON <= point.orientation <= candidate['orientation'] + POINT_MATCH_EPSILON
                ):

                # we found a match.
                return index


        if not self.free_points:
            # double the storage capacity
            old_size = len(self.points)
            self.points = np.resize(self.points, old_size*2)
            self.points[old_size:] = tuple(i[2] for i in self._point_row_template)
            self.free_points = set(range(old_size, 2*old_size))

        index = self.free_points.pop()
        row = self.points[index]

        row['x'] = point.x
        row['y'] = point.y
        row['z'] = point.z
        row['orientation'] = point.orientation
        row['speed'] = point.speed
        row['timestamp'] = point.timestamp
        row['previous'] = -1
        row['next'] = -1
        row['tag'] = -1
        row['edge_tag'] = -1
        row['cluster_id'] = -1


        self.point_index.insert(id=index, coordinates=(row['x'], row['y'], row['x'], row['y']),)

        if self.latest_timestamp < point.timestamp:
            self.latest_timestamp = point.timestamp

        self.points_to_recluster.add(index)


        if self.settings.auto_tag_gps_points_based_on_equipment_proximity:
             self._check_point_for_equipment_proximity(index)

        return index


    def remove_point(self, point_index):
        """
        Remove a GPS point from the cluster manager
        point_index: the index of the point in the self.points array.
        """

        row = self.points[point_index]

        if row['cluster_id'] >= 0:
            cluster = self.clusters[row['cluster_id']]
            cluster.remove_point(point_index)
            if not cluster.points:
                cluster.kill()


        if point_index in self.points_to_recluster:
            self.points_to_recluster.remove(point_index)

        self.point_index.delete(id=point_index, coordinates=(row['x'], row['y'], row['x'], row['y']))

        if row['next'] >= 0:
            self.points['previous'][row['next']] = -1

        if row['previous'] >= 0:
            self.points['next'][row['previous']] = -1
            self.points['edge_tag'][row['previous']] = -1

        self.points[point_index] = tuple(i[2] for i in self._point_row_template)
        self.free_points.add(point_index)


    def remove_points_in_shape(self, shape, max_timestamp=INF):
        """
        Remove all GPS points within the given shape.

        shape: a shapely geometry object
        max_timestamp: only points with timestamp <= max_timestamp are removed.
        """

        candidate_ids = self.point_index.intersection(shape.bounds, objects=False)

        for candidate_id in candidate_ids:
            point = self.points[candidate_id]
            if point['timestamp'] <= max_timestamp and shape.contains(shapely.geometry.Point(point['x'], point['y'])):
                self.remove_point(candidate_id)


    def remove_points_in_circle(self, center, radius, max_timestamp=INF):
        """
        Remove all GPS points within the given circle.

        center: the center of the circle as tuple
        radius: the radius of the circle
        max_timestamp: only points with timestamp <= max_timestamp are removed.
        """

        candidate_ids = self.point_index.intersection(
                (center[0]-radius, center[1]-radius, center[0]+radius, center[1]+radius),
                objects=False,
            )

        for candidate_id in candidate_ids:
            point = self.points[candidate_id]
            if point['timestamp'] <= max_timestamp and point_distance2d(center, (point['x'], point['y'])) <= radius:
                self.remove_point(candidate_id)


    def add_edge(self, source, target):
        """Add an edge (consecutive GpsPoint objects) to the system.

        source: GpsPoint
        target: GpsPoint

        If source or target are not known to the system yet, they are added
        using add_point(). Thus, it is sufficient to use this method and
        add_point() does not have to (but can) be called explicitly.
        """

        if source.timestamp == target.timestamp and source.x == target.x and source.y == target.y and source.z == target.z and source.orientation == target.orientation:
            self.logger.warning('A GPS point was passed as both elements of a pair. Ignoring the connection. First points: %s. Second point: %s' % (source, target))
            self.add_point(source) # add the point so we don't miss it alltogether
            return


        source_index = self.add_point(source)
        target_index = self.add_point(target)

        if source_index == target_index:
            self.logger.warning('A GPS pair (numerically) maps to the same point. Ignoring the connection. First points: %s. Second point: %s. Mapped point is: %s.' % (source, target, self.points[source_index]))
            return



        distance_between_points = point_distance2d((source.x, source.y), (target.x, target.y))
        time_betweeen_points = target.timestamp - source.timestamp
        if self.settings.max_allowed_pair_time_threshold < time_betweeen_points and self.settings.max_allowed_pair_distance_threshold < distance_between_points:
            return

        if 0 < time_betweeen_points and self.settings.max_allowed_pair_speed < distance_between_points / time_betweeen_points:
            return


        if self.points[source_index]['next'] >= 0:
            assert self.points[source_index]['next'] == target_index,\
                "The point %s which is mapped to %s already has an outgoing edge to %s, but an edge to %s which is mapped to %s should be created." % \
                (source, self.points[source_index], self.points[self.points['next'][source_index]], target, self.points[target_index])
        else:
            assert self.points[target_index]['previous'] == -1,\
                "The point %s which is mapped to %s already has an incoming edge from %s, but an edge fom %s which is mapped to %s should be created." % \
                (target, self.points[target_index], self.points[self.points['previous'][target_index]], source, self.points[source_index])

            self.points[source_index]['next'] = target_index
            self.points[target_index]['previous'] = source_index
            self.points[source_index]['edge_tag'] = -1

            source_cluster_id = self.points[source_index]['cluster_id']
            target_cluster_id = self.points[target_index]['cluster_id']
            if source_cluster_id >= 0 and target_cluster_id >= 0:
                source_cluster = self.clusters[source_cluster_id]
                target_cluster = self.clusters[target_cluster_id]
                if source_cluster is not target_cluster:
                    reversing = vehicle_is_reversing(self.points[source_index], self.points[target_index])
                    if not reversing:
                        source_cluster.add_desired_connection(target_cluster,
                                                              tag=self.points[source_index]['edge_tag'],
                                                              reversing=reversing)
                    else:
                        target_cluster.add_desired_connection(source_cluster,
                                                              tag=self.points[source_index]['edge_tag'],
                                                              reversing=reversing)

        if source_index in self.pending_edge_tags_to_propagate:
            self._propagate_point_tag(source_index)
        if target_index in self.pending_edge_tags_to_propagate:
            self._propagate_point_tag(target_index)


    def _check_point_for_equipment_proximity(self, point_index):
        """After adding a point, should be checked for proximity to diggers or dumps.
        add_point() does this automatically.

        This proximity then triggers automatic tagging of points.
        """

        point = self.points[point_index]

        tag = None
        tag_type= None

        for digger, (digger_position, _) in self.digger_positions.items():
            if point_distance2d((point['x'], point['y']), digger_position) < 75:
                if digger in self.equipment_locations:
                    tag, _ = self.equipment_locations[digger]
                    tag_type = TagType.LOAD_LOCATION

        for dump, (dump_position, _) in self.dump_positions.items():
            if point_distance2d((point['x'], point['y']), dump_position) < 75:
                tag = dump
                tag_type = TagType.DUMP_LOCATION

        if tag is not None:
            self.set_point_tag(point_index, tag, tag_type=tag_type)


    def set_point_tag(self, point, tag, tag_type):
        """Set a tag for a point and propagate it.

        point: GpsPoint
        tag: string with the tag.
        tag_type: one of the constant in types.TagType
        """

        if isinstance(point, GpsPoint):
            point_index = self.add_point(point)
        elif isinstance(point, (int,np.integer)):
            point_index = point
        else:
            raise ValueError("Only GpsPoints and integers are allowed to be used to refer to a point. %s found." % (type(point),))

        if isinstance(tag, string_types):
            try:
                tag = self.tag_names.index(tag)
            except ValueError:
                self.tag_names.append(tag)
                tag = len(self.tag_names)-1

        elif isinstance(tag, int):
            assert 0 <= tag < len(self.tag_names), "The tag id is out of range."

        else:
            raise ValueError("A tag of unexpected type was given. The tag must be int or string. Found %s of type %s" % (tag, type(tag)))


        point = self.points[point_index]

        key = (tag, tag_type)
        if key not in self.latest_tags or self.latest_tags[key][1] < point['timestamp']:
            self.latest_tags[key] = ((point['x'], point['y'], point['z']), point['timestamp'])

        if point['tag'] == tag:
            return

        old_tag = point['tag']
        point['tag'] = tag

        if point['cluster_id'] >= 0:
            cluster = self.clusters[point['cluster_id']]
            cluster.point_tags[old_tag] -= 1
            cluster.point_tags[tag] += 1

        tag_data = _TagPropagationData(
                tag=tag,
                propagate_backwards=True,
                propagate_forwards=True,
                backwards_time_horizon=point['timestamp'] - self.settings.event_backtrack_time,
                forwards_time_horizon=point['timestamp'] + self.settings.event_backtrack_time,
                backwards_distance=self.settings.event_backtrack_distance,
                forwards_distance=self.settings.event_backtrack_distance,
            )

        self.pending_edge_tags_to_propagate[point_index] = tag_data

        self._propagate_point_tag(point_index)



    def _propagate_point_tag(self, point_index):
        """Process the propagation of tags across neighbouring points."""

        stack = [point_index]

        while stack:
            point_index = stack.pop()
            point = self.points[point_index]

            tag_data = self.pending_edge_tags_to_propagate[point_index]

            keep_propagation_pending = False

            if tag_data.propagate_backwards:
                if point['previous'] >= 0:
                    previous_point_index = point['previous']
                    previous_point = self.points[previous_point_index]
                    if previous_point['timestamp'] >= tag_data.backwards_time_horizon:
                        distance = point_distance2d((previous_point['x'], previous_point['y']), (point['x'], point['y']))
                        if distance <= tag_data.backwards_distance:
                            if previous_point['edge_tag'] < 0:
                                self.set_edge_tag(previous_point_index, tag_data.tag)
                                if previous_point_index not in self.pending_edge_tags_to_propagate:
                                    self.pending_edge_tags_to_propagate[previous_point_index] = tag_data._replace(propagate_forwards=False, backwards_distance=tag_data.backwards_distance-distance)
                                    stack.append(previous_point_index)

                else:
                    keep_propagation_pending = True


            if tag_data.propagate_forwards:
                if point['next'] >= 0:
                    next_point_index = point['next']
                    next_point = self.points[next_point_index]
                    if next_point['timestamp'] <= tag_data.forwards_time_horizon:
                        distance = point_distance2d((point['x'], point['y']), (next_point['x'], next_point['y']))
                        if distance <= tag_data.forwards_distance:
                            if point['edge_tag'] < 0:
                                self.set_edge_tag(point_index, tag_data.tag)
                                if next_point_index not in self.pending_edge_tags_to_propagate:
                                    self.pending_edge_tags_to_propagate[next_point_index] = tag_data._replace(propagate_backwards=False, forwards_distance=tag_data.forwards_distance-distance)
                                    stack.append(next_point_index)

                else:
                    keep_propagation_pending = True


            # if we are more than 10 minutes past the horizon of this, we consider it done.
            if tag_data.forwards_time_horizon < self.latest_timestamp - 600:
                keep_propagation_pending = False


            if not keep_propagation_pending:
                del self.pending_edge_tags_to_propagate[point_index]


    def set_edge_tag(self, source_index, tag):
        """Set a tag for an edge between points.

        This is usually done automatically when tags are propagated.
        """

        source = self.points[source_index]
        assert source['next'] >= 0
        target = self.points[source['next']]

        if source['cluster_id'] >= 0 and target['cluster_id'] >= 0:
            source_cluster = self.clusters[source['cluster_id']]
            target_cluster = self.clusters[target['cluster_id']]
            if source_cluster is not target_cluster:
                old_tag = source['edge_tag']
                if not vehicle_is_reversing(source, target):
                    source_cluster.connections[target_cluster].remove_tag(old_tag)
                    source_cluster.connections[target_cluster].add_tag(tag)
                else:
                    target_cluster.connections[source_cluster].remove_tag(old_tag)
                    target_cluster.connections[source_cluster].add_tag(tag)

        source['edge_tag'] = tag


    def get_metadata_info(self):
        """
        Get some meta data and statistical information about the data contained
        in this cluster manager.
        """

        result = dict()

        result['latest_data_timestamp'] = self.latest_timestamp
        oldest_data_timestamp = None
        for index, timestamp in enumerate(self.points['timestamp']):
            if index not in self.free_points:
                if oldest_data_timestamp is None or timestamp < oldest_data_timestamp:
                    oldest_data_timestamp = timestamp
        result['oldest_data_timestamp'] = float('nan') if oldest_data_timestamp is None else int(oldest_data_timestamp)
        result['gps_point_count'] = len(self.points) - len(self.free_points)

        return result


    def get_road_map(self):
        """
        Extract a road map from this cluster manager.
        This is the main means of getting the output of this algorithm.
        """

        self.logger.debug("Memory usage before cluster manager prepare_road_map: %iMB" % (psutil.Process(os.getpid()).memory_info().rss / (1024**2),))

        cluster_connection_statistics = self.prepare_road_map(return_cluster_connection_statistics=True)
        self.logger.debug("Memory usage after cluster manager prepare_road_map: %iMB" % (psutil.Process(os.getpid()).memory_info().rss / (1024**2),))


        useful_edges_in_zones, cluster_position_overrides = self.get_useful_paths_in_zones()

        # first, figure out which edges are in use
        cluster_pairs = set()
        for source in self.clusters.values():
            for target in source.neighbours:
                if source.zone < 0 or source.zone != target.zone:
                    cluster_pairs.add((source, target))

        for source, target in useful_edges_in_zones:
            cluster_pairs.add((source, target))


        # now, figure out the clusters we actually need and form a list
        clusters = set()
        for pair in cluster_pairs:
            clusters.update(pair)
        clusters = list(clusters)


        # create the graph
        graph = igraph.Graph(directed=True)


        # add vertices for the clusters
        graph.add_vertices(len(clusters))

        graph.vs['cluster'] = [c.id for c in clusters]
        graph.vs['areas'] = None

        cluster_to_vertex = {c:i for i,c in enumerate(clusters)}

        for cluster in clusters:

            assert cluster.points, "A cluster without any points was encountered. This is a bug. Such a cluster should have been killed already."

            vertex = graph.vs[cluster_to_vertex[cluster]]

            if cluster in cluster_position_overrides:
                vertex['orientation'] = cluster_position_overrides[cluster].orientation
                vertex['position'] = (cluster_position_overrides[cluster].x, cluster_position_overrides[cluster].y, cluster_position_overrides[cluster].z)
                vertex['z_variance'] = cluster_position_overrides[cluster].z_variance
                vertex['weight'] = cluster_position_overrides[cluster].count
            else:
                vertex['orientation'] = cluster.position[2]
                position_z = sum(self.points[p]['z'] for p in cluster.points) / len(cluster.points)
                vertex['position'] = (cluster.position[0], cluster.position[1], position_z)
                vertex['z_variance'] = np.var(tuple(self.points[p]['z'] for p in cluster.points))
                vertex['weight'] = len(cluster.points)


        # add the edges to the graph
        # calculate the edges first and then add them in bulk - much, much faster!
        edges_list = []
        weights_list = []
        last_traversal_list = []

        for source, target in cluster_pairs:
            edges_list.append((cluster_to_vertex[source], cluster_to_vertex[target]))

            try:
                weight, source_timestamp, target_timestamp = cluster_connection_statistics[source, target]
            except KeyError:
                weight = 0
                source_timestamp = target_timestamp = float('nan')

            weights_list.append(weight)
            last_traversal_list.append(max(source_timestamp, target_timestamp))

        graph.add_edges(edges_list)
        graph.es['weight'] = weights_list
        graph.es['last_traversal'] = last_traversal_list



        # calculate and add the tagged areas
        areas = []

        tag_id_to_area_index = dict()

        geometries_by_zone = collections.defaultdict(list)

        for cluster in self.clusters.values(): # use all clusters for the zone calculation

            if cluster.zone >= 0:

                geometries_by_zone[cluster.zone].append(shapely.geometry.Point(cluster.position[:2]))

                for target, inducing_connections in cluster.direct_connections.items():

                    if inducing_connections and target.zone >= 0 and target.zone == cluster.zone:

                        line = shapely.geometry.LineString([cluster.position[:2], target.position[:2]])
                        geometries_by_zone[cluster.zone].append(line)

        self.logger.debug("Memory usage before cluster manager geometries: %iMB" % (psutil.Process(os.getpid()).memory_info().rss / (1024**2),))
        for tag_id, geometries in geometries_by_zone.items():

            assert tag_id >= 0, "A non-zone was added to the zones. There's probably a bug in the code above within this method."

            # we first buffer everything a little to ensure that we're dealing with polygons.
            # this is to work around an odd bug in shapely/libgeos which makes it crash with
            # runaway memory consumption during the buffering of full geometries in rare
            # circumstances.
            geometries = [g.buffer(1, resolution=1) for g in geometries]
            geometry = shapely.ops.unary_union(geometries)
            geometry = geometry.buffer(19)

            if isinstance(geometry, shapely.geometry.Polygon):
                polygons = (tuple(geometry.exterior.coords),)

            elif isinstance(geometry, shapely.geometry.MultiPolygon):
                polygons = tuple(tuple(p.exterior.coords) for p in geometry.geoms)

            else:
                raise NotImplementedError("The geometry object for an area is of unexpected type %s." % (type(geometry),))

            geometry = geometry.buffer(-10)
            area = road_network.RoadNetworkArea(
                    polygons=polygons,
                    name=self.tag_names[tag_id],
                    source=road_network.RoadNetworkAreaSource.TAG,
                )

            areas.append(area)
            area_index = len(areas)-1
            tag_id_to_area_index[tag_id] = area_index

        self.logger.debug("Memory usage after cluster manager geometries: %iMB" % (psutil.Process(os.getpid()).memory_info().rss / (1024**2),))


        for cluster in clusters:
            if cluster.zone >=0:
                vertex = graph.vs[cluster_to_vertex[cluster]]
                if vertex['areas'] is None:
                    vertex['areas'] = []
                vertex['areas'].append(tag_id_to_area_index[cluster.zone])


        self.logger.debug("Memory usage before cluster manager intersection areas: %iMB" % (psutil.Process(os.getpid()).memory_info().rss / (1024**2),))
        # calculate and add the intersection areas
        intersections = self.find_intersections()

        for intersection_clusters, polygons in intersections:

            area = road_network.RoadNetworkArea(
                    polygons=polygons,
                    source=road_network.RoadNetworkAreaSource.INTERSECTION,
                )

            areas.append(area)

            for cluster in intersection_clusters:
                vertex = graph.vs[cluster_to_vertex[cluster]]
                if vertex['areas'] is None:
                    vertex['areas'] = []
                vertex['areas'].append(len(areas)-1)
        self.logger.debug("Memory usage after cluster manager intersection areas: %iMB" % (psutil.Process(os.getpid()).memory_info().rss / (1024**2),))


        # assemble the result
        result = road_network.RoadNetwork(
                graph=graph,
                areas=areas,
                points_of_interest=self.get_points_of_interest(),
                metadata=self.get_metadata_info(),
            )

        self.add_z_to_road_map_areas(result)

        return result


    def add_z_to_road_map_areas(self, road_map):

        z_values = [[[] for _ in range(len(a.polygons))] for a in road_map.areas]

        polygons = [[shapely.geometry.Polygon(p) for p in a.polygons] for a in road_map.areas]

        for vertex in road_map.graph.vs:
            if vertex['areas'] is not None:
                for area_index in vertex['areas']:
                    if len(polygons[area_index]) == 1:
                        # there's only one plygon, so we don't need to do any spacial computations.
                        z_values[area_index][0].append(vertex['position'][2])
                    else:
                        # we need to check which polygons match.
                        point = shapely.geometry.Point(vertex['position'][0], vertex['position'][1])
                        for polygon_index, polygon in enumerate(polygons[area_index]):
                            if polygon.contains(point):
                                z_values[area_index][polygon_index].append(vertex['position'][2])

        for area_index, area in enumerate(road_map.areas):
            polygons = []
            for polygon_index, polygon in enumerate(area.polygons):
                if z_values[area_index][polygon_index]:
                    z_value = sum(z_values[area_index][polygon_index]) / len(z_values[area_index][polygon_index])
                    polygons.append(tuple((p[0], p[1], z_value) for p in polygon))
                else:
                    self.logger.debug("Removing empty area polygon for area '%s'" % (str(area.name),))

            if not polygons:
                self.logger.warning("All area polygons for area '%s' have been removed." % (str(area.name),))

            road_map.areas[area_index] = area._replace(polygons=polygons)



    def prepare_road_map(self, return_cluster_connection_statistics=False):
        """
        Prepare the data for extraction of a road map.

        Usually called automatically by get_road_map().
        """

        self.prune_and_merge_clusters()

        self.assign_points_to_clusters()

        self.update_connections()

        self.calculate_zones()

        # reset suppressed connections
        for cluster in self.clusters.values():
            cluster.suppressed_connections.clear()

        cluster_connection_statistics = self.get_cluster_connection_statistics()
        self.suppress_little_used_roads(cluster_connection_statistics=cluster_connection_statistics)

        self.suppress_multi_path_connections()

        self.suppress_disconnected_roads()

        if return_cluster_connection_statistics:
            return cluster_connection_statistics


    def suppress_little_used_roads(self, cluster_connection_statistics=None):
        """
        For roads outside of zones, suppress all roads that are used less than
        settings.minimum_edge_weight times.

        The intention is that roads that are used only once are treated as outliers
        in the data. However, if a road is within a zone, it is still kept since
        zones are cleaned up by different mechanisms.
        """

        if self.settings.minimum_edge_weight <= 1:
            return

        if cluster_connection_statistics is None:
            cluster_connection_statistics = self.get_cluster_connection_statistics()

        for source in self.clusters.values():
            if source.zone < 0:
                for target in source.neighbours:
                    if target.zone < 0:
                        if cluster_connection_statistics[source, target][0] < self.settings.minimum_edge_weight:
                            source.suppressed_connections.add(target)


    def get_cluster_connection_statistics(self):

        # collect traversal statistics to attach them to edges later on.
        connection_stats = dict() # dict with triplets: [weight, source_timestamp, target_timestamp]
        for source_index in range(len(self.points)):
            if source_index in self.free_points:
                continue

            source = self.points[source_index]

            if source['next'] < 0:
                continue

            target = self.points[source['next']]

            if vehicle_is_reversing(source, target):
                source, target = target, source
            del source_index

            if source['cluster_id'] < 0:
                continue
            source_cluster = self.clusters[source['cluster_id']]

            if target['cluster_id'] < 0:
                continue
            target_cluster = self.clusters[target['cluster_id']]

            if target_cluster is source_cluster:
                continue

            connection = source_cluster.connections[target_cluster]

            for current_source, current_target in connection.cluster_pairs:

                key = (current_source, current_target)

                if key not in connection_stats:
                    connection_stats[key] = [1, source['timestamp'], target['timestamp']]
                else:
                    connection_stats[key][0] += 1
                    if source['timestamp'] < connection_stats[key][1]:
                        connection_stats[key][1] = source['timestamp']
                    if target['timestamp'] > connection_stats[key][2]:
                        connection_stats[key][2] = target['timestamp']

        return connection_stats


    def get_points_of_interest(self):
        """
        Get a list of points of interest which can be attached to the road map.
        """

        result = []

        location_markers = dict()

        for digger, (position, _) in self.digger_positions.items():
            if digger in self.equipment_locations:
                location, timestamp = self.equipment_locations[digger]

                if location not in location_markers or location_markers[location][0] < timestamp:

                    point_of_interest = road_network.RoadNetworkPointOfInterest(
                        type=road_network.RoadNetworkPointOfInterestType.DIGGER_POSITION,
                        position=position,
                        last_seen_timestamp=timestamp,
                        name=location,
                        equipment=digger,
                    )

                    location_markers[location] = (timestamp, point_of_interest)

        for dump, (position, timestamp) in self.dump_positions.items():
            location = dump

            if location not in location_markers or location_markers[location][0] < timestamp:
                point_of_interest = road_network.RoadNetworkPointOfInterest(
                    type=road_network.RoadNetworkPointOfInterestType.DUMP_POSITION,
                    position=position,
                    last_seen_timestamp=timestamp,
                    name=location,
                )

                location_markers[location] = (timestamp, point_of_interest)

        for _, point_of_interest in location_markers.values():
            result.append(point_of_interest)

        for (tag_id, tag_type), (position, timestamp) in self.latest_tags.items():
            tag_name = self.tag_names[tag_id]

            if tag_type == TagType.DUMP_LOCATION:
                point_of_interest_type = road_network.RoadNetworkPointOfInterestType.TRUCK_DUMP_POSITION
            elif tag_type == TagType.LOAD_LOCATION:
                point_of_interest_type = road_network.RoadNetworkPointOfInterestType.TRUCK_DIG_POSITION
            else:
                point_of_interest_type = None

            if point_of_interest_type is not None:
                point_of_interest = road_network.RoadNetworkPointOfInterest(
                    type=point_of_interest_type,
                    position=position,
                    last_seen_timestamp=timestamp,
                    name=tag_name,
                )

                result.append(point_of_interest)

        return result

    def prune_old_points(self):
        """
        Remove obsolete points from this cluster manager.
        """

        if self.settings.disable_point_pruning:
            return

        protected_hop_count_when_pruning = self.settings.protected_hop_count_when_pruning

        point_prune_data = _PointPruneData(
            points_flagged_for_deletion=set(),
            connection_weight_adjustment=dict(),
        )

        for cluster in list(self.clusters.values()):
            cluster.flag_points_for_pruning(point_prune_data=point_prune_data)


        for point_index in point_prune_data.points_flagged_for_deletion:

            can_prune = True

            cluster_jumps = 0
            current = point_index
            while cluster_jumps < protected_hop_count_when_pruning:
                next = self.points[current]['next']
                if next < 0:
                    break
                if next not in point_prune_data.points_flagged_for_deletion:
                    if cluster_jumps < protected_hop_count_when_pruning:
                        can_prune = False
                    break
                if self.points[current]['cluster_id'] != self.points[next]['cluster_id']:
                    cluster_jumps += 1
                current = next

            if not can_prune:
                continue

            cluster_jumps = 0
            current = point_index
            while cluster_jumps < protected_hop_count_when_pruning:
                next = self.points[current]['previous']
                if next < 0:
                    break
                if next not in point_prune_data.points_flagged_for_deletion:
                    if cluster_jumps < protected_hop_count_when_pruning:
                        can_prune = False
                    break
                if self.points[current]['cluster_id'] != self.points[next]['cluster_id']:
                    cluster_jumps += 1
                current = next

            if can_prune:
                self.remove_point(point_index)



    def prune_and_merge_clusters(self):
        """
        Check for clusters that are empty and must be killed and clusters that are
        too close together and must be merged. Perform the killing and merging.
        """

        # prune empty clusters
        for cluster in list(self.clusters.values()):

            if not cluster.points:
                cluster.kill()


        if self.clusters:

            # merge clusters that are too close together
            clusters, cluster_positions = self._prepare_cluster_positions_for_tree()
            cluster_positions = np.array(cluster_positions) # workaround for some python/numpy/scipy versions
            nearest_cluster_index = sklearn.neighbors.KDTree(cluster_positions)

            all_close_by_candidates, all_close_by_candidate_distances = nearest_cluster_index.query_radius(
                    X=cluster_positions,
                    r=self.settings.cluster_merge_radius,
                    count_only=False,
                    return_distance=True,
                    sort_results=True,
                )

            clusters_to_merge = set()
            cluster_pairs_to_merge = []

            for index, (close_by_candidates, close_by_candidate_distances) in enumerate(zip(all_close_by_candidates, all_close_by_candidate_distances)):

                for candidate, distance in zip(close_by_candidates, close_by_candidate_distances):
                    if clusters[candidate] is clusters[index]:
                        # skip closeness with self.
                        continue

                    if clusters[index] not in clusters_to_merge and clusters[candidate] not in clusters_to_merge:
                        clusters_to_merge.add(clusters[index])
                        clusters_to_merge.add(clusters[candidate])
                        cluster_pairs_to_merge.append((clusters[index],clusters[candidate]))

                    # we only process the first neighbour
                    break

            for cluster1, cluster2 in cluster_pairs_to_merge:

                self.logger.debug("Merging cluster %i [%.1f, %.1f, %.1f] (%i p) with cluster %i [%.1f, %.1f, %.1f] (%i p) (distance: %.1f)" % ( (cluster1.id,) + cluster1.position + (len(cluster1.points), cluster2.id) + cluster2.position + (len(cluster2.points),point_distance(cluster1.position, cluster2.position, self.angular_weight))))

                while cluster2.points:
                    point = next(iter(cluster2.points))
                    cluster2.remove_point(point)
                    cluster1.add_point(point)

                cluster2.kill()

                # schedule all points for re-clustering in the next round in case we made too much of a mess
                self.points_to_recluster.update(cluster1.points)




    def _prepare_cluster_positions_for_tree(self):
        """Helper function that prepares the positions of all clusters so they
        can be used in a 3-dimensional spacial index and nearest neighbour computations
        correctly handle the orientation.

        This is achieved by adding every cluster to the index twice. The second copy is with
        the orientation wrapped around by 360 degrees.
        """

        clusters = []
        cluster_positions = []

        angular_weight360 = self.angular_weight * 360

        for cluster in self.clusters.values():

            # add the position of the cluster to the index
            position = cluster.position[:2] + (self.angular_weight*center_angle(cluster.position[2]),)

            clusters.append(cluster)
            cluster_positions.append(position)

            # also add a second position with the angle wrapped around
            if position[2] < 0:
                wrapped_position = position[:2] + (position[2] + angular_weight360,)
            else:
                wrapped_position = position[:2] + (position[2] - angular_weight360,)

            clusters.append(cluster)
            cluster_positions.append(wrapped_position)

        return clusters, cluster_positions


    def assign_points_to_clusters(self):
        """
        All points that are currently unassigned are assigned to clusters. Furthermore, points in clusters
        that must be re-checked (e.g. because the cluster has moved too much or a new cluster was created nearby)
        are checked and re-assigned to another cluster if necessary.

        This method is
        """


        connections_added_due_to_passing = 0
        connections_added_due_to_being_at_new_position = 0
        connections_added_due_to_being_at_new_cluster_position = 0


        if self.clusters:

            # build an index to quickly identify the closest cluster for points
            clusters, cluster_positions = self._prepare_cluster_positions_for_tree()
            cluster_positions = np.array(cluster_positions) # workaround for some python/numpy/scipy versions

            nearest_cluster_index = sklearn.neighbors.KDTree(cluster_positions)


            # collect all the points we want to (re)assign


            for cluster in self.clusters.values():
                # check if the cluster has moved and needs to be re-clustered.
                if point_distance(cluster.position, cluster.position_at_last_update, angular_weight=self.angular_weight) > self.settings.cluster_move_threshold:

                    # schedule all the points of the cluster for re-clustering
                    self.points_to_recluster.update(cluster.points)
                    cluster.position_at_last_update = cluster.position

                    # schedule all the points of neighbouring clusters for re-clustering
                    close_by_clusters = nearest_cluster_index.query_radius(X=[cluster.position], r=2*self.settings.cluster_radius, count_only=False, return_distance=False)
                    for close_by_cluster in close_by_clusters[0]:
                        self.points_to_recluster.update(clusters[close_by_cluster].points)

                    # while we're at it, also schedule the connection using this cluster for update.
                    before = len(self.cluster_connections_to_update)
                    self.cluster_connections_to_update.update(cluster.passing_connections)
                    connections_added_due_to_passing += len(self.cluster_connections_to_update) - before

                    # also schedule connections at the new position for update
                    connections = self.connection_index.intersection(tuple(cluster.position[:2])*2, objects=False)
                    before = len(self.cluster_connections_to_update)
                    for connection in (self.cluster_connections[c] for c in connections):
                        if connection.is_valid_intermediate(cluster):
                            self.cluster_connections_to_update.add(connection)
                    connections_added_due_to_being_at_new_position += len(self.cluster_connections_to_update) - before


            unclustered_points = []

            if self.points_to_recluster:

                points_to_recluster = list(self.points_to_recluster)

                distances, indices = nearest_cluster_index.query(
                        X=[(self.points[i]['x'], self.points[i]['y'], self.angular_weight*self.points[i]['orientation']) for i in points_to_recluster],
                        k=1,
                        return_distance=True,
                        dualtree=True,
                    )


                for index, point_index in enumerate(points_to_recluster):

                    if distances[index] > self.settings.cluster_radius:
                        unclustered_points.append(point_index)
                    else:
                        if self.points[point_index]['cluster_id'] >= 0:
                            if self.clusters[self.points[point_index]['cluster_id']] is not clusters[indices[index][0]]:
                                self.clusters[self.points[point_index]['cluster_id']].remove_point(point_index)
                                clusters[indices[index][0]].add_point(point_index)
                        else:
                            clusters[indices[index][0]].add_point(point_index)

        else:
            unclustered_points = list(self.points_to_recluster)

        # all points have either been checked, re-clustered or added to unclustered_points.
        self.points_to_recluster.clear()


        for point_index in unclustered_points: # note: the points in unclustered_points could still be part of a cluster at the moment. In this case they don't belong into the cluster they are in.
            point = self.points[point_index]
            candidates = self.cluster_index.intersection(
                    (point['x']-self.settings.cluster_radius, point['y']-self.settings.cluster_radius, point['x']+self.settings.cluster_radius, point['y']+self.settings.cluster_radius),
                    objects=False,
                )
            best_candidate = None
            best_distance = INF
            for candidate in candidates:
                distance = point_distance(self.clusters[candidate].position, (point['x'], point['y'], point['orientation']), angular_weight=self.angular_weight)
                if distance < best_distance:
                    best_distance = distance
                    best_candidate = candidate

            if best_distance <= self.settings.cluster_radius:
                if point['cluster_id'] >= 0:
                    if self.clusters[point['cluster_id']] is not self.clusters[best_candidate]:
                        self.clusters[point['cluster_id']].remove_point(point_index)
                        self.clusters[best_candidate].add_point(point_index)
                else:
                    self.clusters[best_candidate].add_point(point_index)
            else:
                if point['cluster_id'] >= 0:
                    self.clusters[point['cluster_id']].remove_point(point_index)
                cluster = Cluster(cluster_manager=self, point_index=point_index)
                connections = self.connection_index.intersection(tuple(cluster.position[:2])*2, objects=False)
                before = len(self.cluster_connections_to_update)
                for connection in (self.cluster_connections[c] for c in connections):
                    if connection.is_valid_intermediate(cluster):
                        self.cluster_connections_to_update.add(connection)
                connections_added_due_to_being_at_new_cluster_position += len(self.cluster_connections_to_update) - before



        self.logger.debug("Connections for update: %i/%i (pass: %i pos: %i new: %i)" % (len(self.cluster_connections_to_update), len(self.cluster_connections), connections_added_due_to_passing,connections_added_due_to_being_at_new_position,connections_added_due_to_being_at_new_cluster_position))



    def calculate_zones(self, dont_shrink_zones=False):
        """Calculate the tag zones. This looks at the tags points and edges and the
        connectivity of the graph. It then determines which clusters should be considered
        to be part of each zone. It sets the `zone` field of all clusters according
        to its results."""

        # clear all the current zone tags - we're calculating them fresh.
        for cluster in self.clusters.values():
            cluster.zone = -1
            cluster.raw_zone = -1


        # create stats of the tags of the connections so we can use them.
        direct_connection_tags = collections.defaultdict(collections.Counter)

        for connection in self.cluster_connections.values():
            previous = None
            for current in connection.cluster_sequence:
                if previous is not None:
                    if current not in previous.suppressed_connections:
                        direct_connection_tags[previous,current] += connection.tags
                previous = current


        # build individual graphs for each area
        area_graphs_by_tag_id = dict()


        # assign each connection that has a most common tag that is not None to the corresponding graph
        for (source, target), tags in direct_connection_tags.items():
            most_common, most_common_count = tags.most_common(1)[0]

            if most_common >= 0 and most_common_count >= self.settings.minimum_zone_votes:

                if most_common not in area_graphs_by_tag_id:
                    area_graphs_by_tag_id[most_common] = igraph.Graph(directed=True)
                    area_graphs_by_tag_id[most_common]['cluster_to_vertex'] = dict()

                graph = area_graphs_by_tag_id[most_common]

                for cluster in (source, target):
                    if cluster not in graph['cluster_to_vertex']:
                        graph.add_vertex()
                        vertex = graph.vs[len(graph.vs)-1]
                        vertex['cluster'] = cluster
                        graph['cluster_to_vertex'][cluster] = vertex.index

                graph.add_edge(graph['cluster_to_vertex'][source], graph['cluster_to_vertex'][target])


        # now remove the tails from each graph.
        # the idea is, to remove the part of an area that is extending along properly formed roads.
        for tag, graph in area_graphs_by_tag_id.items():

            assert tag >= 0, "This shouldn't happen. There's a bug in the above code of this method."

            # tag the clusters
            for cluster in graph.vs['cluster']:
                if cluster is not None:
                    cluster.raw_zone = tag


            # Mark the seeds. Those are the vertices that actually have the tag set to them at least once.
            graph.vs['is_seed'] = False
            for cluster, vertex in graph['cluster_to_vertex'].items():
                if tag in cluster.point_tags and cluster.point_tags[tag] > 0:
                    graph.vs[vertex]['is_seed'] = True


            del graph['cluster_to_vertex'] # this will no longer valid after deleting vertices because the indices will change.


            # Loop over the graph until we don't find anything to delete any more.
            # While this might not be the most efficient, it is easy to implement and
            # the graph should be quite small, so each iteration should be fast and only a few iterations are needed.
            while True:
                to_delete = set()

                for vertex in graph.vs:

                    # delete tails that lead into the graph until they branch for the first time or we hit a seed
                    if vertex.indegree() == 0:
                        current = vertex
                        while current.index not in to_delete and not current['is_seed'] and current.outdegree() <= 1:
                            to_delete.add(current.index)
                            if current.outdegree() == 1:
                                current = graph.vs[graph.neighbors(current, mode=igraph.OUT)[0]]
                            else:
                                break

                    # delete tails that lead out of the graph until they branch for the first time or we hit a seed
                    if vertex.outdegree() == 0:
                        current = vertex
                        while current.index not in to_delete and not current['is_seed'] and current.indegree() <= 1:
                            to_delete.add(current.index)
                            if current.indegree() == 1:
                                current = graph.vs[graph.neighbors(current, mode=igraph.IN)[0]]
                            else:
                                break

                if to_delete:
                    graph.delete_vertices(list(to_delete))
                else:
                    break




            # tag the clusters
            for cluster in graph.vs['cluster']:
                if cluster is not None:
                    cluster.zone = tag



        # Now, extend the areas to capture little things that weren't tagged.
        # The idea is, that paths that lead out of the area and back into it are added.
        # Also add paths that lead to junctions that can only read the area.
        # We repeat this process until we can't add anything else.


        MAX_STRAY_SEARCH_HOPS = 15
        MAX_META_ZONE_HOPS = 4
        LONGER_THAN_ANY_PATH = 10000000 # a value larger than the length of reasonable paths in the road network


        # get the cluster connectivity in a convenient form
        neighbours = {True: collections.defaultdict(set), False: collections.defaultdict(set)}
        for cluster in self.clusters.values():
            for neighbour, demanded_by in cluster.direct_connections.items():
                if demanded_by:
                    neighbours[True][cluster].add(neighbour)
                    neighbours[False][neighbour].add(cluster)


        # get zones that are overlapping
        meta_zone_maps = {c.zone:frozenset([c.zone]) for c in self.clusters.values() if c.zone >= 0}
        for cluster in self.clusters.values():
            for target in neighbours[True][cluster]:
                if cluster.zone != target.zone:
                    if cluster.zone >= 0 and target.zone >= 0:
                        if target.zone not in meta_zone_maps[cluster.zone]:
                            meta_zone = frozenset(meta_zone_maps[cluster.zone] | meta_zone_maps[target.zone])
                            for zone in meta_zone:
                                meta_zone_maps[zone] = meta_zone


        # we potentially start the search at all clusters. If a cluster is not in a
        # zone, we'll skip it. (we do that later). If a cluster gets added to a zone,
        # it also gets (re-)added to this set.
        clusters_to_process = set(self.clusters.values())


        # nasty little helper for debugging...
        def dp(path, message):
            return
            if path[0].id in []:
                self.logger.debug(message, [c.id for c in path], [c.zone for c in path])

        while clusters_to_process:
            cluster = clusters_to_process.pop()

            if cluster.zone < 0:
                dp([cluster], 'no zone')
                continue

            dp([cluster], 'search')


            for initial_direction in (True, False):

                queue = priorityDictionary()
                queue_data = dict()
                closed = set()
                start_point = (cluster, initial_direction)
                queue[start_point] = 0
                queue_data[start_point] = [(cluster,), 0]
                leading_elsewhere = set()
                dead_end_paths = []

                while queue:
                    current_point = queue.smallest()
                    current_cost = queue[current_point]
                    del queue[current_point]

                    if current_point in closed:
                        continue
                    closed.add(current_point)


                    current, current_direction = current_point
                    current_path, current_changes = queue_data.pop(current_point)

                    current_hops = len(current_path)-1

                    if current_hops >= MAX_STRAY_SEARCH_HOPS:
                        for curr in current_path[1:]:
                            leading_elsewhere.add(curr)
                        dp(current_path, 'max stray')
                        continue

                    if current in current_path[:-1]:
                        dp(current_path, 'loop')
                        dead_end_paths.append(current_path)
                        continue

                    if current.zone >= 0:

                        if current.zone not in meta_zone_maps[cluster.zone]:
                            for curr in current_path[1:]:
                                leading_elsewhere.add(curr)
                            dp(current_path, 'other zone')
                            continue

                        if current.zone in meta_zone_maps[cluster.zone] and current_hops > 0:
                            if current_changes <= 1:
                                if current.zone == cluster.zone or current_hops <= MAX_META_ZONE_HOPS:
                                    for curr in current_path:
                                        if curr.zone < 0:
                                            curr.zone = cluster.zone
                                            clusters_to_process.add(curr)
                                        dp(current_path, 'into same meta zone')
                            continue



                    if len(neighbours[not current_direction][current]) > 1:
                        for neighbour in neighbours[not current_direction][current]:
                            if len(current_path) < 2 or neighbour is not current_path[-2]:
                                next_point = (neighbour, not current_direction)
                                next_cost = current_cost + 1 + LONGER_THAN_ANY_PATH
                                if next_point not in queue or next_cost < queue[next_point]:
                                    queue[next_point] = next_cost
                                    queue_data[next_point] = [current_path+(neighbour,), current_changes+1]


                    if len(neighbours[current_direction][current]) > 0:

                        for neighbour in neighbours[current_direction][current]:
                            next_point = (neighbour, current_direction)
                            next_cost = current_cost + 1
                            if next_point not in queue or next_cost < queue[next_point]:
                                queue[next_point] = next_cost
                                queue_data[next_point] = [current_path+(neighbour,), current_changes]

                    else:
                        dead_end_paths.append(current_path)



                for path in dead_end_paths:

                    for curr in path:
                        if curr in leading_elsewhere:
                            dp(current_path, 'dead end keep')
                            break
                    else:
                        dp(current_path, 'dead end cull')
                        for curr in path:
                            if curr.zone < 0:
                                curr.zone = cluster.zone
                                clusters_to_process.add(curr)









    def get_useful_paths_in_zones(self):
        """
        Determine which paths within a zone are useful to be included into the output road map.
        """

        if not self.clusters:
            return dict(), dict()

        SEARCH_RADIUS = self.settings.intersection_search_radius
        ANGULAR_THRESHOLD = self.settings.intersection_search_angular_threshold
        LEFT_HAND_SIDE_DRIVING = (self.settings.roadnetwork_driving_side == 'left')
        SEARCH_DISTANCE = 10

        def find_opposite_clusters(cluster):


            result = []

            nearby_candidates = self.cluster_index.intersection(
                    (
                        cluster.position[0]-SEARCH_RADIUS,
                        cluster.position[1]-SEARCH_RADIUS,
                        cluster.position[0]+SEARCH_RADIUS,
                        cluster.position[1]+SEARCH_RADIUS,
                    ),
                    objects=False,
                )


            for candidate in nearby_candidates:
                candidate = self.clusters[candidate]

                # We now vet the cluster. The order of the tests doesn't really matter.
                # Let's put the cheap tests first to cull as much as we can quickly.

                # use the search radius as circle (the index look up above was in a box).
                if point_distance2d(cluster.position, candidate.position) > SEARCH_RADIUS:
                    continue

                # only consider clusters that roughtly go in the opposite direction.
                delta_orientation = center_angle(candidate.position[2] - cluster.position[2] + 180)

                if delta_orientation < -ANGULAR_THRESHOLD or ANGULAR_THRESHOLD < delta_orientation:
                    continue


                # only consider clusters that are on the correct side (lane wise)
                delta_x = candidate.position[0] - cluster.position[0]
                delta_y = candidate.position[1] - cluster.position[1]

                delta_direction = center_angle(math.degrees(math.atan2(delta_y, delta_x)) - cluster.position[2])

                if LEFT_HAND_SIDE_DRIVING:
                    if delta_direction > 0:
                        continue
                else:
                    if delta_direction < 0:
                        continue

                result.append(candidate)


            return result


        wanted_connections = collections.defaultdict(set)


        into_zone = [];
        out_of_zone = [];
        meta_zone_maps = {c.zone:frozenset([c.zone]) for c in self.clusters.values() if c.zone >= 0}



        for cluster in self.clusters.values():
            for target in cluster.neighbours:
                if cluster.zone != target.zone:

                    if cluster.zone >= 0 and target.zone < 0:
                        out_of_zone.append((cluster, target))

                    if cluster.zone < 0 and target.zone >= 0:
                        into_zone.append((cluster, target))

                    if cluster.zone >= 0 and target.zone >= 0:
                        if target.zone not in meta_zone_maps[cluster.zone]:
                            meta_zone = frozenset(meta_zone_maps[cluster.zone] | meta_zone_maps[target.zone])
                            for zone in meta_zone:
                                meta_zone_maps[zone] = meta_zone


        cluster_list = list(self.clusters.values())
        nearest_cluster_index = sklearn.neighbors.KDTree(np.array([c.position[:2] for c in cluster_list]))

        location_positions = dict()
        for equipment, (position, _) in self.digger_positions.items():
            if equipment in self.equipment_locations:
                location_positions[self.equipment_locations[equipment][0]] = tuple(position[:2])
        for equipment, (position, _) in self.dump_positions.items():
            location_positions[equipment] = tuple(position[:2])

        location_list = list(location_positions)

        #An exception happened in this block checking if location list has expected data and dimension
        #Can use np.shape but I think it is too much
        if location_list is not None:
            self.logger.debug("Size of first element of location_list:" + str(len(location_list)))
        else:
            self.logger.debug("location_list is None")

        if not location_list:
            distances, indices  = [], []
        else:
            distances, indices = nearest_cluster_index.query(
                    X=[location_positions[i] for i in location_list],
                    k=1,
                    return_distance=True,
                    dualtree=True,
                )

        for (index,) in indices:
            cluster = cluster_list[index]
            if cluster.zone >= 0:
                for source,target in into_zone:
                    if meta_zone_maps[target.zone] is meta_zone_maps[cluster.zone]:
                        wanted_connections[target].add(cluster)
                for source,target in out_of_zone:
                    if meta_zone_maps[source.zone] is meta_zone_maps[cluster.zone]:
                        wanted_connections[cluster].add(source)


        #needed_goals = dict()

        neighbours = collections.defaultdict(set)
        reverse_neighbours = collections.defaultdict(set)

        for cluster in self.clusters.values():
            for neighbour, counter in cluster.get_neighbours_with_counts(dont_suppress_connections=True).items():
                if counter.forward > 0:
                    neighbours[cluster].add(neighbour)
                    reverse_neighbours[neighbour].add(cluster)
                if counter.reverse > 0:
                    reverse_neighbours[cluster].add(neighbour)
                    neighbours[neighbour].add(cluster)

        for index, edge in enumerate(into_zone):
            source, target = edge

            returning_clusters = set(find_opposite_clusters(source))

            for goal, _ in out_of_zone:

                if meta_zone_maps.get(target.zone, None) is not meta_zone_maps.get(goal.zone, None):
                    continue


                goal_is_going_elsewhere = False

                stack = [(goal, 0)]
                closed = set()

                while stack:
                    current, distance = stack.pop()

                    if current in closed:
                        continue

                    closed.add(current)

                    if current in returning_clusters:
                        continue

                    if distance >= SEARCH_DISTANCE:
                        goal_is_going_elsewhere = True
                        break

                    for neighbour in neighbours[current]:
                        stack.append((neighbour, distance+1))


                if goal_is_going_elsewhere:

                    source_is_going_elsewhere = False

                    goal_returning_clusters = set(find_opposite_clusters(goal))

                    stack = [(source, 0)]
                    closed = set()

                    while stack:
                        current, distance = stack.pop()

                        if current in closed:
                            continue

                        closed.add(current)

                        if current in goal_returning_clusters:
                            continue

                        if distance >= SEARCH_DISTANCE:
                            source_is_going_elsewhere = True
                            break

                        for neighbour in reverse_neighbours[current]:
                            stack.append((neighbour, distance+1))

                    if not source_is_going_elsewhere:
                        goal_is_going_elsewhere = False


                if goal_is_going_elsewhere:
                    wanted_connections[target].add(goal)


        needed_edges_in_zone = collections.defaultdict(list)
        better_cluster_points = collections.defaultdict(set)

        for index, (source, goals) in enumerate(wanted_connections.items()):

            goals = set(goals) # we want to modify our copy
#


            # first try to find actual taken paths that lead to the goal
            paths = collections.defaultdict(lambda:collections.defaultdict(list))
            for point_index in source.points:
                if self.points[point_index]['cluster_id'] < 0:
                    continue
                path = [self.clusters[self.points[point_index]['cluster_id']]]
                point_path = [point_index]
                current = self.points[point_index]['next']
                while current >= 0 and self.points[current]['cluster_id'] >= 0:
                    point_path.append(current)
                    cluster = self.clusters[self.points[current]['cluster_id']]
                    if meta_zone_maps.get(cluster.zone, None) is not meta_zone_maps.get(source.zone, None):
                        break
                    if cluster is not path[-1]:
                        if cluster in path[-1].connections:
                            path.extend(path[-1].connections[cluster].cluster_sequence[1:])
                        elif path[-1] in cluster.connections:
                            path.extend(reversed(cluster.connections[path[-1]].cluster_sequence[:-1]))
                        else:
                            raise AssertionError("The GPS data has a connection which does not exist. There's a bug somwhere.")
                        if cluster in goals:
                            paths[cluster][tuple(path)].append(list(point_path))
                    current = self.points[current]['next']

            for goal, paths_to_goal in paths.items():
                best_path = None
                best_count = 0
                for path, point_paths in paths_to_goal.items():
                    if best_count < len(point_paths):

                        previous = None
                        for current in path:
                            if previous is not None:
                                if current in previous.suppressed_connections:
                                    break
                        else:
                            best_count = len(point_paths)
                            best_path = path


                if best_path is not None:
                    previous = None
                    for current in best_path:
                        if previous is not None:
                            needed_edges_in_zone[(previous, current)].append('H')
                        previous = current

                    for point_path in paths_to_goal[best_path]:
                        for point in point_path:
                            better_cluster_points[self.points[point]['cluster_id']].add(point)


#                    previous = None
#                    for current in best_path:
#                        if current not in better_cluster_positions:
#                            continue
#                        if previous is not None:
#                            plt.plot([better_cluster_positions[previous].x, better_cluster_positions[current].x], [better_cluster_positions[previous].y, better_cluster_positions[current].y], color='black')
#                        previous = current


                    goals.remove(goal)



            # now search for paths to the remaining goals

            queue = priorityDictionary()
            queue[source] = 0

            coming_from = dict()

            closed = set()

            while queue and goals:

                current = queue.smallest()
                current_distance = queue[current]
                del queue[current]

                if current in closed:
                    continue
                closed.add(current)

                if current in goals:
                    goals.remove(current)

                    reverse_current = current
                    while reverse_current in coming_from:
                        needed_edges_in_zone[(coming_from[reverse_current], reverse_current)].append('D')
                        reverse_current = coming_from[reverse_current]

                if meta_zone_maps.get(current.zone, None) is meta_zone_maps.get(source.zone, None):
                    for neighbour in neighbours[current]:
                        if neighbour not in closed:
                            neighbour_distance = current_distance + dubins_distance(current.position, neighbour.position, self.settings.dubins_turning_radius)

                            if neighbour not in queue or neighbour_distance < queue[neighbour]:
                                queue[neighbour] = neighbour_distance
                                coming_from[neighbour] = current


#        import matplotlib.pyplot as plt
#        for (source, target), info in needed_edges_in_zone.items():
#            plt.plot([source.position[0], target.position[0]], [source.position[1], target.position[1]], color='green')
#            plt.text(0.5*(source.position[0] + target.position[0]), 0.5*(source.position[1] + target.position[1]), info)



        cluster_position_overrides = dict()
        for cluster_id, points in better_cluster_points.items():
            cluster_position_overrides[self.clusters[cluster_id]] = point_mean([self.points[i] for i in points])


        return needed_edges_in_zone, cluster_position_overrides



    def suppress_multi_path_connections(self):
        """
        Attempts to clean up situations where there are multiple paths belonging to the
        same road segment (e.g. a shortcut in addition to the real curve).
        """


        if not self.settings.improve_multipath:
            return


        incoming_connections = collections.defaultdict(set)
        for cluster in self.clusters.values():
            for neighbour in cluster.neighbours:
                incoming_connections[neighbour].add(cluster)


        MAX_SEARCH_DISTANCE = self.settings.multipath_search_max_path_hops

        IGNORE_ZONES = self.settings.multipath_search_ignore_zones

        for start in self.clusters.values():


            # use clusters as start points that have more than one outgoing connection.
            if ( (not IGNORE_ZONES) or start.zone < 0) and len(start.neighbours) > 1:


                # do a depth first search to find the local multi-connections
                stack = [(start,)]
                paths_to_goals = collections.defaultdict(list)

                while stack:
                    path = stack.pop()

                    for neighbour in path[-1].neighbours:
                        if (not IGNORE_ZONES) or neighbour.zone < 0:
                            new_path = path + (neighbour,)
                            assert new_path not in paths_to_goals[neighbour]
                            paths_to_goals[neighbour].append(new_path)
                            if len(new_path) > 1 and len(new_path) <= MAX_SEARCH_DISTANCE:
                                stack.append(new_path)


                # prune all single paths
                paths_to_goals = {g:p for g, p in paths_to_goals.items() if len(p) > 1}

                # prune paths that don't meet at the last node
                paths_to_goals = {g:p for g, p in paths_to_goals.items() if len(set(i[-2] for i in p)) > 1}

                # prune paths that don't split at the first node
                paths_to_goals = {g:p for g, p in paths_to_goals.items() if len(set(i[1] for i in p)) > 1}


                # find the strongest path
                for goal, paths in paths_to_goals.items():

                    # find the strongest path
                    best_paths = []
                    best_weight = -INF

                    for path in paths:
                        min_weight = INF
                        weight_sum = 0
                        for index in range(len(path)-1):
                            weight = 0
                            source = path[index]
                            source_target = (source, path[index+1])

                            for connection in source.passing_connections:
                                if source_target in connection.cluster_pairs:
                                    weight += connection.weight

                            if weight < min_weight:
                                min_weight = weight

                            weight_sum += weight


                        if best_weight <= min_weight:
                            if best_weight < min_weight:
                                best_paths = []
                                best_weight = min_weight

                            best_paths.append((path, weight_sum / (len(path)-1) ))


                    best_paths.sort(key=lambda x:-x[1])

                    assert best_paths, "There is no best path. This cannot happen since we started with a non-empty set of paths. Something is very wrong."

                    best_path = best_paths[0][0]

                    # find edges that could be removed (suppressed)

                    all_clusters_in_path_set = set()
                    for path in paths:
                        all_clusters_in_path_set.update(path)

                    clusters_in_best_path = set(best_path)

                    cluster_pairs_in_best_path = set((best_path[i], best_path[i+1]) for i in range(len(best_path)-1))

                    connections_to_suppress = set()

                    for cluster in all_clusters_in_path_set:


                        for neighbour in cluster.neighbours:


                            if (cluster, neighbour) in cluster_pairs_in_best_path:
                                # never delete anything in the best path
                                continue

                            # determine whether the neighbour can reach anything else
                            # than going back to the best path
                            can_only_reach_the_best_path = True
                            if neighbour not in clusters_in_best_path:
                                stack = [neighbour]
                                closed = set()
                                while stack and can_only_reach_the_best_path:
                                    current = stack.pop()
                                    if current not in closed:
                                        closed.add(current)
                                        for next_cluster in current.neighbours:
                                            if next_cluster in all_clusters_in_path_set:
                                                if next_cluster not in clusters_in_best_path:
                                                    stack.append(next_cluster)
                                            else:
                                                can_only_reach_the_best_path = False
                                                break


                            if can_only_reach_the_best_path:

                                # determine whether the cluster can be reached from anything else
                                # but the best path
                                can_only_be_reached_from_best_path = True
                                if cluster not in clusters_in_best_path:
                                    stack = [cluster]
                                    closed = set()
                                    while stack and can_only_be_reached_from_best_path:
                                        current = stack.pop()
                                        if current not in closed:
                                            closed.add(current)

                                            for next_cluster in incoming_connections[current]:
                                                if (next_cluster,current) not in next_cluster.suppressed_connections:
                                                    if next_cluster in all_clusters_in_path_set:
                                                        if next_cluster not in clusters_in_best_path:
                                                            stack.append(next_cluster)
                                                    else:
                                                        can_only_be_reached_from_best_path = False
                                                        break


                                if can_only_be_reached_from_best_path:
                                    connections_to_suppress.add((cluster,neighbour))

                    for source, target in connections_to_suppress:
                        source.suppressed_connections.add(target)


    def find_intersections(self):
        """
        Analyses the graph structure to detect intersections in the road map.
        """

        SEARCH_RADIUS = self.settings.intersection_search_radius
        ANGULAR_THRESHOLD = self.settings.intersection_search_angular_threshold
        LEFT_HAND_SIDE_DRIVING = (self.settings.roadnetwork_driving_side == 'left')
        MAX_INTERSECTION_PATH_HOPS = self.settings.intersection_search_max_path_hops


        result = []


        # build the graph
        graph = igraph.Graph(directed=True)
        graph.add_vertices(len(self.clusters))
        graph.vs['cluster'] = list(self.clusters.values())
        graph['clusters'] = {v['cluster']:v.index for v in graph.vs}
        for cluster, vertex in graph['clusters'].items():
            for neighbour in cluster.neighbours:
                graph.add_edge(vertex, graph['clusters'][neighbour])

        for cluster, vertex_index in graph['clusters'].items():
            vertex = graph.vs[vertex_index]
            if cluster.zone >= 0:
                vertex['closed'] = True
            elif vertex.indegree() <= 1 and vertex.outdegree() <= 1:
                vertex['closed'] = True
            else:
                vertex['closed'] = False


        partners_split_to_join = collections.defaultdict(set)
        partners_join_to_split = collections.defaultdict(set)


        for seed_vertex in graph.vs:
            if seed_vertex['closed']:
                continue


            if seed_vertex.outdegree() > 1:

                seed_cluster = seed_vertex['cluster']

                nearby_candidates = self.cluster_index.intersection(
                        (
                            seed_cluster.position[0]-SEARCH_RADIUS,
                            seed_cluster.position[1]-SEARCH_RADIUS,
                            seed_cluster.position[0]+SEARCH_RADIUS,
                            seed_cluster.position[1]+SEARCH_RADIUS,
                        ),
                        objects=False,
                    )


                for candidate in nearby_candidates:
                    candidate = self.clusters[candidate]

                    # We now vet the cluster. The order of the tests doesn't really matter.
                    # Let's put the cheap tests first to cull as much as we can quickly.

                    # don't look at clusters we already visited.
                    if graph.vs[graph['clusters'][candidate]]['closed']:
                        continue

                    # only consider clusters that have the correct degree
                    if graph.vs[graph['clusters'][candidate]].indegree() <= 1:
                        continue

                    # use the search radius as circle (the index look up above was in a box).
                    if point_distance2d(seed_cluster.position, candidate.position) > SEARCH_RADIUS:
                        continue

                    # only consider clusters that roughtly go in the opposite direction.
                    delta_orientation = center_angle(candidate.position[2] - seed_cluster.position[2] + 180)

                    if delta_orientation < -ANGULAR_THRESHOLD or ANGULAR_THRESHOLD < delta_orientation:
                        continue


                    # only consider clusters that are on the correct side (lane wise)
                    delta_x = candidate.position[0] - seed_cluster.position[0]
                    delta_y = candidate.position[1] - seed_cluster.position[1]

                    delta_direction = center_angle(math.degrees(math.atan2(delta_y, delta_x)) - seed_cluster.position[2])

                    if LEFT_HAND_SIDE_DRIVING:
                        if delta_direction > 0:
                            continue
                    else:
                        if delta_direction < 0:
                            continue

                    partners_split_to_join[seed_cluster].add(candidate)
                    partners_join_to_split[candidate].add(seed_cluster)


        self._last_cluster_pairs_for_debug = [] # don't commit
        for source, targets in partners_split_to_join.items(): # don't commit
            for target in targets: # don't commit
                self._last_cluster_pairs_for_debug.append((source, target)) # don't commit


        for seed_vertex in graph.vs:
            if seed_vertex['closed']:
                continue

            if seed_vertex.outdegree() > 1 or seed_vertex.indegree() > 1:

                stack = [seed_vertex]

                belonging_edges = set()

                while stack:

                    current = stack.pop()

                    if current['closed']:
                        continue
                    current['closed'] = True

                    if current['cluster'].zone >= 0:
                        continue

                    if current.outdegree() > 1:
                        for other in partners_split_to_join[current['cluster']]:
                            other = graph.vs[graph['clusters'][other]]
                            if other['cluster'].zone < 0:
                                belonging_edges.add(frozenset((current,other)))
                                stack.append(other)

                    if current.indegree() > 1:
                        for other in partners_join_to_split[current['cluster']]:
                            other = graph.vs[graph['clusters'][other]]
                            if other['cluster'].zone < 0:
                                belonging_edges.add(frozenset((current,other)))
                                stack.append(other)

                    if current.outdegree() > 1 and current.indegree() <= 1:
                        sub_stack = [(current,)]
                        while sub_stack:
                            sub_path = sub_stack.pop()

                            for sub_neighbour in sub_path[-1].neighbors(mode=igraph.OUT):
                                if sub_neighbour['cluster'].zone < 0:
                                    new_path = sub_path + (sub_neighbour,)
                                    if sub_neighbour.indegree() > 1:
                                        for index in range(len(new_path)-1):
                                            belonging_edges.add(frozenset((new_path[index],new_path[index+1])))
                                            stack.append(sub_neighbour)
                                    elif len(new_path) <= MAX_INTERSECTION_PATH_HOPS:
                                        sub_stack.append(new_path)

                    if current.indegree() > 1 and current.outdegree() <= 1:
                        sub_stack = [(current,)]
                        while sub_stack:
                            sub_path = sub_stack.pop()

                            for sub_neighbour in sub_path[-1].neighbors(mode=igraph.IN):
                                if sub_neighbour['cluster'].zone < 0:
                                    new_path = sub_path + (sub_neighbour,)
                                    if sub_neighbour.outdegree() > 1:
                                        for index in range(len(new_path)-1):
                                            belonging_edges.add(frozenset((new_path[index],new_path[index+1])))
                                            stack.append(sub_neighbour)
                                    elif len(new_path) <= MAX_INTERSECTION_PATH_HOPS:
                                        sub_stack.append(new_path)


                belonging_vertices = set()
                for edge in belonging_edges:
                    belonging_vertices.update(edge)

                if belonging_edges:
                    geometries = []
                    for source, target in belonging_edges:
                        line = shapely.geometry.LineString([source['cluster'].position[:2], target['cluster'].position[:2]])
                        geometries.append(line)



                    # we first buffer everything a little to ensure that we're dealing with polygons.
                    # this is to work around an odd bug in shapely/libgeos which makes it crash with
                    # runaway memory consumption during the buffering of full geometries in rare
                    # circumstances.
                    geometries = [g.buffer(1, resolution=1) for g in geometries]
                    geometry = shapely.ops.unary_union(geometries)
                    geometry = geometry.buffer(19)
                    geometry = geometry.buffer(-10)



                    if isinstance(geometry, shapely.geometry.Polygon):
                        polygons = (tuple(geometry.exterior.coords),)

                    elif isinstance(geometry, shapely.geometry.MultiPolygon):
                        polygons = tuple(tuple(p.exterior.coords) for p in geometry.geoms)

                    else:
                        raise NotImplementedError("The geometry object for an area is of unexpected type %s." % (type(geometry),))

                    result.append((tuple(v['cluster'] for v in belonging_vertices), polygons))


        return result


    def suppress_disconnected_roads(self):
        """
        Search for parts of the road map which are disconnected to the rest and suppress them.
        """

        # first find the connected components of the graph
        components = dict()

        for cluster in self.clusters.values():
            if cluster in components:
                continue

            component_id = len(components)
            stack = [cluster]

            clusters_seen_in_this_round = []
            joined_another_component = False

            while stack:
                current = stack.pop()
                clusters_seen_in_this_round.append(current)

                if current in components:

                    if components[current] != component_id:
                        # we reached another cluster. Rename everything we have in this cluster so far.
                        new_component_id = components[current]
                        if joined_another_component:
                            # we need to search everything we have. This should be a rare event once the main component of the graph has been found.
                            for key, value in components.items():
                                if value == component_id:
                                    components[key] = new_component_id
                        else:
                            # This should be the most common case which is fast.
                            for key in clusters_seen_in_this_round:
                                components[key] = new_component_id

                        joined_another_component = True
                        component_id = new_component_id

                else:
                    components[current] = component_id

                    for neighbour in current.neighbours:
                        stack.append(neighbour)


        # reorganise the data structure so things are grouped the other way around and sorted by component size
        component_lists = collections.defaultdict(list)
        for cluster, component_id in components.items():
            component_lists[component_id].append(cluster)
        components = sorted(component_lists.values(), key=lambda x:-len(x))

        # work out which components we want to suppress
        to_suppress = list()
        for index, component in enumerate(components):
            if index >= self.settings.maximum_road_network_component_count:
                to_suppress.append(component)
            elif len(component) < self.settings.minimum_road_network_component_size:
                to_suppress.append(component)

        # suppress all connections in the components we don't like
        for component in to_suppress:
            for cluster in component:
                cluster.suppressed_connections.update(cluster.neighbours)


    def update_connections(self):
        """Run the update procedure for all connections that require updating."""

        while self.cluster_connections_to_update:
            connection = self.cluster_connections_to_update.pop()
            if connection.id in self.cluster_connections:
                connection.update()

    def perform_idle_tasks(self):

        task_performed = False

        if self.points_to_recluster:
            self.assign_points_to_clusters()
            task_performed = True

        if self.cluster_connections_to_update:
            self.update_connections()
            task_performed = True

        return task_performed


    def recalculate_all_connections(self, method=None, improve_multipath=None):
        """
        Intended for testing only. Allows to recalculate all cluster connection
        sequences.

        If method is given, the "intermediate_point_method" of the settings is
        changed before the calculation. This allows to evaluate the effect of
        using different methods.
        """

        if method is not None:
            self.settings = self.settings._replace(intermediate_point_method=method)

        if improve_multipath is not None:
            self.settings = self.settings._replace(improve_multipath=improve_multipath)


        self.cluster_connections_to_update.update(self.cluster_connections.values())

        self.update_connections()
