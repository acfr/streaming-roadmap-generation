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


import math
import numpy as np
import collections

from .helpers import INF, EPSILON, center_angle, point_difference, vehicle_is_reversing
from .cluster_connection import ClusterConnection


class ForwardReverseCounter(object):
    def __init__(self, forward=0, reverse=0):
        self.forward = forward
        self.reverse = reverse

    @property
    def count(self):
        return self.forward + self.reverse

    def add_forward(self):
        self.forward += 1

    def remove_forward(self):
        self.forward -= 1
        assert self.forward >= 0

    def add_reverse(self):
        self.reverse += 1

    def remove_reverse(self):
        self.reverse -= 1
        assert self.reverse >= 0


class Cluster(object):
    """
    A cluster object represents a set of GPS points that share a similar position
    and orientation. Clusters form the vertices of the road map that is generated
    by this algorithm.

    Gps points can be dynamically added to and removed from cluster objects. The position
    of the cluster is updated accordingly.

    Cluster objects also keep track of various types of connections between clusters.
    """


    def _init_empty(self):

        self.cluster_manager = None
        """The :class:`ClusterManager` that drives creating of the map."""

        self.id = None
        """An integer which uniquely identifies the cluster within the cluster manager."""

        self.position = None
        """
        The position and orientation of this cluster.
        Undefined as long as no points are part of this cluster, but we immediately add the first point below."""

        self.points = set()
        """the set of the indices of the points that are held by this cluster."""

        self.point_tags = collections.Counter()
        """A multiset of the tags that are assigned to the gps points that belong to this cluster."""

        self.zone = -1
        """If the cluster is tagged to be part of a zone (named area), it's id is stored here."""

        self.raw_zone = -1
        """The raw zone of the cluster before further processing. Mostly useful for debugging and analysis."""


        self.desired_connections = collections.defaultdict(ForwardReverseCounter)
        """
        Multiset of connections to other clusters that are mandated by gps points. Whenever a gps point
        that is part of this cluster is followed by a gps point thata is part of another cluster, the other
        cluster is added to this multiset (i.e. it's count is increased.)
        """

        self.connections = dict()
        """
        Whenever another cluster is part of `desired_connections` (i.e. its count is not zero), a connection
        object connecting the two clusters is created. This dict holds all such connection objects. The key
        is the target cluster and the value is the :class:`ClusterConnection` object."""

        self.passing_connections = set()
        """
        :class:`ClusterConnection` objects attempt to improve the graph by identifying a set of intermediate
        clusters that the connection passes through. Here, all :class:`ClusterConnection` objects that touch
        this cluster, either as source, target or intermediate waypoint, are stored for easy access.
        """

        self.direct_connections = collections.defaultdict(set)
        """
        Not all clusters that are connected according to `desired_connections` are actually connected
        in the final road map. This is due to intermediate waypoints that are found by the :class:`ClusterConnection`
        objects. This field stores the clusters that are actually connected to this cluster (i.e. clusters
        that follow this cluster in the cluster sequence of a ClusterConnection). It is implemented as
        dict of sets such that each ClusterConnection that mandates this direct connection is added to the relevant
        set. Thus, it can be kept track of why a connection is there and when the connection must be removed again.
        """

        self.suppressed_connections = set()
        """
        If a connection ought to be suppressed even if it is part of direct_connections, then the neighbouring
        cluster is added to this set. This allows to temporarily mark connections as removed without interfering
        with the multiset semantic of direct_connections.
        This is intended for post-processing steps that happen before a map is presented as output.
        Note: clusters may be listed in suppressed_connections even though they are not in direct_connections.
        """

        self.position_at_last_update = None
        """Whenever a cluster has moved too much (due to points being added or removed), things should be checked.
        E.g. whether the points should still belong to this cluster and whether all the passing connections should
        still have the right intermediate clusters, etc.
        Here, we store the position of the cluster when it was last checked to be correct so we can determine
        how much it has moved and whether we should update it.
        """



    def __init__(self, cluster_manager, point_index):

        self._init_empty()

        self.cluster_manager = cluster_manager

        self.id = self.cluster_manager.next_cluster_id

        self.cluster_manager.next_cluster_id += 1 # increment counter to form unique ids.


        self.cluster_manager.clusters[self.id] = self # register this cluster at the manager.
        self.cluster_manager.points[point_index]['cluster_id'] = self.id # register that the initial point is part of this cluster

        point = self.cluster_manager.points[point_index]
        self.set_position((point['x'], point['y'], point['orientation'])) # set the position of this cluster to be at the initial point
        self.points.add(point_index)

        # If the point has a tag, register it.
        self.point_tags[point['tag']] += 1


        self.position_at_last_update = self.position


        # we just added our initial point to the cluster, so we need to process the connections it demands.
        self._process_connections_after_adding_point(point_index)

        #self._assert_mean()


    @classmethod
    def make_new_when_loading_from_snapshot(cls, cluster_manager, id, points, position_at_last_update):

        self = cls.__new__(cls)

        self._init_empty()

        self.cluster_manager = cluster_manager

        self.id = id

        self.cluster_manager.clusters[self.id] = self # register this cluster at the manager.

        self.points = set(points)

        for point in self.points:
            assert self.cluster_manager.points[point]['cluster_id'] == self.id, "The loaded point is not assigned to this cluster. Something is wrong."


        for point in self.points:
            self.point_tags[self.cluster_manager.points[point]['tag']] += 1


        # we need to do some complication to find the 'correct' mean of the orientation.
        sum_x = 0
        sum_y = 0
        for point_index in self.points:
            sum_x += math.cos(math.radians(self.cluster_manager.points[point]['orientation']))
            sum_y += math.sin(math.radians(self.cluster_manager.points[point]['orientation']))
        center_orientation = math.degrees(math.atan2(sum_y, sum_x))

        orientation_sum = 0
        for point in self.points:
            orientation_sum += center_angle(self.cluster_manager.points[point]['orientation'] - center_orientation)
        orientation_mean = center_angle( (orientation_sum / len(self.points)) + center_orientation)


        position = (
                sum(self.cluster_manager.points[p]['x'] for p in self.points) / len(self.points),
                sum(self.cluster_manager.points[p]['y'] for p in self.points) / len(self.points),
                orientation_mean,
            )

        self.set_position(position)

        self.position_at_last_update = tuple(position_at_last_update)


        for point in self.points:
            self._process_connections_after_adding_point(point, dont_instantiate_connection=True, other_cluster_may_be_missing=True)


        return self


    def kill(self):
        """Destroy this object: de-register it from the cluster manager, etc."""

        assert not self.points
        assert not [i for i in self.desired_connections.values() if i.count > 0]
        assert not self.connections

        if self.position is not None:
            self.cluster_manager.cluster_index.delete(id=self.id, coordinates=tuple(self.position)[:2]*2)

        del self.cluster_manager.clusters[self.id]

        for connection in list(self.passing_connections):
            connection.update()

        assert not self.passing_connections
        assert not [i for i in self.direct_connections.values() if i]


        self.__dict__.clear()

    @property
    def logger(self):
        return self.cluster_manager.logger


    @property
    def neighbours(self):
        return [n for n,v in self.direct_connections.items() if v and n not in self.suppressed_connections]

    def get_neighbours_with_counts(self, dont_suppress_connections=False):
        result = dict()
        for neighbour, connections in self.direct_connections.items():
            if dont_suppress_connections or neighbour not in self.suppressed_connections:
                forward = 0
                reverse = 0
                for connection in connections:
                    counter = connection.source.desired_connections[connection.target]
                    forward += counter.forward
                    reverse += counter.reverse

                result[neighbour] = ForwardReverseCounter(forward=forward, reverse=reverse)

        return result

    def get_connection_counts(self, neighbour):
        if neighbour in self.suppressed_connections:
            return ForwardReverseCounter()
        else:
            forward = 0
            reverse = 0
            for connection in self.direct_connections[neighbour]:
                counter = connection.source.desired_connections[connection.target]
                forward += counter.forward
                reverse += counter.reverse

            return ForwardReverseCounter(forward=forward, reverse=reverse)


    def set_position(self, value):
        """Set the position of this cluster and do the house-keeping that comes with it."""

        if self.position is None or not np.all(self.position == value):
            if self.position is not None:
                self.cluster_manager.cluster_index.delete(id=self.id, coordinates=tuple(self.position)[:2]*2) # remove the old position from the spacial index.

            self.position = tuple(value)
            self.cluster_manager.cluster_index.insert(id=self.id, coordinates=tuple(self.position)[:2]*2) # add the new position to the spacial index.

            #self._assert_mean()


    def _assert_mean(self):
        """For debugging: raise an exception if the position of the cluster does not match the mean position of all its gps points."""
        raise NotImplementedError()

        if self.points:
            try:
                assert np.linalg.norm(np.mean([p[:2] for p in self.points], axis=0) - np.array(self.position[:2])) < 0.5
            except:
                self.logger.debug(str(len(self.points)))
                self.logger.debug(str(self.position))
                self.logger.debug(str(np.mean([p[:2] for p in self.points], axis=0)))
                self.logger.debug(str(np.linalg.norm(np.mean([p[:2] for p in self.points], axis=0) - np.array(self.position[:2]))))
                raise


    def add_point(self, point_index):
        """Add a point to this cluster and do all the house keeping that comes with doing so."""

        #self._assert_mean()

        assert point_index not in self.points
        self.cluster_manager.points[point_index]['cluster_id'] = self.id # register where the point is.

        # update the position of the cluster. This calculation allows to do this without having
        # to calculate the mean from scratch. Don't change anything here unless you knwo
        # what you are doing.
        position = self.position
        self.points.add(point_index)
        point = self.cluster_manager.points[point_index]
        self.set_position(np.add(position, np.divide(point_difference((point['x'], point['y'], point['orientation']), position), len(self.points))))

        # register if the point is tagged.
        self.point_tags[point['tag']] += 1

        # update the inter-cluster connections that might be demanded by this point.
        self._process_connections_after_adding_point(point_index)

        #self._assert_mean()



    def _process_connections_after_adding_point(self, point_index, dont_instantiate_connection=False, other_cluster_may_be_missing=False):
        """This method is called after a point is added. It takes care of instantiating
        the necessary connections to other clusters.

        It is called by __init__() and add_point().
        """

        point = self.cluster_manager.points[point_index]

        other_index = point['next']
        if other_index >= 0:
            other_point = self.cluster_manager.points[other_index]
            if other_point['cluster_id'] >= 0:
                tag = point['edge_tag']
                try:
                    other_cluster = self.cluster_manager.clusters[other_point['cluster_id']]
                except:
                    if not other_cluster_may_be_missing:
                        raise
                else:
                    if other_cluster is not self:
                        if not vehicle_is_reversing(point, other_point):
                            # moving forwards
                            self.add_desired_connection(other_cluster, tag=tag, reversing=False, dont_instantiate_connection=dont_instantiate_connection)
                        else:
                            # moving backwards
                            other_cluster.add_desired_connection(self, tag=tag, reversing=True, dont_instantiate_connection=dont_instantiate_connection)

        other_index = point['previous']
        if other_index >= 0:
            other_point = self.cluster_manager.points[other_index]
            if other_point['cluster_id'] >= 0:
                tag = other_point['edge_tag']
                try:
                    other_cluster = self.cluster_manager.clusters[other_point['cluster_id']]
                except:
                    if not other_cluster_may_be_missing:
                        raise
                else:
                    if other_cluster is not self:
                        if not vehicle_is_reversing(other_point, point):
                            # moving forwards
                            other_cluster.add_desired_connection(self, tag=tag, reversing=False, dont_instantiate_connection=dont_instantiate_connection)
                        else:
                            # moving backwards
                            self.add_desired_connection(other_cluster, tag=tag, reversing=True, dont_instantiate_connection=dont_instantiate_connection)



    def remove_point(self, point_index):
        """Remove a point from the cluster and perform the necessary house keeping."""


        #self._assert_mean()

        assert point_index in self.points
        self.points.remove(point_index) # remove the point. This must happen BEFORE updating the position.
        self.cluster_manager.points[point_index]['cluster_id'] = -1 # de-register it from the manager

        point = self.cluster_manager.points[point_index]

        # update the position of the cluster to be the mean of the remaining points.
        # Don't change this formula unless you kow what you are doing.
        # The point must be removed from self.points BEFORE updating the postion for this formula to be correct.
        position = self.position
        if self.points:
            self.set_position(np.subtract(position, np.divide(point_difference((point['x'], point['y'], point['orientation']), position), len(self.points))))

        # keep track of the tags if the point was tagged.
        self.point_tags[point['tag']] -= 1


        # keep track of the connection information - the opposite of _process_connections_after_adding_point().
        other_index = point['next']
        if other_index >= 0:
            other_point = self.cluster_manager.points[other_index]
            if other_point['cluster_id'] >= 0:
                tag = point['edge_tag']
                other_cluster = self.cluster_manager.clusters[other_point['cluster_id']]
                if other_cluster is not self:
                    if not vehicle_is_reversing(point, other_point):
                        self.remove_desired_connection(other_cluster, tag=tag, reversing=False)
                    else:
                        other_cluster.remove_desired_connection(self, tag=tag, reversing=True)

        other_index = point['previous']
        if other_index >= 0:
            other_point = self.cluster_manager.points[other_index]
            if other_point['cluster_id'] >= 0:
                tag = other_point['edge_tag']
                other_cluster = self.cluster_manager.clusters[other_point['cluster_id']]
                if other_cluster is not self:
                    if not vehicle_is_reversing(other_point, point):
                        other_cluster.remove_desired_connection(self, tag=tag, reversing=False)
                    else:
                        self.remove_desired_connection(other_cluster, tag=tag, reversing=True)

        #self._assert_mean()




    def add_desired_connection(self, other, tag, reversing, dont_instantiate_connection=False):
        """
        Add a desired connection and perform the necessary house keeping.
        This method is usually called automatically by _process_connections_after_adding_point().

        However, if a gps point gets a follow up point after it was added to the
        system, then it must be called by the cluster manager.

        Note: connections are kept track of as a multiset. Thus, this method should be
        called each time a connection is demanded, irrespective of whether it exists
        already for other reasons.
        """

        assert self is not other
        if reversing:
            self.desired_connections[other].add_reverse() # increment the counter
        else:
            self.desired_connections[other].add_forward() # increment the counter

        if not dont_instantiate_connection:

            # instantiate the connectio if it doesn't already exist.
            if other not in self.connections:
                self.connections[other] = ClusterConnection(self.cluster_manager, source=self, target=other)

            # keep track of the tags.
            self.connections[other].add_tag(tag)


    def remove_desired_connection(self, other, tag, reversing):
        """
        Remove a desired connection and do the necessary house keeping.

        This is usually called automatically when removing a point results
        in a connection no longer being required.

        Note: connections are kept track of as a multiset. Thus, this method should be
        called each time a connection is no longer demanded, irrespective of whether
        it should still exists for other reasons.
        """

        assert self is not other
        assert other in self.connections

        if reversing:
            self.desired_connections[other].remove_reverse() # decrease the counter
        else:
            self.desired_connections[other].remove_forward() # decrease the counter

        # assert self.desired_connections[other].count >= 0, "A connection was removed more times than it was added. There is a nasty bug somewhere."

        # keep track of the tags
        self.connections[other].remove_tag(tag)

        # destroy the connection object if it isn't needed any more.
        if self.desired_connections[other].count == 0 and other in self.connections:
            self.connections[other].kill()
            del self.connections[other]


    def _get_connection_weight(self, first_point_index, second_point_index, point_prune_data):

        first_point = self.cluster_manager.points[first_point_index]
        second_point = self.cluster_manager.points[second_point_index]

        assert first_point['cluster_id'] >= 0, "The first point isn't assigned to a cluster. Only assigned points may be processed by this method."
        assert second_point['cluster_id'] >= 0, "The second point isn't assigned to a cluster. Only assigned points may be processed by this method."

        reversing = vehicle_is_reversing(first_point, second_point)

        if reversing:
            first_point, second_point = second_point, first_point

        first_cluster = self.cluster_manager.clusters[first_point['cluster_id']]
        second_cluster = self.cluster_manager.clusters[second_point['cluster_id']]

        assert first_cluster is not second_cluster, "This method must only be called for points in different clusters."

        connection = first_cluster.connections[second_cluster]

        result = INF

        for source_cluster, target_cluster in zip(connection.cluster_sequence, connection.cluster_sequence[1:]):
            local_weight = 0
            for connection in source_cluster.passing_connections:
                if (source_cluster, target_cluster) in connection.cluster_pairs:
                    local_weight += connection.weight
                    local_weight += point_prune_data.connection_weight_adjustment.get((connection.source.id, connection.target.id), 0)

            assert local_weight >= 0, "The weight is smaller than zero. There is a bug somewhere. Most likely something with the connection_weight_adjustment is buggy."

            if local_weight < result:
                result = local_weight

        return result



    def flag_points_for_pruning(self, point_prune_data):

        now = self.cluster_manager.latest_timestamp

        time_cut_off = now - self.cluster_manager.settings.minimum_prune_age

        prune_age_factor = self.cluster_manager.settings.assume_truck_every_when_pruning

        closed = set()

        for pivot_point_index in sorted((p for p in self.points if self.cluster_manager.points[p]['timestamp'] < time_cut_off), key=lambda x:self.cluster_manager.points[x]['timestamp']):
            if pivot_point_index in closed or pivot_point_index not in self.points:
                # the point has been processed or pruned already
                continue

            closed.add(pivot_point_index)

            # build path
            path = [pivot_point_index]

            previous_connection_weight = None
            points_forming_the_previous_connection = None
            while True:
                current_point_index = path[0]
                next_point_index = self.cluster_manager.points[current_point_index]['previous']

                if next_point_index < 0:
                    break

                elif self.cluster_manager.points[next_point_index]['cluster_id'] < 0:
                    break

                elif self.cluster_manager.points[next_point_index]['cluster_id'] != self.id:
                    previous_connection_weight = self._get_connection_weight(next_point_index, current_point_index, point_prune_data=point_prune_data)
                    points_forming_the_previous_connection = (next_point_index, current_point_index)
                    break

                else:
                    closed.add(next_point_index)
                    path.insert(0, next_point_index)

            next_connection_weight = None
            points_forming_the_next_connection = None
            while True:
                current_point_index = path[-1]
                next_point_index = self.cluster_manager.points[current_point_index]['next']

                if next_point_index < 0:
                    break

                elif self.cluster_manager.points[next_point_index]['cluster_id'] < 0:
                    break

                elif self.cluster_manager.points[next_point_index]['cluster_id'] != self.id:
                    next_connection_weight = self._get_connection_weight(current_point_index, next_point_index, point_prune_data=point_prune_data)
                    points_forming_the_next_connection = (current_point_index, next_point_index)
                    break

                else:
                    closed.add(next_point_index)
                    path.append(next_point_index)


            path_age = now - max(self.cluster_manager.points[p]['timestamp'] for p in path)

            can_remove_path = True

            if path_age <= self.cluster_manager.settings.minimum_prune_age:
                can_remove_path = False

            if can_remove_path and previous_connection_weight is not None:
                if previous_connection_weight < self.cluster_manager.settings.maximum_connection_count:
                    if path_age < self.cluster_manager.settings.maximum_prune_age:
                        if path_age < self.cluster_manager.settings.minimum_prune_age + prune_age_factor * previous_connection_weight:
                            can_remove_path = False

            if can_remove_path and next_connection_weight is not None:
                if next_connection_weight < self.cluster_manager.settings.maximum_connection_count:
                    if path_age < self.cluster_manager.settings.maximum_prune_age:
                        if path_age < self.cluster_manager.settings.minimum_prune_age + prune_age_factor * next_connection_weight:
                            can_remove_path = False

            if can_remove_path:
                if points_forming_the_previous_connection is not None:
                    assert points_forming_the_previous_connection[1] == path[0], "The path and the previous connection don't match. There's a bug in this function."
                    if points_forming_the_previous_connection[0] not in point_prune_data.points_flagged_for_deletion and points_forming_the_previous_connection[1] not in point_prune_data.points_flagged_for_deletion:
                        point_prune_data.connection_weight_adjustment[points_forming_the_previous_connection] = point_prune_data.connection_weight_adjustment.get(points_forming_the_previous_connection, 0) - 1

                if points_forming_the_next_connection is not None:
                    assert points_forming_the_next_connection[0] == path[-1], "The path and the next connection don't match. There's a bug in this function."
                    if points_forming_the_next_connection[0] not in point_prune_data.points_flagged_for_deletion and points_forming_the_next_connection[1] not in point_prune_data.points_flagged_for_deletion:
                        point_prune_data.connection_weight_adjustment[points_forming_the_next_connection] = point_prune_data.connection_weight_adjustment.get(points_forming_the_next_connection, 0) - 1

                point_prune_data.points_flagged_for_deletion.update(path)
