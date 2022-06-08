# Py 2/3 compat imports
from __future__ import print_function, division, absolute_import
from future.builtins import dict, list, object, range, str, bytes, filter, map, zip, ascii, chr, hex, input, next, oct, open, pow, round, super
from future.standard_library import install_aliases; install_aliases()
# This file is compatible with Python 2 and Python 3, please keep it so

__author__ = "Konstantin Seiler, The University of Sydney"
__copyright__ = "Copyright Technological Resources Pty Ltd (a Rio Tinto Group company), 2019"

import sys
import math
import numpy as np
import collections
from .priodict import priorityDictionary


from .helpers import INF, EPSILON, center_angle, point_difference, vehicle_is_reversing, dubins_distance, point_distance, point_distance2d

if sys.version_info.major >= 3:
    from math import isfinite
else:
    from numpy import isfinite


def _angledist(a1, a2):
    return(min(abs(a1-a2),abs((a1-a2) % 360),abs((a2-a1) % 360),abs(a2-a1)))

def _bungee_interpolate_points(p1, p2, max_delta, all_points, spacial_index):

    radius = max_delta
    angle_radius = 20


    main_distance = point_distance2d(p1, p2)

    if main_distance <= max_delta:
        return [p1, p2]



    number_divisions = int(math.ceil(main_distance/max_delta))
    point_fractions = [(i/number_divisions) for i in range(number_divisions+1)]
    points = [ [ (1-i)*p1[0] + i*p2[0], (1-i)*p1[1] + i*p2[1], None ] for i in point_fractions]

    points[0][2] = p1[2]
    points[-1][2] = p2[2]


    for _ in range(15):

        forces = [None,]

        for index in range(1,len(points)-1):

            span = (points[index+1][0]-points[index-1][0], points[index+1][1]-points[index-1][1])
            length_of_span = math.sqrt(span[0]*span[0]+span[1]*span[1])

            points[index][2] = math.degrees(math.atan2(span[1], span[0]))

            force = [0,0]
            force_count = 0

            for candidate_index in spacial_index.intersection((points[index][0]-radius, points[index][1]-radius, points[index][0]+radius, points[index][1]+radius)):
                if _angledist(points[index][2], all_points[candidate_index]['orientation']) <= angle_radius:

                    delta = (all_points[candidate_index]['x'] - points[index][0], all_points[candidate_index]['y'] - points[index][1])
                    length_of_delta = math.sqrt(delta[0]*delta[0] + delta[1]*delta[1])

                    if length_of_delta <= radius:

                        l = (span[0]*delta[0] + span[1]*delta[1]) / (length_of_span*length_of_span)
                        new_force = (delta[0] - l*span[0], delta[1] - l*span[1])
                        assert span[0] * new_force[0] + span[1] * new_force[1] < 0.0001, "the calculated force is not orthogonal to the span. Something is wrong with the maths."
                        new_force_length = math.sqrt(new_force[0]*new_force[0] + new_force[1]*new_force[1])
                        if new_force_length != 0:
                            new_force = (new_force[0]/new_force_length, new_force[1]/new_force_length)

                        assert isfinite(new_force[0])
                        assert isfinite(new_force[1])

                        multiplier = (radius-length_of_delta) / radius
                        force[0] += multiplier * new_force[0]
                        force[1] += multiplier * new_force[1]
                        force_count += 1

            if force_count > 0:
                force[0] *= 2/force_count
                force[1] *= 2/force_count

            if point_distance2d(points[index-1], points[index]) < point_distance2d(points[index], points[index+1]):
                force[0] += span[0] / length_of_span
                force[1] += span[1] / length_of_span
            else:
                force[0] -= span[0] / length_of_span
                force[1] -= span[1] / length_of_span

            assert isfinite(force[0])
            assert isfinite(force[1])


            forces.append(force)


        for index in range(1,len(points)-1):
            points[index][0] += forces[index][0]
            points[index][1] += forces[index][1]


    for index in range(1,len(points)-1):
        span = (points[index+1][0]-points[index-1][0], points[index+1][1]-points[index-1][1])


    #print(points)

    return points




class ClusterConnection(object):
    """
    This class represents a connection between two clusters. The clusters are potentially
    too far away for a direct connection. This class attempts to find intermediate
    clusters to create better paths.
    """

    def __init__(self, cluster_manager, source, target, cluster_sequence=None):

        self.cluster_manager = cluster_manager
        """The cluster manager driving everything."""

        self.id = self.cluster_manager.next_connection_id
        """An integer which uniquely identifies this connection within the cluster manager."""

        self.cluster_manager.next_connection_id += 1 # increment the id counter.

        self.source = source
        """The source :class:`Cluster` object."""

        self.target = target
        """The target :class:`Cluster` object."""

        self.tags = collections.Counter()
        """If the connection is tagged (for area identification), the tags are kept here as multiset."""

        # register the connection at the manager
        assert (self.source, self.target) not in self.cluster_manager.cluster_connections_by_source_and_target
        self.cluster_manager.cluster_connections_by_source_and_target[(self.source, self.target)] = self

        # register the connection in the spacial index of the cluster manager.
        self.spacial_box = self.get_spacial_box()
        self.cluster_manager.cluster_connections[self.id] = self
        self.cluster_manager.connection_index.insert(id=self.id, coordinates=self.spacial_box)

        # find the intermediate waypoints
        self.cluster_sequence = self.find_connecting_sequence() if cluster_sequence is None else cluster_sequence
        """List of :class:`Cluster` objects representing the path of this connection. The first and
        last items of the list are always the source and target. Inbetween can be an arbitrary number
        of intermediate clusters.
        """

        self.cluster_pairs = self._get_cluster_pairs(self.cluster_sequence)
        """For convenience, we store the pairs of consecutive clusters of the cluster_sequence here."""

        # register this connection and its direct connections with the clusters.
        for source, target in self.cluster_pairs:
            source.direct_connections[target].add(self)

        for cluster in self.cluster_sequence:
            cluster.passing_connections.add(self)




    def kill(self):
        """Unregister and destroy this connection object."""

        for source, target in self.cluster_pairs:
            source.direct_connections[target].remove(self)

        for cluster in self.cluster_sequence:
            cluster.passing_connections.remove(self)

        self.cluster_manager.connection_index.delete(id=self.id, coordinates=self.spacial_box)

        del self.cluster_manager.cluster_connections[self.id]

        del self.cluster_manager.cluster_connections_by_source_and_target[(self.source, self.target)]

        if self in self.cluster_manager.cluster_connections_to_update:
            self.cluster_manager.cluster_connections_to_update.remove(self)

        self.__dict__ = {'id': self.id}



    @property
    def weight(self):
        """The number of gps points (better: gps point pairs) that result in this connection's existence."""
        return self.source.desired_connections[self.target].count


    def _get_cluster_pairs(self, cluster_sequence):

        result = set()
        for index in range(len(cluster_sequence)-1):
            result.add((cluster_sequence[index], cluster_sequence[index+1]))

        return result


    def add_tag(self, tag):
        """Add a tag to this connection. Tags are kept as multiset."""
        self.tags[tag] += 1


    def remove_tag(self, tag):
        """Remove a tag to this connection. Tags are kept as multiset."""
        self.tags[tag] -= 1
        assert self.tags[tag] >= 0
        if self.tags[tag] == 0:
            del self.tags[tag]


    def update(self):
        """Re-calculate the sequence of intermediate clusters.

        If the sequence has changed, perform all the house keeping of de-registering
        and re-registering this connection with the clusters involved.
        """

        # update where the connection is in the spacial index.
        new_spacial_box = self.get_spacial_box()
        if new_spacial_box != self.spacial_box:
            self.cluster_manager.connection_index.delete(id=self.id, coordinates=self.spacial_box)
            self.spacial_box = new_spacial_box
            self.cluster_manager.connection_index.insert(id=self.id, coordinates=self.spacial_box)

        # re-calculate the cluster sequence.
        new_cluster_sequence = self.find_connecting_sequence()

        # do the house keeping if the cluster sequence has changed.
        if new_cluster_sequence != self.cluster_sequence:

            new_cluster_pairs = self._get_cluster_pairs(new_cluster_sequence)

            # we delete first and add afterwards because it's a set, not a counter.
            for source, target in self.cluster_pairs:
                source.direct_connections[target].remove(self)
            for source, target in new_cluster_pairs:
                source.direct_connections[target].add(self)

            # remove first and add afterwards because it's a set, not a counter.
            for cluster in self.cluster_sequence:
                    cluster.passing_connections.remove(self)
            for cluster in new_cluster_sequence:
                cluster.passing_connections.add(self)

            self.cluster_sequence = new_cluster_sequence
            self.cluster_pairs = new_cluster_pairs


    def is_valid_intermediate(self, intermediate):
        """
        Crude check whether the given cluster is in principle suitable to be an intermediate
        point for this connection. This can be used to quickly decide that a cluster is
        irrelevant for this connection.
        """

        return self._is_valid_intermediate(self.source, self.target, intermediate)


    def _is_valid_intermediate(self, source, target, intermediate):
        """
        Same as is_valid_intermediate(), but instead of using the connection's source
        and target clusters as endpoints, the given clusters are considered.
        This can be used as helper when searching for a suitable path.
        """

        ANGLE_TOLERANCE = 30

        if intermediate is source:
            return False

        if intermediate is target:
            return False

        total_delta = np.subtract(target.position[:2], source.position[:2])
        total_distance = math.sqrt(total_delta[0]*total_delta[0]+total_delta[1]*total_delta[1])

        if total_distance < EPSILON:
            return False

        reference_angle = math.degrees(math.atan2(total_delta[1], total_delta[0]))

        min_angle = min(
                -ANGLE_TOLERANCE,
                center_angle(source.position[2] - reference_angle) - ANGLE_TOLERANCE,
                center_angle(target.position[2] - reference_angle) - ANGLE_TOLERANCE,
            )
        max_angle = max(
                ANGLE_TOLERANCE,
                center_angle(source.position[2] - reference_angle) + ANGLE_TOLERANCE,
                center_angle(target.position[2] - reference_angle) + ANGLE_TOLERANCE,
            )

        intermediate_angle = center_angle(intermediate.position[2] - reference_angle)
        if intermediate_angle < min_angle or intermediate_angle > max_angle:
            return False

        return True



    def get_spacial_box(self):
        """
        Get the relevant bounding box for this connection for registering in the spacial index.

        The idea is, that all clusters that are potentially relevant for being intermediate
        waypoints are within this box.
        """

        delta = np.subtract(self.target.position[:2], self.source.position[:2])
        crow_distance = math.sqrt(delta[0]*delta[0]+delta[1]*delta[1])
        mid_point = np.divide(np.add(self.source.position[:2], self.target.position[:2]), 2)

        radius = crow_distance / 2
        return (mid_point[0]-radius, mid_point[1]-radius, mid_point[0]+radius, mid_point[1]+radius)


    def find_connecting_sequence(self):
        """
        Find the sequence of intermediate waypoints.

        This method doesn't do any of the work, but calls different implementations
        depending on the given settings.
        """

        settings = self.cluster_manager.settings

        if settings.intermediate_point_method == settings.CHORD_ANGLE:
            result = self._find_connecting_sequence_chord()

        elif settings.intermediate_point_method == settings.DUBINS_METHOD:
            result = self._find_connecting_sequence_dubin()

        elif settings.intermediate_point_method == settings.GRAVITY_METHOD:
            result = self._find_connecting_sequence_gravity()

        else:
            raise NotImplementedError("An unknown method for making cluster connections was requested: %s" % (settings.intermediate_point_method,))

        return tuple(result)



    def _get_relevant_clusters(self):
        """
        Create a list of all clusters that are potentially relevant for finding intermediate waypoints.
        """


        # the relevant clusters are all clusters that lie in the circle that has source and target as antipodal points.
        delta = np.subtract(self.target.position[:2], self.source.position[:2])
        crow_distance = math.sqrt(delta[0]*delta[0]+delta[1]*delta[1])
        mid_point = np.divide(np.add(self.source.position[:2], self.target.position[:2]), 2)
        radius = crow_distance / 2

        relevant_clusters_candidates = self.cluster_manager.cluster_index.intersection(self.spacial_box, objects=False)

        result = []
        for candidate in relevant_clusters_candidates:
            candidate = self.cluster_manager.clusters[candidate]
            delta = np.subtract(candidate.position[:2], mid_point)
            if math.sqrt(delta[0]*delta[0]+delta[1]*delta[1]) <= radius:
                result.append(candidate)

        if not self.target in result:
            result.append(self.target)

        return result


    def _find_connecting_sequence_chord(self):
        """
        An implementatino of find_connecting_sequence()
        """


        relevant_clusters = self._get_relevant_clusters()


        # do a dijkstra like search using a sum-of-squares metric.

        queue = priorityDictionary()

        queue[self.source] = 0
        coming_from = {self.source:None}
        closed = set()

        path = None

        for current in queue:

            current_cost = queue[current]

            if current in closed:
                continue

            closed.add(current)


            if current is self.target:
                path = []
                while current is not None:
                    path.insert(0, current)
                    current = coming_from[current]
                break


            if coming_from[current] is not None:
                # only make this check now because it is expensive
                # ideally, we'd check this before putting it onto the queue, but for better performance, we delay it.
                # This could make us miss connections in corner cases where the shortest path is invalid but a longer path is valid
                # but was already replaced by the shorter one.
                if not self._is_valid_intermediate(coming_from[current], self.target, current):
                    continue


            for neighbour in relevant_clusters:
                if neighbour.points and neighbour not in closed:
                    delta = np.subtract(neighbour.position[:2], current.position[:2])
                    distance = math.sqrt(delta[0]*delta[0]+delta[1]*delta[1])
                    neighbour_cost = current_cost + (distance*distance)

                    if neighbour not in queue or neighbour_cost < queue[neighbour]:

                        #if neighbour is target or self._is_valid_intermediate(current, target, neighbour): # make this check late because it is expensive.
                        queue[neighbour] = neighbour_cost
                        coming_from[neighbour] = current

        assert path is not None, "No path from source to target was found. This isn't possible since there is always the direct connection as a solution. There must be a bug."
        return path


    def _dubins_distance(self, source, waypoint):
        """Helper which calculates the distance between two points by considering
        the distance a Dubins car would have to travel to get from A to B."""
        return dubins_distance(source, waypoint, self.cluster_manager.settings.dubins_turning_radius)


    def _find_connecting_sequence_dubin(self):
        """
        An implementatino of find_connecting_sequence()
        """

        relevant_clusters = self._get_relevant_clusters()


        # do a dijkstra like search using a sum-of-squares metric.

        queue = priorityDictionary()

        queue[self.source] = 0
        coming_from = {self.source:None}
        closed = set()

        path = None

        for current in queue:

            current_cost = queue[current]

            if current in closed:
                continue

            closed.add(current)


            if current is self.target:
                path = []
                while current is not None:
                    path.insert(0, current)
                    current = coming_from[current]
                break


            for neighbour in relevant_clusters:
                if neighbour.points and neighbour not in closed:

                    if neighbour in queue:
                        delta = np.subtract(neighbour.position[:2], current.position[:2])
                        distance = math.sqrt(delta[0]*delta[0]+delta[1]*delta[1])
                        neighbour_cost_lower_bound = current_cost + (distance*distance)

                        if neighbour_cost_lower_bound >= queue[neighbour]:
                            continue

                    dubins_distance = self._dubins_distance(current.position[:3], neighbour.position[:3])
                    neighbour_cost = current_cost + (dubins_distance*dubins_distance)

                    if neighbour not in queue or neighbour_cost < queue[neighbour]:
                        queue[neighbour] = neighbour_cost
                        coming_from[neighbour] = current

        assert path is not None, "No path from source to target was found. This isn't possible since there is always the direct connection as a solution. There must be a bug."
        return path


    def _find_connecting_sequence_gravity(self):
        """
        An implementatino of find_connecting_sequence()
        """

        cluster_radius = self.cluster_manager.settings.cluster_radius

        sample_distance = cluster_radius / 2

        better_path = _bungee_interpolate_points(
                self.source.position,
                self.target.position,
                max_delta=sample_distance,
                all_points=self.cluster_manager.points,
                spacial_index=self.cluster_manager.point_index,
            )

        path = []

        for point in better_path:

            candidates = self.cluster_manager.cluster_index.intersection(
                    (point[0]-cluster_radius, point[1]-cluster_radius, point[0]+cluster_radius, point[1]+cluster_radius),
                    objects=False,
                )
            best_candidate = None
            best_distance = INF
            for candidate in candidates:
                candidate = self.cluster_manager.clusters[candidate]
                distance = point_distance(candidate.position, point, angular_weight=self.cluster_manager.angular_weight)
                if distance < best_distance:
                    best_distance = distance
                    best_candidate = candidate


            if best_distance <= cluster_radius:
                if not best_candidate in path:
                    path.append(best_candidate)

                    if best_candidate is self.target:
                        break


        assert path, "No path from source to target was found. This isn't possible since there is always the direct connection as a solution. There must be a bug."
        assert path[0] is self.source, "The source of the path doesn't match the source of the connection. This should not happen because the first point of the path should match the position exactly (so it should be the closest)"
        assert path[-1] is self.target, "The target of the path doesn't match the target of the connection. This should not happen because the last point of the path should match the position exactly (so it should be the closest)"

        return path
