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
import weakref
import collections

import matplotlib
import matplotlib.pyplot as plt
import dubins
import shapely
import shapely.geometry
import shapely.ops
import descartes
import datetime
import itertools
import random

from .helpers import INF, EPSILON, EPOCH, point_distance, point_distance2d, vehicle_is_reversing

try:
    from math import isfinite
except ImportError:
    from numpy import isfinite



class ClusterManagerPainterThreadsafe(object):

    def __init__(self, queue_driver, axis=None):

        if axis is None:
            axis = plt.figure().add_subplot(111)

        self.queue_driver = queue_driver
        self.axis = axis

        with self.queue_driver.cluster_manager_lock as cluster_manager:
            self.settings = cluster_manager.settings
        self.angular_weight = self.settings.cluster_radius / self.settings.angular_threshold

        self.lines = dict()
        self.cluster_markers = dict()
        self.cluster_patches_to_clusters = weakref.WeakKeyDictionary()
        self.connection_lines_to_clusters = weakref.WeakKeyDictionary()

        self.digger_markers = dict()
        self.bucket_markers = []
        self.dump_markers = dict()

        self.x_lim = [INF, -INF]
        self.y_lim = [INF, -INF]
        self.fixed_limits = False

        self._axis_tracking_data = None

        self.area_markers = []
        self.raw_area_markers = []
        self.intersection_markers = []

        self.update()

        self._last_picked_cluster = None
        self._pick_drawings = []
        self.axis.figure.canvas.mpl_connect('pick_event', self._on_pick_callback)

        self.axis.figure.show()
        #plt.pause(0.0001)


    # we don't really pickle any of the internal states. We simply pickle
    # the queue_driver manager and then construct the painter from scratch upon
    # unpickling.
    def __getstate__(self):
        return {'queue_driver': self.queue_driver}

    def __setstate__(self, state):
        self.__init__(queue_driver=state['queue_driver'], axis=None)


    def _on_pick_callback(self, event):
        # redirect this method call so it works with ipython's way of modifying running code.
        return self._on_pick_callback_real(event)

    def _on_pick_callback_real(self, event):

        artist = event.artist

        while self._pick_drawings:
            to_remove = self._pick_drawings.pop()
            try:
                to_remove.remove()
            except:
                pass

        with self.queue_driver.cluster_manager_lock as cluster_manager:

            try:
                cluster = self.cluster_patches_to_clusters[artist]
            except KeyError:
                pass
            else:

                if self._last_picked_cluster is None:
                    distance = None
                else:
                    distance = point_distance(self._last_picked_cluster.position, cluster.position, angular_weight=self.angular_weight)
                print("Cluster %s at %s in zone %s (distance to last: %s)" % (cluster.id, cluster.position, cluster.zone, distance))
                self._last_picked_cluster = cluster


                for point_index in sorted(cluster.points, key=lambda x:cluster_manager.points[x]['timestamp']):
                    point = cluster_manager.points[point_index]
                    if point['tag'] >=0:
                        tag = cluster_manager.tag_names[point['tag']]
                    else:
                        tag = ""

                    point_timestamp = (EPOCH + datetime.timedelta(seconds=int(point['timestamp']))).strftime("%Y-%m-%d %H:%M:%S")

                    print("%i: %s %s %s" % (point_index, point, tag, point_timestamp))
                    patch = matplotlib.patches.Polygon(
                            self._get_position_marker_points((point['x'], point['y'], point['orientation']), size=2),
                            color='g',
                            facecolor='g',
                        )
                    self.axis.add_patch(patch)
                    self._pick_drawings.append(patch)



            try:
                source_cluster, target_cluster = self.connection_lines_to_clusters[artist]
            except KeyError:
                pass
            else:

                print("==========================================================")
                plotted_pairs = []
                for connection in source_cluster.passing_connections:
                    if (source_cluster, target_cluster) in connection.cluster_pairs:
                        print(connection.tags)
                        for source_point_index in connection.source.points:
                            source_point = cluster_manager.points[source_point_index]
                            target_point_index = source_point['next']
                            if target_point_index >= 0:
                                target_point = cluster_manager.points[target_point_index]
                                if target_point['cluster_id'] >= 0 and cluster_manager.clusters[target_point['cluster_id']] is connection.target:
                                    plotted_pairs.append((source_point_index, target_point_index))
                                    source_point_timestamp = (EPOCH + datetime.timedelta(seconds=int(source_point['timestamp']))).strftime("%Y-%m-%d %H:%M:%S")
                                    target_point_timestamp = (EPOCH + datetime.timedelta(seconds=int(target_point['timestamp']))).strftime("%Y-%m-%d %H:%M:%S")
                                    print("%s %s ==> %s %s" % (source_point, source_point_timestamp, target_point, target_point_timestamp))
                                    path, _ = dubins.shortest_path(
                                            (source_point['x'], source_point['y'], math.radians(source_point['orientation'])),
                                            (target_point['x'],target_point['y'],math.radians(target_point['orientation'])),
                                            cluster_manager.settings.dubins_turning_radius,
                                        ).sample_many(0.5)

                                    self._pick_drawings.extend(self.axis.plot([p[0] for p in path], [p[1] for p in path], color=(0,0,1,0.3)))

                if len(plotted_pairs) == 1:
                    for path in plotted_pairs:
                        path = list(path)
                        for _ in range(5):
                            current = cluster_manager.points[path[0]]
                            if current['previous'] >= 0:
                                path.insert(0, current['previous'])
                            else:
                                break
                        for _ in range(5):
                            current = cluster_manager.points[path[-1]]
                            if current['next'] >= 0:
                                path.append(current['next'])
                            else:
                                break


                        for point_index in path:
                            point = cluster_manager.points[point_index]
                            print("Path point:", point_index, point)
                            patch = matplotlib.patches.Polygon(
                                    self._get_position_marker_points((point['x'], point['y'], point['orientation']), size=2),
                                    color='g',
                                    facecolor='g',
                                )
                            self.axis.add_patch(patch)
                            self._pick_drawings.append(patch)

                        self._pick_drawings.extend(self.axis.plot([cluster_manager.points[p]['x'] for p in path], [cluster_manager.points[p]['y'] for p in path], color=(0,1,0,0.3)))



        self.flush_plots()

    def re_init(self):
        self.axis.cla()
        self.__init__(queue_driver=self.queue_driver, axis=self.axis)


    def set_window_title(self, title):
        self.axis.figure.canvas.set_window_title(title)


    def track_other_painter(self, other=None):

        if not hasattr(self, '_axis_tracking_data'):
            self._axis_tracking_data = None

        if self._axis_tracking_data is not None:
            axis, cid_x, cid_y = self._axis_tracking_data
            axis.callbacks.disconnect(cid_x)
            axis.callbacks.disconnect(cid_y)
            self._axis_tracking_data = None

        def transfer_limits(axis):
            something_changed = False

            xlim = axis.get_xlim()
            if xlim != self.axis.get_xlim():
                self.axis.set_xlim(xlim)
                something_changed = True

            ylim = axis.get_ylim()
            if ylim != self.axis.get_ylim():
                self.axis.set_ylim(ylim)
                something_changed = True

            if something_changed:
                self.flush_plots()

        if other is not None:
            axis = other.axis
            cid_x = axis.callbacks.connect('xlim_changed', transfer_limits)
            cid_y = axis.callbacks.connect('ylim_changed', transfer_limits)

            self._axis_tracking_data = axis, cid_x, cid_y




    def get_connecting_sequence_coordinates(self, source, target):

        result = ([],[])

        for cluster in [source, target]:
            result[0].append(cluster.position[0])
            result[1].append(cluster.position[1])

        return result


    def _get_cluster_marker_points(self, cluster):
        if cluster.zone < 0:
            size = 5
        else:
            size = 3
        size = 5 # only temporary to make paper figures
        return self._get_position_marker_points(cluster.position, size=size)


    def _get_digger_marker_points(self, position, size=50):

        #angle = math.radians(position[2])
        angle = 0
        transform = np.array([
                [math.cos(angle), -math.sin(angle), position[0]],
                [math.sin(angle), math.cos(angle), position[1]],
                [0,0,1]
            ])

        result = [
                tuple(np.matmul(transform, [1*size, 0, 1]))[:2],
                tuple(np.matmul(transform, [0.25*size, 0.25*size, 1]))[:2],
                tuple(np.matmul(transform, [0, 1*size, 1]))[:2],
                tuple(np.matmul(transform, [-0.25*size, 0.25*size, 1]))[:2],
                tuple(np.matmul(transform, [-1*size, 0, 1]))[:2],
                tuple(np.matmul(transform, [-0.25*size, -0.25*size, 1]))[:2],
                tuple(np.matmul(transform, [0, -1*size, 1]))[:2],
                tuple(np.matmul(transform, [0.25*size, -0.25*size, 1]))[:2],
                tuple(np.matmul(transform, [1*size, 0, 1]))[:2],
            ]

        return result

    def _get_dump_marker_points(self, position):
        return self._get_digger_marker_points(position)

    def _get_bucket_marker_points(self, position):
        return self._get_digger_marker_points(position, size=2)


    def _get_position_marker_points(self, position, size):

        angle = math.radians(position[2])
        transform = np.array([
                [math.cos(angle), -math.sin(angle), position[0]],
                [math.sin(angle), math.cos(angle), position[1]],
                [0,0,1]
            ])

        result = [
                tuple(np.matmul(transform, [0,0,1]))[:2],
                tuple(np.matmul(transform, [-0.5*size,-0.5*size,1]))[:2],
                tuple(np.matmul(transform, [1*size,0,1]))[:2],
                tuple(np.matmul(transform, [-0.5*size,0.5*size,1]))[:2],
                tuple(np.matmul(transform, [0,0,1]))[:2],
            ]

        return result


    def remove_equipment_markers(self):
        for digger in list(self.digger_markers):
            self.digger_markers[digger][1].remove()
            self.digger_markers[digger][2].remove()
            del self.digger_markers[digger]

        for dump in list(self.dump_markers):
            for artist in self.dump_markers[dump][1]:
                artist.remove()
            del self.dump_markers[dump]

        stack = list(self.bucket_markers)
        while stack:
            item = stack.pop()
            if isinstance(item, list):
                stack.extend(item)
            else:
                item.remove()
        del self.bucket_markers[:]



    def update_digger_positions(self):

        colour = (0.99,0,0)

        with self.queue_driver.cluster_manager_lock as cluster_manager:

            for digger in list(self.digger_markers):
                if True or digger not in cluster_manager.digger_positions:
                    self.digger_markers[digger][1].remove()
                    self.digger_markers[digger][2].remove()
                    del self.digger_markers[digger]

            for digger, (position, _) in cluster_manager.digger_positions.items():

                position = tuple(position[:3])

                if digger in self.digger_markers:
                    if self.digger_markers[digger][0] == position:
                        continue

                    else:
                        points = self._get_digger_marker_points(position)
                        self.digger_markers[digger][1].set_xy(points)
                        self.digger_markers[digger][2].set_position(position[:2])
                        self.digger_markers[digger][0] = position

                else:
                    points = self._get_digger_marker_points(position)
                    patch = matplotlib.patches.Polygon(points, edgecolor=colour, facecolor=colour, picker=False)
                    self.axis.add_patch(patch)
                    #patch = self.axis.plot([position[0]], [position[1]], '*', color=colour, picker=False)

                    text_artist = self.axis.text(position[0], position[1], digger, horizontalalignment='center', verticalalignment='center', clip_on=True)

                    self.digger_markers[digger] = [position, patch, text_artist]


    def update_bucket_markers(self):

        with self.queue_driver.cluster_manager_lock as cluster_manager:

            for bucket_position, digger_position in cluster_manager.buckets[len(self.bucket_markers):]:

                points = self._get_bucket_marker_points(bucket_position)
                artist = matplotlib.patches.Polygon(points, edgecolor='green', facecolor='green', picker=False)

                artists = [artist]


                if digger_position is not None:
                    artist = self.axis.plot([bucket_position[0], digger_position[0]], [bucket_position[1], digger_position[1]], color='green')
                    artists.append(artist)

                self.bucket_markers.append(artists)


    def update_dump_positions(self):


        with self.queue_driver.cluster_manager_lock as cluster_manager:

            for dump in list(self.dump_markers):
                if dump not in cluster_manager.dump_positions:
                    for artist in self.dump_markers[dump][1]:
                        artist.remove()
                    del self.dump_markers[dump]

            for dump, (position, _) in cluster_manager.dump_positions.items():

                position = tuple(position[:3])

                if dump in self.dump_markers:
                    if self.dump_markers[dump][0] == position:
                        continue

                    else:
                        points = self._get_dump_marker_points(position)
                        self.dump_markers[dump][1][0].set_xy(points)
                        if len(self.dump_markers[dump][1]) > 1:
                            self.dump_markers[dump][1][1].set_position(position[:2])
                        self.dump_markers[dump][0] = position

                else:
                    points = self._get_dump_marker_points(position)
                    patch = matplotlib.patches.Polygon(points, edgecolor='blue', facecolor='blue', picker=False)
                    self.axis.add_patch(patch)
                    #patch = self.axis.plot([position[0]], [position[1]], '*', color='blue', picker=False)

                    patches = [patch]
                    text_artist = self.axis.text(position[0], position[1], dump, horizontalalignment='center', verticalalignment='center', fontsize='x-small', clip_on=True)
                    patches.append(text_artist)
                    self.dump_markers[dump] = [position, patches]



    def remove_cluster_markers(self):
        for item in self.cluster_markers.values():
            item[1].remove()
        self.cluster_markers.clear()

    def update_cluster_markers(self):


        existent_clusters = set()

        with self.queue_driver.cluster_manager_lock as cluster_manager:

            for cluster in cluster_manager.clusters.values():

                existent_clusters.add(cluster)

                if cluster.zone < 0:
                    colour = (1,0,0)
                else:
                    colour = (0,1,0)

                if cluster in self.cluster_markers:

                    if cluster.position != self.cluster_markers[cluster][0] or colour != self.cluster_markers[cluster][2]:
                        points = self._get_cluster_marker_points(cluster)
                        self.cluster_markers[cluster][1].set_xy(points)
                        self.cluster_markers[cluster][0] = cluster.position
                        self.cluster_markers[cluster][1].set_color(colour)
                        self.cluster_markers[cluster][2] = colour

                else:
                    points = self._get_cluster_marker_points(cluster)
                    patch = matplotlib.patches.Polygon(points, edgecolor=colour, facecolor=colour, picker=True)
                    self.cluster_patches_to_clusters[patch] = cluster
                    self.axis.add_patch(patch)
                    self.cluster_markers[cluster] = [cluster.position, patch, colour]

        for cluster in list(self.cluster_markers):
            if cluster not in existent_clusters:
                self.cluster_markers[cluster][1].remove()
                del self.cluster_markers[cluster]

    def _get_age_colour_mapper(self):
        mpl_cmap = plt.get_cmap('jet').reversed()
        result = lambda a:mpl_cmap(float(max(0,min(1,a / (30*86400)))))
        return result

    def show_colour_map(self):
        axis = plt.figure().add_subplot(111)
        colour_mapper = self._get_age_colour_mapper()

        low = 0
        high = 30*86400
        delta = 3600/3
        scale = 1/86400

        current = low
        while current < high:
            current_high = min(current+delta, high)
            colour = colour_mapper(current)

            patch = matplotlib.patches.Rectangle((current*scale, 0), (current_high-current)*scale, 1, color=colour)
            axis.add_patch(patch)

            current = current_high

        axis.set_xlim([low*scale,high*scale])
        axis.set_ylim([0,1])
        axis.set_yticks([])



    def update_connections(self, useful_connections_in_zones=dict(), cluster_position_overrides=dict()):

        print('update_connections')


        lines = dict()
        cluster_pairs_for_lines = dict()

        with self.queue_driver.cluster_manager_lock as cluster_manager:

            connection_stats = cluster_manager.get_cluster_connection_statistics()

            #plt.figure().add_subplot(111).hist([(cluster_manager.latest_timestamp-i[2])/86400 for i in connection_stats.values()], bins=500)

            colour_mapper = self._get_age_colour_mapper()


            connections_to_draw = set()

            for source in cluster_manager.clusters.values():
                #for target in [n for n,v in source.direct_connections.items() if v]:
                for target in source.neighbours:
                    if source.zone < 0 or target.zone < 0:
                        connections_to_draw.add((source, target))

            connections_to_draw.update(useful_connections_in_zones)

            suppressed_connections = set()

            for source in cluster_manager.clusters.values():
                for target in [n for n,v in source.direct_connections.items() if v]:
                    if source.zone < 0 or target.zone < 0 or True:
                        if (source, target) not in connections_to_draw:
                            suppressed_connections.add((source, target))
                            connections_to_draw.add((source, target))


            for source, target in connections_to_draw:

                tags = collections.Counter()
                for connection in source.passing_connections:
                    if (source, target) in connection.cluster_pairs:
                        tags += connection.tags

                non_none_count = sum(tags.values()) - tags[-1]
                if non_none_count == 0:
                    annotation_text = ''
                else:
                    annotation_text = '%.0f' % (100*non_none_count / (non_none_count + tags[-1]))
                    for tag, count in tags.items():
                        annotation_text += '|%s:%i' % (cluster_manager.tag_names[tag], count)

                annotation_text = ''


                #if isinstance(useful_connections_in_zones, dict):
                #    if (source, target) in useful_connections_in_zones:
                #       annotation_text = ''.join(useful_connections_in_zones[(source, target)])


                if (source,target) not in connection_stats:
                    colour = 'black'
                else:
                    age = cluster_manager.latest_timestamp - connection_stats[(source,target)][2]
                    colour = colour_mapper(age)

                colour = (1,0,0) # temporary for paper figures only


                if (source, target) in suppressed_connections:
                    colour = (0.1,0,0,0.1)


                if source in cluster_position_overrides:
                    source_position = tuple(cluster_position_overrides[source][:2])
                else:
                    source_position = tuple(source.position[:2])

                if target in cluster_position_overrides:
                    target_position = tuple(cluster_position_overrides[target][:2])
                else:
                    target_position = tuple(target.position[:2])

                key = (source_position, target_position, colour, annotation_text)

                lines[key] = True
                cluster_pairs_for_lines[key] = (source, target)


            for key in list(self.lines):
                if key not in lines:
                    for artist in self.lines.pop(key):
                        if artist is not None:
                            artist.remove()


            for key in lines:
                if key not in self.lines:

                    self.lines[key] = []

                    X = (key[0][0], key[1][0])
                    Y = (key[0][1], key[1][1])
                    colour = key[2]
                    annotation_text = key[3]

                    line = self.axis.plot(X, Y, color=colour, picker=True)
                    assert len(line) == 1
                    line = line[0]

                    self.lines[key].append(line)

                    if annotation_text:
                        text_artist = self.axis.text(0.5*(X[0]+X[-1]), 0.5*(Y[0]+Y[-1]), annotation_text, horizontalalignment='center', verticalalignment='center', fontsize='x-small', clip_on=True)
                        self.lines[key].append(text_artist)


            self.connection_lines_to_clusters.clear()
            for key, artists in self.lines.items():
                if key in cluster_pairs_for_lines:
                    self.connection_lines_to_clusters[artists[0]] = cluster_pairs_for_lines[key]

        print('connections updated')



    def colour_zones(self, colour):
         for item in self.area_markers:
             if isinstance(item, matplotlib.patches.PathPatch):
                 item.set_color(colour)

    def remove_zones(self):
        for item in self.area_markers:
            item.remove()
        del self.area_markers[:]


    def remove_raw_zones(self):
        for item in self.raw_area_markers:
            item.remove()
        del self.raw_area_markers[:]



    def plot_zones(self):

        for item in self.area_markers:
            item.remove()
        del self.area_markers[:]


        geometries_by_zone = collections.defaultdict(list)

        with self.queue_driver.cluster_manager_lock as cluster_manager:

            for cluster in cluster_manager.clusters.values():

                if cluster.zone >= 0:

                    geometries_by_zone[cluster_manager.tag_names[cluster.zone]].append(shapely.geometry.Point(cluster.position[:2]))

                    for target, inducing_connections in cluster.direct_connections.items():


                        if inducing_connections and target.zone >= 0 and target.zone == cluster.zone:

                            line = shapely.geometry.LineString([cluster.position[:2], target.position[:2]])
                            geometries_by_zone[cluster_manager.tag_names[cluster.zone]].append(line)

        for name, geometries in geometries_by_zone.items():

            # we first buffer everything a little to ensure that we're dealing with polygons.
            # this is to work around an odd bug in shapely/libgeos which makes it crash with
            # runaway memory consumption during the buffering of full geometries in rare
            # circumstances.
            geometries = [g.buffer(1, resolution=1) for g in geometries]
            geometry = shapely.ops.unary_union(geometries)
            geometry = geometry.buffer(19)

            if isinstance(geometry, shapely.geometry.Polygon):
                geometry = shapely.geometry.Polygon(geometry.exterior.coords)

            elif isinstance(geometry, shapely.geometry.MultiPolygon):
                geometry = shapely.geometry.MultiPolygon([shapely.geometry.Polygon(p.exterior.coords) for p in geometry.geoms])

            else:
                raise NotImplementedError("The geometry object for an area is of unexpected type %s." % (type(geometry),))

            geometry = geometry.buffer(-10)


            if not geometry.is_empty:
                try:
                    colour = tuple(i/256 for i in (hash(str(name)) % 256**3).to_bytes(3,'big')) + (0.5,)
                except AttributeError:
                    def bytes_to_float(input_bytes):
                        isinstance(input_bytes, bytes) or exit(99)
                        if (len(input_bytes) == 0):
                            return 0.
                        (input_bytes[0] < 0x80) or exit(98)

                        shift = i1 = 0
                        for p in range(1, len(input_bytes)+1):
                            i1 += (input_bytes[-p] << shift)
                            shift += 8

                        return float(i1)
                    colour = tuple(bytes_to_float(bytes(i))/256 for i in np.flip(np.array(hash(name) % 256**3).tobytes())[::-1]) + (0.5,)

                colour = (0,0,0.8,0.3)

                patch = descartes.PolygonPatch(geometry, color=colour)
                self.axis.add_patch(patch)
                self.area_markers.append(patch)

                if isinstance(geometry, shapely.geometry.MultiPolygon):
                    for poly1, poly2 in itertools.combinations(geometry.geoms, 2):
                        point1, point2 = shapely.ops.nearest_points(poly1, poly2)
                        artists = self.axis.plot([point1.x, point2.x], [point1.y, point2.y], color=colour)
                        self.area_markers.extend(artists)


                text_position = geometry.centroid
                text_artist = self.axis.text(text_position.x, text_position.y, name, horizontalalignment='center', verticalalignment='center', clip_on=True)
                self.area_markers.append(text_artist)


    def plot_raw_zones(self):
        print('raw')

        for item in self.raw_area_markers:
            item.remove()
        del self.raw_area_markers[:]


        geometries_by_zone = collections.defaultdict(list)

        with self.queue_driver.cluster_manager_lock as cluster_manager:

            for cluster in cluster_manager.clusters.values():

                if cluster.raw_zone >= 0:

                    geometries_by_zone[cluster.raw_zone].append(shapely.geometry.Point(cluster.position[:2]))

                    for target, inducing_connections in cluster.direct_connections.items():

                        if inducing_connections and target.raw_zone >= 0 and target.raw_zone == cluster.raw_zone:

                            line = shapely.geometry.LineString([cluster.position[:2], target.position[:2]])
                            geometries_by_zone[cluster.raw_zone].append(line)

        for name, geometries in geometries_by_zone.items():

            # we first buffer everything a little to ensure that we're dealing with polygons.
            # this is to work around an odd bug in shapely/libgeos which makes it crash with
            # runaway memory consumption during the buffering of full geometries in rare
            # circumstances.
            geometries = [g.buffer(1, resolution=1) for g in geometries]
            geometry = shapely.ops.unary_union(geometries)
            geometry = geometry.buffer(19)

            if isinstance(geometry, shapely.geometry.Polygon):
                geometry = shapely.geometry.Polygon(geometry.exterior.coords)

            elif isinstance(geometry, shapely.geometry.MultiPolygon):
                geometry = shapely.geometry.MultiPolygon([shapely.geometry.Polygon(p.exterior.coords) for p in geometry.geoms])

            else:
                raise NotImplementedError("The geometry object for an area is of unexpected type %s." % (type(geometry),))

            geometry = geometry.buffer(-10)


            if not geometry.is_empty:

                colour = (0.2,0,0,0.2)
                patch = descartes.PolygonPatch(geometry, color=colour)
                self.axis.add_patch(patch)
                self.raw_area_markers.append(patch)

                #text_position = geometry.centroid
                #text_artist = self.axis.text(text_position.x, text_position.y, name, horizontalalignment='center', verticalalignment='center', clip_on=True)
                #self.raw_area_markers.append(text_artist)


    def colour_intersections(self, colour):
         for item in self.intersection_markers:
             if isinstance(item, matplotlib.patches.PathPatch):
                 item.set_color(colour)

    def remove_intersections(self):
        for item in self.intersection_markers:
            item.remove()
        del self.intersection_markers[:]



    def plot_intersections(self):

        self.remove_intersections()


        with self.queue_driver.cluster_manager_lock as cluster_manager:

            for _, polygons in cluster_manager.find_intersections():

                for polygon in polygons:

                    geometry = shapely.geometry.Polygon(polygon)
                    patch = descartes.PolygonPatch(geometry, facecolor=(0,0.99,0,0.5), edgecolor=(0,0.99,0))
                    self.axis.add_patch(patch)
                    self.intersection_markers.append(patch)

#            for source, target in cluster_manager._last_cluster_pairs_for_debug:
#                artists = self.axis.plot([source.position[0], target.position[0]], [source.position[1], target.position[1]], ':', color='red')
#                self.intersection_markers.extend(artists)


    def remove_edges(self, max_count=None):
        count = 0
        for key in list(self.lines):
            for artist in self.lines[key]:
                if artist is not None:
                    artist.remove()
            del self.lines[key]

            count += 1
            if max_count is not None and count >= max_count:
                break

    def colour_edges(self, colour):
        for key in list(self.lines):
            self.lines[key][2].set_color(colour)

    def plot_cluster_assignments(self):

        if 'cluster_assignment_plots' not in self.__dict__:
            self.cluster_assignment_plots = []

        print('removing old plots')
        for artist in self.cluster_assignment_plots:
            artist.remove()
        self.cluster_assignment_plots.clear()

        print('creating new plot')
        with self.queue_driver.cluster_manager_lock as cluster_manager:

            pivot_position = None
            while pivot_position is None:
                point_index, point = random.choice(list(enumerate(cluster_manager.points)))
                if point_index not in cluster_manager.free_points:
                    pivot_position = (point['x'], point['y'])

            seen_positions = set()

            for point_index, point in enumerate(cluster_manager.points):
                if point_index in cluster_manager.free_points:
                    continue

                position = (point['x'], point['y'], point['orientation'])

                if math.sqrt( (position[0]-pivot_position[0])**2 + (position[1]-pivot_position[1])**2 ) > 500:
                    continue

                if position not in seen_positions:
                    patch = matplotlib.patches.Polygon(
                        self._get_position_marker_points(position, size=2),
                        color='g',
                        facecolor='g',
                    )
                    self.cluster_assignment_plots.append(patch)
                    self.axis.add_patch(patch)
                    seen_positions.add(position)

                if point['cluster_id'] >= 0:
                    cluster = cluster_manager.clusters[point['cluster_id']]

                    patches = self.axis.plot([position[0], cluster.position[0]], [position[1], cluster.position[1]], color=(0.5, 0.5, 0.5))
                    self.cluster_assignment_plots.extend(patches)



    def update(self):

        print("update plot")

        with self.queue_driver.cluster_manager_lock as cluster_manager:
            latest_timestamp = cluster_manager.latest_timestamp

        if isfinite(latest_timestamp):
            date = (EPOCH+datetime.timedelta(seconds=latest_timestamp)).strftime("%Y-%m-%d %H:%M:%S")
        else:
            date = '-'
        self.axis.set_title("Latest timestamp: %s (%s)" % (latest_timestamp, date))


        self.update_digger_positions()
        #self.update_bucket_markers()
        self.update_dump_positions()

        self.update_cluster_markers()

        #self.plot_raw_zones()
        self.plot_zones()

        self.plot_intersections()



        with self.queue_driver.cluster_manager_lock as cluster_manager:

            useful_connections_in_zones, cluster_position_overrides = cluster_manager.get_useful_paths_in_zones()
            #cluster_position_overrides = dict() # this is only temporary to make figures.

            self.update_connections(useful_connections_in_zones=useful_connections_in_zones, cluster_position_overrides=cluster_position_overrides)


            if not self.fixed_limits:
                MARGIN = 100
                if cluster_manager.clusters:
                    limits_updated = False
                    for cluster in cluster_manager.clusters.values():
                        position = cluster.position

                        if position[0]-MARGIN < self.x_lim[0]:
                            self.x_lim[0] = position[0]-2*MARGIN
                            limits_updated = True

                        if self.x_lim[1] < position[0]+MARGIN:
                            self.x_lim[1] = position[0]+2*MARGIN
                            limits_updated = True

                        if position[1]-MARGIN < self.y_lim[0]:
                            self.y_lim[0] = position[1]-2*MARGIN
                            limits_updated = True

                        if self.y_lim[1] < position[1]+MARGIN:
                            self.y_lim[1] = position[1]+2*MARGIN
                            limits_updated = True

                    if limits_updated:
                        self.axis.set_xlim(self.x_lim)
                        self.axis.set_ylim(self.y_lim)
                        self.axis.set_aspect('equal')
                        self.axis.figure.canvas.toolbar.update()


            print("getting zone stuff")


#            for source, target in needed_edges:
#                self.axis.plot([source.position[0], target.position[0]], [source.position[1], target.position[1]], color='blue')


        print("drawing plot")


        self.flush_plots()


        print("plotting done")

    def set_fixed_limits(self, x_lim, y_lim):
        self.x_lim[0] = x_lim[0]
        self.x_lim[1] = x_lim[1]
        self.y_lim[0] = y_lim[0]
        self.y_lim[1] = y_lim[1]
        self.fixed_limits = True

        self.axis.set_xlim(self.x_lim)
        self.axis.set_ylim(self.y_lim)
        self.axis.set_aspect('equal')
        self.axis.figure.canvas.toolbar.update()


    def run_event_loop(self, duration):

        manager = matplotlib.backend_bases.Gcf.get_active()
        if manager is not None:
            canvas = manager.canvas
            if canvas.figure.stale:
                canvas.draw_idle()
            if not (hasattr(canvas, "_event_loop") and canvas._event_loop.isRunning()):
                canvas.start_event_loop(duration)


    def flush_plots(self, duration=0.000001):

        self.axis.figure.canvas.draw()
        self.axis.figure.canvas.flush_events()

        self.run_event_loop(duration)



