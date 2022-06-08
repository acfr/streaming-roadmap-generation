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


import time
import threading
import json
import bz2
import datetime
import sys

from .helpers import INF, EPOCH
from .cluster_manager import ClusterManager
from .feeder_queue import FeederQueue
from .settings import StreamingRngSettings

if sys.version_info.major >= 3:
    from math import isfinite
else:
    from numpy import isfinite


class _DataAccessLock(object):
    """
    Convenience object to manage a lock for accessing a data object.

    It is intended to be used as context guard as follows:

    creation:
        lock = DataAccessLock(SomeClass())

    access:
        with lock as data:
            data.do_something()


    The lock is reentrant.
    """

    def __init__(self, data):
        self._lock = threading.RLock()
        self._data = data

    def __enter__(self):
        self._lock.acquire()
        return self._data

    def __exit__(self, *args, **kwargs):
        self._lock.release()

    def acquire(self):
        self._lock.acquire()

    def release(self):
        self._lock.release()


class _ContextCounter(object):
    """
    Thread save counter that can be used as context manager.
    It counts how many times the counter is being used.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._value = 0

    @property
    def value(self):
        """The number of times this object is currently being used as context manager."""
        with self._lock:
            return self._value

    def __enter__(self):
        with self._lock:
            self._value += 1

    def __exit__(self, *args, **kwargs):
        with self._lock:
            self._value -= 1



class ClusterManagerQueueDriver(object):
    """
    Queue based driver for the road map generation by the ClusterManager.

    It provides a thread safe interface to road map generation.

    Data is provided asynchronously by an arbitrary numbers of queues.


    Implementation note: if multiple locks are held, they must be aquired in the
        order they are defined in __init__ to avoid deadlocks.
    """

    def __init__(self, settings=None, queues=[], snapshot_file=None, logger=None, load_settings_from_snapshot=False):
        """
        settings: StreamingRngSettings object. The settings that are used within the algorithm.
        queues: list of FeederQueue objects. An initial list of queues to add to the driver.
        """

        # Implementation note: if multiple locks are held, they must be aquired in the
        # order they are defined in __init__ to avoid deadlocks.

        if logger is None:
            import logging
            logger = logging.getLogger('ClusterManagerQueueDriver')
        self.logger = logger

        if settings is None and snapshot_file is None:
            raise ValueError("Required argument missing: either 'settings' or 'snapshot_data' must be provided.")

        if settings is None:
            settings = StreamingRngSettings()


        self.settings = settings
        """The settings that are used within the algorithm."""


        self._stop_lock = threading.Lock()
        """Lock to protect stop()"""

        self._stop_run_threads = False
        """If set to True, run() will terminate on the next iteration.
        Used by stop()

        Hold _stop_lock to manipulate, read without lock
        """


        self._run_lock = threading.Lock()
        """Lock for running an iteration of the main loop of run().

        Ensures, only one thread is currently processing data. Can also be used to block
        run() from processing while major updates are happending (e.g. loading a snapshot).
        """

        self._iterations_since_assigning_points = 0 # hold _run_lock to manipulate
        """Counter to determine when points must be assigned next.

        hold _run_lock to manipulate
        """

        self._iterations_since_updating_connections = 0 # hold _run_lock to manipulate
        """Counter to determine when connections must be updated next.

        hold _run_lock to manipulate
        """

        self._iterations_since_pruning_points = 0 # hold _run_lock to manipulate
        """Counter to determine when points must be pruned next.

        hold _run_lock to manipulate
        """

        self._debug_print_data = dict() # hold _run_lock to manipulate
        """Dict to aggregate data for smart debug printing.

        hold _run_lock to manipulate
        """


        self.run_counter = _ContextCounter()
        """Threadsafe counter to track whether "run()" is running."""



        self._queues_write_lock = threading.Lock()
        """Lock to protect write access to _queues."""
        self._queues = list(queues)
        """The list of FeederQueue objects in use.

        hold the _queues_write_lock when changing the list.
        read access is allowed without locking. Thus, a valid list must be present at all times.
        """


        self.cluster_manager_lock = _DataAccessLock(ClusterManager(self.settings, self.logger))
        """Context guarded lock which provides safe access to the ClusterManager object."""

        if snapshot_file is not None:
            self.load_snapshot_from_file(snapshot_file, load_settings_from_snapshot=load_settings_from_snapshot)


    @property
    def _cluster_manager_unsafe(self):
        """
        Direct access to the ClusterManager object. This is not thread safe. It is
        mainly intended as convenience when debugging.
        """
        return self.cluster_manager_lock._data


    def add_queue(self, new_queue):
        """
        Add a new feeder queue to the driver.

        new_queue: FeederQueue object.

        Note: there is no remove_queue() method. The way to remove a queue is
            to close the queue. Closed queues will be removed automatically.
        """
        with self._queues_write_lock:
            self._queues.append(new_queue)

    def close_all_queues(self):
        """
        Close all queues.
        After this, any attempt to push more data into any of the queues will raise an exception.
        """
        with self._queues_write_lock:
            for queue in self._queues:
                queue.close()


    def run(self):
        """
        Run the road map generation algorithm.

        Runs until there are no more queues that are not closed.

        Note: while queues can be added while run() executes, it is advisable to add
            queues before calling run(). Otherwise, there is a race condition with
            run exiting if queues aren't added in time.
        """

        with self.run_counter:

            self.logger.info("Starting run() of ClusterManagerQueueDriver")


            while not self._stop_run_threads:

                with self._run_lock:


                    best_queue = None
                    best_queue_time = INF
                    newest_data_in_queue = -INF
                    there_is_an_empty_queue = False

                    # check which data point we're going to process next
                    # access the queues by index to account for multithreading
                    index = 0
                    while index < len(self._queues):

                        try:
                            candidate = self._queues[index]
                        except IndexError:
                            continue

                        if candidate.closed:
                            with self._queues_write_lock:
                                if index < len(self._queues) and self._queues[index].closed:
                                    del self._queues[index]
                        else:
                            index += 1

                            queue_time = candidate.peek_time()

                            if queue_time is None:
                                there_is_an_empty_queue = True

                            elif queue_time < best_queue_time:
                                best_queue = candidate
                                best_queue_time = queue_time

                            latest_time = candidate.latest_time()
                            if latest_time > newest_data_in_queue:
                                newest_data_in_queue = latest_time

                    if there_is_an_empty_queue and best_queue is not None:
                        if newest_data_in_queue < best_queue_time + 180:
                            # there is an empty live queue but the latest data to process is pretty recent.
                            # don't process the data we have to give the live queue a chance to catch up.
                            best_queue = None


                    # All queues are closed, we stop, otherwise, we wait a little
                    if best_queue is None:
                        if self._queues:

                            # offer the cluster manager a chance to do some house keeping
                            tic = time.time()
                            with self.cluster_manager_lock as cluster_manager:
                                cluster_manager.perform_idle_tasks()
                            time_delta = time.time() - tic

                            # wait a little if the cluster manager didn't use the time.
                            if time_delta < 0.5:
                                time.sleep(0.5-time_delta)

                            continue
                        else:
                            break


                    data_type, timestamp, data = best_queue.pop() # safe because we already peeked, so we know that there is data.

                    data_type_debug_name = {
                            FeederQueue.GPS_POINT: 'gps_point',
                            FeederQueue.GPS_PAIR: 'gps_pair',
                            FeederQueue.DIGGER_POSITION: 'digger_pos',
                            FeederQueue.BUCKET_POSITION: 'bucket_pos',
                            FeederQueue.DUMP_POSITION: 'dump_pos',
                            FeederQueue.EQUIPMENT_LOCATION: 'eq_loc',
                            FeederQueue.GPS_TAG: 'gps_tag',
                    }.get(data_type, str(data_type))

                    time_without_seconds = (EPOCH+datetime.timedelta(seconds=timestamp)).strftime("%Y-%m-%d %H:%M")

                    if self._debug_print_data.get('_last_timestamp') != time_without_seconds:
                        if '_last_timestamp' in self._debug_print_data:
                            message = 'Received data for time %s: ' % (self._debug_print_data['_last_timestamp'],)
                            message += ', '.join(['%s: %i' % (k,v) for k,v in self._debug_print_data.items() if not k.startswith('_')])
                            self.logger.debug(message)
                        self._debug_print_data = {'_last_timestamp': time_without_seconds}

                    self._debug_print_data[data_type_debug_name] = self._debug_print_data.get(data_type_debug_name, 0) + 1



                    with self.cluster_manager_lock as cluster_manager:

                        if isfinite(cluster_manager.latest_timestamp) and timestamp + 10 < cluster_manager.latest_timestamp:
                            self.logger.debug('Data of type "%s" from %s queue is fed to the cluster manager out of order (%i seconds earlier). The latest timestamp in the cluster manager is %i (%s). The timestamp of the new data is %i (%s).' %
                                            (
                                                    data_type_debug_name,
                                                    'live' if best_queue.live_mode else 'historic',
                                                    cluster_manager.latest_timestamp - timestamp,
                                                    cluster_manager.latest_timestamp,
                                                    (EPOCH+datetime.timedelta(seconds=cluster_manager.latest_timestamp)).strftime("%Y-%m-%d %H:%M:%S"),
                                                    timestamp,
                                                    (EPOCH+datetime.timedelta(seconds=timestamp)).strftime("%Y-%m-%d %H:%M:%S"),

                                            )
                                        )


                        if data_type == FeederQueue.GPS_POINT:
                            point = data
                            cluster_manager.add_point(point)

                        elif data_type == FeederQueue.GPS_PAIR:
                            source, target = data
                            cluster_manager.add_edge(source, target)

                        elif data_type == FeederQueue.DIGGER_POSITION:
                            digger, position = data
                            cluster_manager.set_digger_position(timestamp, digger, position)

                        elif data_type == FeederQueue.BUCKET_POSITION:
                            digger, position = data
                            cluster_manager.add_bucket_position(digger, position)

                        elif data_type == FeederQueue.DUMP_POSITION:
                            dump, position = data
                            cluster_manager.set_dump_position(timestamp, dump, position)

                        elif data_type == FeederQueue.EQUIPMENT_LOCATION:
                            equipment, location = data
                            cluster_manager.set_equipment_location(timestamp, equipment, location)

                        elif data_type == FeederQueue.GPS_TAG:
                            point, tag, tag_type = data
                            cluster_manager.set_point_tag(point, tag, tag_type)

                        else:
                            raise NotImplementedError("Unexpected data type found in queue: %s" % (data_type,))



                        self._iterations_since_assigning_points += 1
                        self._iterations_since_updating_connections += 1
                        self._iterations_since_pruning_points += 1


                        assign_points = False
                        update_connections = False
                        prune_points = False

                        if self._iterations_since_assigning_points >= self.settings.assign_points_every:
                            assign_points = True

                        if self._iterations_since_updating_connections >= self.settings.update_connections_every:
                            assign_points = True
                            update_connections = True

                        if self._iterations_since_pruning_points >= self.settings.prune_points_every:
                            assign_points = True
                            update_connections = True
                            prune_points = True


                        if assign_points:
                            cluster_manager.prune_and_merge_clusters()
                            cluster_manager.assign_points_to_clusters()
                            self._iterations_since_assigning_points = 0

                        if update_connections:
                            cluster_manager.update_connections()
                            self._iterations_since_updating_connections = 0

                        if prune_points:
                            cluster_manager.prune_old_points()
                            self._iterations_since_pruning_points = 0



            with self.cluster_manager_lock as cluster_manager:

                for _ in range(20):
                    cluster_manager.assign_points_to_clusters()

                for _ in range(2):
                    cluster_manager.prepare_road_map()


        self.logger.info("Finished with run() of ClusterManagerQueueDriver")


    def stop(self):
        """
        Stop all currently running instances of run().
        """

        with self._stop_lock:

            try:
                self._stop_run_threads = True

                while self.run_counter.value > 0:
                    time.sleep(0.1)

            finally:
                self._stop_run_threads = False


    def get_road_map(self):
        """
        Return the latest version of the road map.

        Note: depending on the state of the algorithm, considerable work might have to
            be done by this method if there are pending operations that must be performed
            before a good road map can be returned.

        returns: RoadNetwork object.
        """

        with self.cluster_manager_lock as cluster_manager:
            return cluster_manager.get_road_map()


    def save_snapshot_to_file(self, file):
        with self.cluster_manager_lock as cluster_manager:
            cluster_manager.save_snapshot_to_File(file)


    def load_snapshot_from_file(self, file, load_settings_from_snapshot=False):
        """
        Restore a snapshot in the cluster manager.

        snapshot_data: a snapshot as created by get_snapshot_data()

        If the snapshot data is bz2 compressed, it will be uncompressed automatically.

        Note: this method will remove all queues from the driver.

        Note: this method stops any run() threads. (this would most likely be
            a side effect anyway since the queues are removed, but there's a
            race condition, so the termination is done explicitly).
        """

        self.stop()

        with self._run_lock:
            with self.cluster_manager_lock as cluster_manager:
                try:
                    cluster_manager.load_snapshot_from_file(file, load_settings_from_snapshot=load_settings_from_snapshot)

                finally:
                    self.settings = cluster_manager.settings
                    self._iterations_since_assigning_points = 0
                    self._iterations_since_updating_connections = 0
                    self._iterations_since_pruning_points = 0

                    with self._queues_write_lock:
                        del self._queues[:]
