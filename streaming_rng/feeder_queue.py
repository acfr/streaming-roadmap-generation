# Py 2/3 compat imports
from __future__ import print_function, division, absolute_import
from future.builtins import dict, list, object, range, str, bytes, filter, map, zip, ascii, chr, hex, input, next, oct, open, pow, round, super
from future.standard_library import install_aliases; install_aliases()
# This file is compatible with Python 2 and Python 3, please keep it so

__author__ = "Konstantin Seiler, The University of Sydney"
__copyright__ = "Copyright Technological Resources Pty Ltd (a Rio Tinto Group company), 2019"

import heapq
import threading
from six import string_types
from .helpers import INF


class FeederQueue(object):
    """
    An input queue for the ClusterManagerQueueDriver.

    The queue is thread safe in the sence that one thread can push onto the
    queue while another one can pop from it.

    Data in each queue must be in chronological order.

    The queue has two modes that differ in how the queue behaves if it's empty:
        historic mode: reading from the queue blocks until data is available.
            The assumption when the queue is empty is, that there's a delay in the
            feeder thread.
        live mode: reading from the queue returns None to signify that there is
            no data. The assumption when the queue is empty is, that the data source is
            up to date and no new data is available.
    """

    # internal commands have numbers < 0
    _CLOSE = -1
    _LIVE_MODE = -2
    _HISTORY_MODE = -3
    _EXCEPTION = -4

    # data types have numbers > 0
    GPS_POINT = 1
    GPS_PAIR = 2
    DIGGER_POSITION = 3
    BUCKET_POSITION = 4
    DUMP_POSITION = 5
    EQUIPMENT_LOCATION = 6
    GPS_TAG = 7

    def __init__(self, live_mode, maxsize=0):
        """
        live_mode: bool. Set to True if the queue is intended to be fed with
            live data in real time. Set to false if the data is historic data.
        maxsize: the maximum size of the queue. If the queue is full, pushing
            onto the queue blocks until space is available.
            If set to zero, the queue size is unlimited.

        Note: for queues that stream large amounts of historic data, it is advisable
            to limit the queue size so the system doesn't waste too much memory
            on buffering the data.
        """

        self._lock = threading.Condition()
        """Lock for queue operations"""

        self._latest_timestamp = -INF
        """Contains the last seen timestamp."""

        self._latest_item_counter = 0
        """Counter for how many items were put onto the queue. 
        This is used to keep the sort stable if multiple items have the same timestamp."""

        self._latest_epoch_counter = 0
        """Counter for the epoch for sort orders."""

        self._queue = []
        """The underlying low level queue.
        Items in the queue are tupels of the form (epoch, timestamp, sequence_number, message_type, data)
        """

        self._maxsize = maxsize
        """The maximum size of the queue."""

        self._top = None
        """The data that is currently at the top of the queue. Since python's
        queue implementation doesn't allow to peek, the item must be popped and
        then stored here."""

        self._closed = False
        """Set to true if the queue is considered closed."""

        self._closed_for_push = False
        """Set to true if the queue is considered closed for further pushes."""

        self._live_mode = live_mode
        """Set to true if the queue is in live mode, False otherwise."""


    def _process_internal_messages(self):
        with self._lock:
            while not self._closed and self._queue and self._queue[0][3] < 0:
                _, _, _, message_type, data = heapq.heappop(self._queue)
                self._lock.notify_all()

                assert message_type < 0

                if message_type == self._CLOSE:
                    self._closed = True
                elif message_type == self._LIVE_MODE:
                    self._live_mode = True
                elif message_type == self._HISTORY_MODE:
                    self._live_mode = False
                elif message_type == self._EXCEPTION:
                    raise data
                else:
                    raise NotImplementedError("An unexpected internal command of %s was found." % (message_type,))

    def _push_message(self, message_type, timestamp, data):
        with self._lock:

            if self._closed_for_push:
                raise RuntimeError("This queue is already closed. No more messages can be pushed onto it.")

            # block until there is space in the queue for us.
            while self._maxsize > 0 and len(self._queue) >= self._maxsize:
                self._lock.wait()

            # get a new running id
            self._latest_item_counter += 1

            # if it is an internal message start a new epoch in the sort order
            if message_type < 0 or timestamp is None:
                self._latest_epoch_counter += 1

            if timestamp is not None:
                self._latest_timestamp = timestamp
            else:
                timestamp = -INF

            # put the element onto the queue.
            item = (self._latest_epoch_counter, timestamp, self._latest_item_counter, message_type, data)
            heapq.heappush(self._queue, item)

            # close the push side if needed:
            if message_type == self._CLOSE:
                self._closed_for_push = True

            # notify thread that may be waiting on an empty queue.
            self._lock.notify_all()

    #====================================#
    # Functions for the consuming thread #
    #====================================#

    @property
    def closed(self):
        """True if the queue is closed."""
        return self._closed

    @property
    def live_mode(self):
        """True if the queue is in live mode."""
        return self._live_mode

    def peek_time(self):
        """
        Return the timestamp of the next item in the queue without popping it.

        To be used by the consuming thread.
        """

        result = self.peek()
        if result is not None:
            result = result[1]
        return result


    def peek(self):
        """
        Return the next item on the queue without popping it.

        To be used by the consuming thread.
        """

        with self._lock:
            while True:
                if self._closed:
                    return None

                if not self._queue:
                    if self._live_mode:
                        return None
                    else:
                        self._lock.wait()

                else:
                    if self._queue[0][3] < 0:
                        self._process_internal_messages()
                    else:
                        _, timestamp, _, message_type, data = self._queue[0]
                        return message_type, timestamp, data

    def pop(self):
        """
        Return the next item on the queue and pop it from the queue.

        To be used by the consuming thread.
        """

        with self._lock:
            while True:
                if self._closed:
                    return None

                if not self._queue:
                    if self._live_mode:
                        return None
                    else:
                        self._lock.wait()

                else:
                    if self._queue[0][3] < 0:
                        self._process_internal_messages()
                    else:
                        _, timestamp, _, message_type, data = heapq.heappop(self._queue)
                        self._lock.notify_all()
                        return message_type, timestamp, data

    def latest_time(self):
        return self._latest_timestamp

    #=======================================================#
    # Functions for the feeding thread to control the queue #
    #=======================================================#

    def close(self):
        """
        Close the queue.

        After calling close on a queue, no more data should be pushed onto the queue.
        Once all data that was on the queue before close() was called has been popped,
        the queue goes into closed state.

        To be used by the pushing thread.
        """
        with self._lock:
            if not self._closed_for_push:
                self._push_message(self._CLOSE, None, None)

    def set_live_mode(self):
        """
        Set queue to live mode.

        The queue will switch its mode once all data that is on the queue at the time
        this method is called has been popped.

        To be used by the pushing thread.
        """
        self._push_message(self._LIVE_MODE, None, None)

    def set_history_mode(self):
        """
        Set queue to historic mode.

        The queue will switch its mode once all data that is on the queue at the time
        this method is called has been popped.

        To be used by the pushing thread.
        """
        self._push_message(self._HISTORY_MODE, None, None)

    def push_exception(self, exception, immediately=False):
        """
        Raise an exception in the consuming thread.

        exception: either an Exception object to raise or a string containing
            a message which will then be encapsulated in a new Exception object.
        immediately: if True, the exception will be put onto the top of the queue
            so it is raised the next time the queue is accessed.
        """

        if isinstance(exception, string_types):
            exception = Exception(exception)

        assert isinstance(exception, Exception), "The provided exception is neither an Exception object nor a string."

        if immediately:
            with self._lock:
                item = (-INF, -INF, -INF, self._EXCEPTION, exception)
                heapq.heappush(self._queue, item)

                # notify thread that may be waiting on an empty queue.
                self._lock.notify_all()
        else:
            self._push_message(self._EXCEPTION, None, exception)

    #===============================================================#
    # Functions for the feeding thread to provide data to the queue #
    #===============================================================#

    def push_gps_point(self, point):
        """
        Provide a single GPS point.

        point: A `GpsPoint` object.

        To be used by the pushing thread.
        """
        self._push_message(self.GPS_POINT, point.timestamp, point)

    def push_gps_point_tag(self, point, tag, tag_type):
        """
        Provide a tag for a GPS point.

        When a point is tagged, it is considered to be part of the area that it is
        tagged for. This information is used to mark areas in the final road map.

        point: A `GpsPoint` object.
        tag: string. The name of the area the point is tagged to be in.
        tag_type: int. One of the constants defined in types.TagType to signify what type the tag is

        To be used by the pushing thread.
        """
        assert isinstance(tag, string_types), "Tags must be given as strings."
        self._push_message(self.GPS_TAG, point.timestamp, (point, tag, tag_type))

    def push_gps_pair(self, source_point, target_point):
        """
        Provide a pairt of consecutive GPS points.

        The points must be consecutive CPS points of the same vehicle. The algorithm uses
        this information to create connectivity and path information along the road map.

        Pairs must consistently reconstruct into paths. Thus, each gps point is only allowed
        to be used as source point of a pair once. Also, each gps point is only allowed to
        be used as target point of a pair once.

        source_point: GpsPoint object. The first of the two points
        target_point: GpsPoint object. The second of the two points

        To be used by the pushing thread.
        """
        self._push_message(self.GPS_PAIR, target_point.timestamp, (source_point, target_point))

    def push_digger_position(self, timestamp, digger, position):
        """
        Provide the GPS position of a digger.

        timestamp: float. Time the measurement was taken in seconds since epoch.
        digger: string. The name of the digger.
        position: tuple of float. 3-tuple with x-y-z position coordinates.

        To be used by the pushing thread.
        """
        assert isinstance(digger, string_types), "Digger names must be given as strings."
        self._push_message(self.DIGGER_POSITION, timestamp, (digger, position))

    def push_bucket_position(self, timestamp, digger, position):
        """
        Provide the GPS position of bucket that was scooped.

        timestamp: float. Time the measurement was taken in seconds since epoch.
        digger: string. The name of the digger that scooped the bucket.
        position: tuple of float. 3-tuple with x-y-z position coordinates of the bucket.

        To be used by the pushing thread.
        """
        assert isinstance(digger, string_types), "Digger names must be given as strings."
        self._push_message(self.BUCKET_POSITION, timestamp, (digger, position))

    def push_dump_position(self, timestamp, dump, position):
        """
        Provide the GPS position of a dump.

        timestamp: float. Time the measurement was taken in seconds since epoch.
        dump: string. The name of the dump.
        position: tuple of float. 3-tuple with x-y-z position coordinates.

        To be used by the pushing thread.
        """
        assert isinstance(dump, string_types), "Dump names must be given as strings."
        self._push_message(self.DUMP_POSITION, timestamp, (dump, position))

    def push_equipment_location(self, timestamp, equipment, location):
        """
        Provide the road network location name of a piece of equipment. Currently
        used for diggers and dumps.

        This information is used for automatic tagging and naming of areas.

        timestamp: float. Time the measurement was taken in seconds since epoch.
        equipment: string. The name of the digger or dump.
        location: string. The name of the road network location of the piece of equipment.

        To be used by the pushing thread.
        """
        assert isinstance(equipment, string_types), "Equipment names must be given as strings."
        assert isinstance(location, string_types), "Locations must be given as strings."
        self._push_message(self.EQUIPMENT_LOCATION, timestamp, (equipment, location))
