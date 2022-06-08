#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__author__ = "Konstantin Seiler, The University of Sydney"
__copyright__ = "Copyright Technological Resources Pty Ltd (a Rio Tinto Group company), 2019"



import csv
import datetime
import threading

try:
    from math import isfinite
except ImportError:
    from numpy import isfinite

import streaming_rng
from streaming_rng import center_angle
from streaming_rng import ClusterManagerQueueDriver, FeederQueue, StreamingRngSettings, GpsPoint
from streaming_rng.cluster_manager_painter_threadsafe import ClusterManagerPainterThreadsafe
from streaming_rng.helpers import EPOCH




INF=float('inf')

def format_time(time):
    return time.strftime("%d/%m/%Y %H:%M:%S (%Z)")



class StreamingRoadNetworkGenerator(object):
    def __init__(self, logger=None):
        if logger is None:
            import logging
            logger = logging.getLogger('StreamingRoadNetworkGenerator')
        self.logger = logger

    def stream_gps_data_pairs(self, feeder_queue):

        try:
            with open('vehicle_positions.csv', 'rt') as f:
                reader = csv.DictReader(f, ['name', 'timestamp', 'x', 'y', 'z', 'speed', 'orientation'])

                last_points = dict()

                for row in reader:

                    point = GpsPoint(
                            x=float(row['x']),
                            y=float(row['y']),
                            z=float(row['z']),
                            orientation=center_angle(float(row['orientation'])),
                            speed=float(row['speed']),
                            timestamp=float(row['timestamp']),
                        )

                    if row['name'] not in last_points:
                        last_points[row['name']] = point

                    else:
                        last_point = last_points[row['name']]
                        feeder_queue.push_gps_pair(last_point, point)
                        last_points[row['name']] = point

        finally:
            feeder_queue.close()

    def stream_gps_tags(self, feeder_queue):

        try:
            with open('tagged_positions.csv', 'rt') as f:
                reader = csv.DictReader(f, ['name', 'timestamp', 'x', 'y', 'z', 'orientation', 'speed', 'tag'])

                for row in reader:

                    point = GpsPoint(
                            x=float(row['x']),
                            y=float(row['y']),
                            z=float(row['z']),
                            orientation=center_angle(float(row['orientation'])),
                            speed=float(row['speed']),
                            timestamp=float(row['timestamp']),
                        )

                    if row['tag'].startswith('LOAD'):
                        tag_type = streaming_rng.types.TagType.LOAD_LOCATION
                    elif row['tag'].startswith('DUMP'):
                        tag_type = streaming_rng.types.TagType.DUMP_LOCATION
                    else:
                        raise ValueError('Cannot guess the tag type from the tag name: %s' % (row['tag'],))

                    feeder_queue.push_gps_point_tag(point, row['tag'], tag_type)

        finally:
            feeder_queue.close()

    def stream_loader_positions(self, feeder_queue):

        try:
            with open('endpoint_positions.csv', 'rt') as f:
                reader = csv.DictReader(f, ['name', 'timestamp', 'x', 'y', 'z', 'orientation'])

                for row in reader:

                    if row['name'].startswith('LOAD'):
                        # Update the GPS location of the loader
                        feeder_queue.push_digger_position(timestamp=float(row['timestamp']), digger=row['name'], position=(float(row['x']), float(row['y']), float(row['z'])))

                        # Update the name of the location the loader is at the moment.
                        # Here we just set the location name to the name of the loader itself.
                        feeder_queue.push_equipment_location(timestamp=float(row['timestamp']), equipment=row['name'], location=row['name'])

                    if row['name'].startswith('DUMP'):
                        # Update the GPS location of the dump point
                        feeder_queue.push_dump_position(timestamp=float(row['timestamp']), dump=row['name'], position=(float(row['x']), float(row['y']), float(row['z'])))

                        # Update the name of the location the loader is at the moment.
                        # Here we just set the location name to the name of the loader itself.
                        feeder_queue.push_equipment_location(timestamp=float(row['timestamp']), equipment=row['name'], location=row['name'])


        finally:
            feeder_queue.close()



    def run_streaming_rng(self):

        self.driver = ClusterManagerQueueDriver(settings=StreamingRngSettings())

        streamer_methods = ['stream_gps_data_pairs', 'stream_gps_tags', 'stream_loader_positions']

        for streamer_method in streamer_methods:
            feeder = FeederQueue(live_mode=False, maxsize=30000)
            threading.Thread(target=getattr(self, streamer_method), args=(feeder,), name=streamer_method).start()
            self.driver.add_queue(feeder)


        run_thread = threading.Thread(target=self.driver.run, name='run_cluster_manager')
        run_thread.start()


        try:

            self.cluster_painter = None
            try:
                self.cluster_painter = ClusterManagerPainterThreadsafe(self.driver)
            except:
                self.logger.error("ERROR: Can't instantiate painter.")
                self.logger.info("Continuing without GUI.")

                while run_thread.is_alive():
                    with self.driver.cluster_manager_lock as cluster_manager:
                        latest_timestamp = cluster_manager.latest_timestamp

                    if isfinite(latest_timestamp):
                        date = (EPOCH+datetime.timedelta(seconds=latest_timestamp)).strftime("%Y-%m-%d %H:%M:%S")
                    else:
                        date = '-'
                    self.logger.info("Latest timestamp: %s (%s)" % (latest_timestamp, date))

            else:
                def prepare_road_map():
                    with self.driver.cluster_manager_lock as cluster_manager:
                        cluster_manager.prepare_road_map()

                while run_thread.is_alive():
                    prepare_road_map_thread = threading.Thread(target=prepare_road_map)
                    prepare_road_map_thread.start()
                    while prepare_road_map_thread.is_alive():
                        self.cluster_painter.run_event_loop(0.1)
                    prepare_road_map_thread.join()

                    self.cluster_painter.update()
                    for _ in range(10):
                        if not run_thread.is_alive():
                            break
                        else:
                            self.cluster_painter.run_event_loop(1)

        except KeyboardInterrupt:
            self.driver.stop()
            run_thread.join()
            raise

        finally:
            self.driver.close_all_queues()

        run_thread.join()
        self.logger.info("Run thread has finished.")

        if self.cluster_painter is not None:
            with self.driver.cluster_manager_lock as cluster_manager:
                cluster_manager.prepare_road_map()
                self.cluster_painter.update()

        result = self.driver.get_road_map()
        return result


    def run(self):

        road_map = self.run_streaming_rng()

        with open('final_map_as_csv.csv', mode='w') as csv_file:
            streaming_rng.road_network.write_csv(road_map, csv_file)

        streaming_rng.road_network.write_dxf(road_map, 'final_map_as_dxf.dxf')

        self.logger.info('done.')



if __name__ == '__main__':

    import logging
    logging.basicConfig(level=logging.DEBUG)

    generator = StreamingRoadNetworkGenerator()
    generator.run()
