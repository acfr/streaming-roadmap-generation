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


from .settings import StreamingRngSettings
from .helpers import center_angle, INF
from .types import GpsPoint
from . import road_network
from .cluster_manager_queue_driver import ClusterManagerQueueDriver
from .feeder_queue import FeederQueue
