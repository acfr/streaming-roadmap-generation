# Streaming Roadmap Genarator

High quality road maps are crucial to efficiently operate a fleet of vehicles. In industrial settings such as open cut 
mining, roads are highly dynamic and continuously change as the operation progresses. Manually updating road maps in 
this setting is expensive and error prone. Previous road mapping methods considered static road networks with at most 
occasional changes. Here, the Streaming Roadmap Generator (SRG) is presented, a framework which maintains a dynamic 
representation of the road map, that is continuously updated as new GNSS positional data is ingested, and allows 
generation of road maps without re-processing historic data. SRG is the first map inferencing algorithm designed for 
dynamic road networks, thus solving the trade-off between a long observation window to achieve coverage and a short 
observation window to avoid mapping outdated features. SRG is able to detect and map free-drive areas, such as benches 
and unload areas in mining, where vehicles no longer follow established roads.

## How to run
The main part of the code resides in the package streaming_rng. A demonstration for how to use the package is 
provided in GenerateRoadMap.py. Simply run it with: 

```python GenerateRoadMap.py```

For confidentiality reasons, no real vehicle telemetry can be provided here. Instead, the provided .csv files contain 
simplistic synthetic data to form a small road map for demponstration purposes.

For deployment with incremental updating using live data it is recommended to adapt the `StreamingRoadNetworkGenerator` 
class to stream live data directly instead of loading .csv files.

## Author
This work was created by Konstantin Seiler, The University of Sydney

## License
Streaming Roadmap Generation. Progressive online mapping of roads in real-time from vehicle location data. Copyright (C) 2022 Konstantin Seiler.
 
This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 
This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 
You should have received a copy of the GNU General Public License along with this program.  If not, see https://www.gnu.org/licenses/.
 
Further information about this program can be obtained from:

- Konstantin Seiler (konstantin.seiler@sydney.edu.au)
- Thomas Albrecht (thomas.albrecht@riotinto.com, Rio Tinto, Central Park, Lvl 4, 152-158 St Georges Tce, Perth, 6000, Western Australia)