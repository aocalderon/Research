#### Creating a (quite simple) SUMO scenario from scratch.
Be sure [SUMO](https://eclipse.dev/sumo/) is installed and $SUMO_HOME is set correctly (this recipe uses version 1.18.0 for Linux). Most of the calls use configuration files.  See `dense.netccfg`, `dense.polycfg`, `dense.sumocfg`, etc. for details in the input and output settings...

-   Call [`netconvert`](https://sumo.dlr.de/docs/netconvert.html) to create the network from OSM file...

    `netconvert -c dense.netccfg`

-   Call [`polyconvert`](https://sumo.dlr.de/docs/polyconvert.html) to extract polygons from OSM file to be included during the visualization...

    `polyconvert -c dense.polycfg`

-   Run `build` script to call [`randomTrips.py`](https://sumo.dlr.de/docs/Tools/Trip.html) and [`duarouter`](https://sumo.dlr.de/docs/duarouter.html) to create random origin and destination, and then generate routes using shortest path algorithms. It creates routes for pedestrians, bicycles, motorcycles, cars, buses and trucks...

    `./dense_randomTrips`

-   Run [`simulate`] script which call [`sumo`](https://sumo.dlr.de/docs/sumo.html) to execute the simulation and save the trajectories to a XML file...

    `./simulate`

-   Run `xml2tsv` script to call [`xml2csv.py`](https://sumo.dlr.de/docs/Tools/Xml.html) and convert the XML file to a TSV file with just ID of the object, lat and lon, speed and time instant...

    `./xml2tsv`

The script `runAll` executes all the steps...
