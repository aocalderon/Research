#### Creating a (quite simple) SUMO scenario from scratch.
Be sure [SUMO](https://eclipse.dev/sumo/) is installed and $SUMO_HOME is set correctly (this recipe uses version 1.18.0 for Linux). Most of the calls use configuration files.  See `pasto.netccfg`, `pasto.polycfg`, `pasto.sumocfg`, etc. for details in the input and output settings...

-   Call [`netconvert`](https://sumo.dlr.de/docs/netconvert.html) to create the network from OSM file...

    `netconvert -c pasto.netccfg`

-   Call [`polyconvert`](https://sumo.dlr.de/docs/polyconvert.html) to extract polygons from OSM file to be included during the visualization...

    `polyconvert -c pasto.polycfg`

-   Run `build` script to call [`randomTrips.py`](https://sumo.dlr.de/docs/Tools/Trip.html) and [`duarouter`](https://sumo.dlr.de/docs/duarouter.html) to create random origin and destination, and then generate routes using shortest path algorithms. It creates routes for pedestrians, bicycles, motorcycles, cars, buses and trucks...

    `./build`

-   Call [`sumo`](https://sumo.dlr.de/docs/sumo.html) to run the simulation and save the trajectories to a XML file...

    `./sumo -c pasto.sumocfg`

    or [`sumo-gui`](https://sumo.dlr.de/docs/sumo-gui.html) to launch the visualization...

    `./sumo-gui -c pasto.sumocfg`

-   Run `xml2tsv` script to call [`xml2csv.py`](https://sumo.dlr.de/docs/Tools/Xml.html) and convert the XML file to a TSV file with just ID of the object, lat and lon, speed and time instant...

    `./xml2tsv`

The script `runAll` executes all the steps...

------------------------------------------------------------------------

Large files were not attached. They are available online [1,2].

| Files                          | Description                                                                                                         |
|--------------------------------|---------------------------------------------------------------------------------------------------------------------|
| `trajectories.xml`             | FCD file from SUMO. It is a XML file with all data exported by the simulation [optional].                                      |
| `trajectories.tsv`             | TSV file converted from FCD file using xml2tsv script. Only has data about trajectory id, lat, lon, time and speed [optional]. |
| `trajectories_vehicles.tsv`    | Intermediate file with data about vehicles (bicycles, motorcycles, buses, cars and trucks) [optional].                         |
| `trajectories_pedestrians.tsv` | Intermediate file with data about pedestrians [optional].                                                                      |
| `pasto.osm`                    | OSM file for the study area. It is the input of netconvert to create the SUMO network [required].                              |

[1] [MEGA](https://mega.nz/folder/ilFSTTiA#asNaby1gUGyaWWtZl6IQMQ) [2]
[Google
Drive](https://drive.google.com/drive/folders/1zk6hm4-jrXSLQFIUzpTBMDt8qrfsvn18?usp=sharing)
