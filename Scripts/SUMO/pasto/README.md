
-   Create the network from OSM file...

    `netconvert -c pasto.netccfg`

-   Extract polygons from OSM file to be included during the
    visualization...

    `polyconvert -c pasto.polycfg`

-   Script to call 'randomTrips.py' and 'duarouter' to create random
    origin and destination, and then generate routes using shortest path
    algorithms. It creates routes for pedestrians, bicycles,
    motorcycles, cars, buses and trucks...

    `./build`

-   Run the simulation calling 'sumo' and save the trajectories to a XML
    file...

    `./simulate -c pasto.sumocfg`

    or to launch the visualization...

    `./simulate-gui -c pasto.sumocfg`

-   Convert the XML file to a TSV file with just ID of the object, lat
    and lon, speed and time instant...

    `./xml2tsv`

The script `runAll` executes all the steps...

------------------------------------------------------------------------

Large files were not attached. They are available online [1,2].

| Files                          | Description                                                                                                         |
|--------------------------------|---------------------------------------------------------------------------------------------------------------------|
| `trajectories.xml`             | FCD file from SUMO. It is a XML file with all data exported by the simulation.                                      |
| `trajectories.tsv`             | TSV file converted from FCD file using xml2tsv script. Only has data about trajectory id, lat, lon, time and speed. |
| `trajectories_vehicles.tsv`    | Intermediate file with data about vehicles (bicycles, motorcycles, buses, cars and trucks).                         |
| `trajectories_pedestrians.tsv` | Intermediate file with data about pedestrians.                                                                      |
| `pasto.osm`                    | OSM file for the study area. It is the input of netconvert to create the SUMO network.                              |

[1] [MEGA](https://mega.nz/folder/ilFSTTiA#asNaby1gUGyaWWtZl6IQMQ) [2]
[Google
Drive](https://drive.google.com/drive/folders/1zk6hm4-jrXSLQFIUzpTBMDt8qrfsvn18?usp=sharing)
