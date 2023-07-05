* netconvert -c pasto.netccfg

* polyconvert -c pasto.polycfg

* ./build

* ./simulate -c pasto.sumocfg or ./simulate-gui -c pasto.sumocfg

* ./xml2tsv

Large files were not attached.  They are available online [1,2].

| Files                          | Description|
|--------------------------------|---------------------------------------------------------------------------------|
| `trajectories.xml`             | FCD file from SUMO.  It is a XML file with all data exported by the simulation. |
| `trajectories.tsv`             | TSV file converted from FCD file using xml2tsv script.  Only has data about trajectory id, lat, lon, time and speed. |
| `trajectories_vehicles.tsv`    | Intermediate file with data about vehicles (bicycles, motorcycles, buses, cars and trucks). |
| `trajectories_pedestrians.tsv` | Intermediate file with data about pedestrians. |
| `pasto.osm`                    | OSM file for the study area.  It is the input of netconvert to create the SUMO network. |

[1] [MEGA](https://mega.nz/folder/ilFSTTiA#asNaby1gUGyaWWtZl6IQMQ)
[2] [Google Drive](https://drive.google.com/drive/folders/1zk6hm4-jrXSLQFIUzpTBMDt8qrfsvn18?usp=sharing)
