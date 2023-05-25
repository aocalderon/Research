require(tidyverse)
require(sf)
require(scales)
#require(ggmap)
#require(osmdata)

CRS = 6423
filename = "quads/n_T316_C2000_Q70.wkt"
data = read_tsv(filename, col_names = c("wkt", "cid", "n"))
geom = st_as_sf(data, crs = CRS, wkt = 1)

#mapBound <- geom %>% sf::st_transform(4326) %>% 
#  st_bbox() %>% st_as_sfc() %>% st_buffer(0.02) %>%
#  st_bbox() %>% as.numeric()
#la_map <- get_map(mapBound, maptype = "roadmap")

#la_bb <- getbb("Los Angeles")
#la_streets <- la_bb %>% opq() %>% add_osm_feature("highway", c("motorway", "primary", "secondary", "tertiary")) %>% osmdata_sf()
#la_small_streets <- la_bb %>% opq() %>% add_osm_feature(key = "highway", value = c("residential", "living_street", "unclassified", "service", "footway")) %>% osmdata_sf()
#la_rivers <- la_bb %>% opq() %>% add_osm_feature(key = "waterway", value = "river") %>% osmdata_sf()

maxlim = max(geom$n)
ggplot(geom) + 
  #geom_sf(data = la_streets$osm_lines, inherit.aes = FALSE, color = "#ffbe7f", size = .4, alpha = .8) +
  #geom_sf(data = la_small_streets$osm_lines, inherit.aes = FALSE, color = "#a6a6a6", size = .2, alpha = .8) +
  #geom_sf(data = la_rivers$osm_lines, inherit.aes = FALSE, color = "#7fc0ff", size = .8, alpha = .5) +  
  geom_sf(aes(fill = n)) + 
  scale_fill_gradient(low = "#FFFFFF", high ="#FF0000") +
  theme(axis.text.x = element_blank(),
                axis.text.y = element_blank(),
                axis.ticks = element_blank())
