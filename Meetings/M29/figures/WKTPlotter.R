require(tidyverse)
library(rgeos)
#library(mapview)
#library(tmap)
library(sf)

wkt = "POLYGON((1979897.70200000004842877 561446.52080000005662441, 1979897.70200000004842877 563977.30319999996572733, 1981901.92639999999664724 563977.30319999996572733, 1981901.92639999999664724 561446.52080000005662441, 1979897.70200000004842877 561446.52080000005662441))"
q1 = st_as_sf(readWKT(wkt))

bbox = st_bbox(q1)
xmin = bbox$xmin
ymin = bbox$ymin
xmax = bbox$xmax
ymax = bbox$ymax

w = xmax - xmin
h = ymax - ymin

ne = c(xmin,ymax)
nw = c(xmin + 2 * w, ymax)
sw = c(xmin + 2 * w, ymax - 2 * h)
se = c(xmin, ymax - 2 * h)

coords = cbind(ne, nw, sw, se, ne)
outer = matrix(coords, ncol = 2, byrow = T)
rings = list(outer) #, inner1, inner2, ... 
polygon = st_polygon(rings)

geom = st_sfc(polygon)
boundary = st_sf(geom = geom)
st_crs(boundary) = 6423
  
plot(boundary, axes = T)

write_tsv(st_as_text(polygon), "test.tsv")
