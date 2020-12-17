library(tidyverse)
library(sf)
library(nngeo)

level1 = st_read("/Datasets/gadm/gadm36_levels_gpkg/gadm36_levels.gpkg", query="SELECT * FROM level1")
polys_prime = st_cast(st_geometry(level1), "POLYGON")
polys = st_remove_holes(polys_prime)