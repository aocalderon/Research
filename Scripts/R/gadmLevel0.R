library(tidyverse)
library(sf)
library(nngeo)

#level0 = st_read("/Datasets/gadm/gadm36_levels_gpkg/gadm36_levels.gpkg", query="SELECT * FROM level0")
writeWKT <- function(code){
  col0 = level0[level0$GID_0 == code, ]
  polys = st_cast(st_geometry(col0), "POLYGON")
  polys = st_remove_holes(polys)
  wkt = st_as_text(polys)
  df = tibble(wkt = wkt)
  df$code = code
  write.table(df, paste0("/tmp/",code,".tsv"), col.names = T, row.names = F, sep = "\t", quote = F)
}

writeWKT("COL")