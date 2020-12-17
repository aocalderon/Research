library(tidyverse)
library(sf)
library(nngeo)

level0 = st_read("/Datasets/gadm/gadm36_levels_gpkg/gadm36_levels.gpkg", query="SELECT * FROM level0")
count = {}
writeWKT <- function(code){
  col0 = level0[level0$GID_0 == code, ]
  polys = st_cast(st_geometry(col0), "POLYGON")
  polys = st_remove_holes(polys)
  wkt = st_as_text(polys)
  df = tibble(wkt = wkt)
  df$code = code
  count = c(count, length(polys))
  #write.table(df, paste0("/Datasets/gadm/WKT/",code,".tsv"), col.names = T, row.names = F, sep = "\t", quote = F)
}

n = 2
#codes = sample(levels(level0$GID_0), n)
codes = levels(level0$GID_0)
for(code in codes){
  writeWKT(code)  
  print(paste(code, " done."))
}
