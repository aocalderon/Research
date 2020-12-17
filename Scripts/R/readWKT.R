library(sf)

for(code in codes){
  map = read_sf(paste0("/tmp/",code,".tsv"))
  plot(map$geometry) + title(main=map$code[1])
}
