require(tidyverse)

P = c("036", "048", "060", "072", "084", "096", "108")
for(p in P){
  f = read_tsv(paste0("partitionsM-",p,".wkt"))
  print.xtable(xtable(f %>% select(partitionId, pointsIn, pointsOut, centersIn, centeresOut, duration) %>% arrange(desc(duration))), 
         file = paste0("table", p, ".tex"))
}