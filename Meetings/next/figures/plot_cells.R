library(tidyverse)
library(knitr)
library(xtable)
library(sf)

data <- read_tsv("~/Research/Datasets/LA/cells_coarser.tsv", col_names = c("wkt", "cid", "area", "n1", "d1", "n2", "d2"))

CRS <- 6423
time_instant <- 320
geom <- st_as_sf(data, crs = CRS, wkt = 1)
filename <- paste0("~/Research/Datasets/LA_50K_T",time_instant,".tsv")
points = read_tsv(filename, col_names = c("oid", "x", "y", "tid")) 


p = ggplot(geom) + 
  geom_sf(aes(fill = d1)) + 
  geom_point(data = points, aes(x = x, y = y), size = 0.1, alpha = 0.1) +   
  scale_fill_gradient(low = "#FFFFFF", high ="#FF0000") +
  xlab("") + ylab("") +
  theme_minimal() +
  theme(axis.text = element_blank()) 

plot(p)
