library(tidyverse)
library(knitr)
library(xtable)
library(sf)

data <- read_tsv("~/Research/Datasets/LA/cells.tsv", col_names = c("wkt", "cid", "area", "n1", "d1", "n2", "d2"))

CRS <- 6423
time_instant <- 320
geom <- st_as_sf(data, crs = CRS, wkt = 1)
filename <- paste0("~/Research/Datasets/LA_50K_T",time_instant,".tsv")
points = read_tsv(filename, col_names = c("oid", "x", "y", "tid")) 

################################################################################
p1 = ggplot(geom) + 
  geom_sf(aes(fill = n1)) + 
  coord_sf(datum = NA) +
  geom_point(data = points, aes(x = x, y = y), size = 0.1, alpha = 0.1) +   
  scale_fill_gradient(low = "#FFFFFF", high ="#FF0000", name = "Points per cell") +
  xlab("") + ylab("") +
  theme_minimal() +
  theme(axis.text = element_blank(), 
        legend.position="top",
        legend.key.height = unit(0.25, 'cm'),
        legend.key.width  = unit(0.75, 'cm'),
        legend.title      = element_text(size=5),
        legend.text       = element_text(size=5),
        legend.margin     = margin(10, 0, 0, 0),
        legend.box.margin = margin(0, 0, -20, 0)
  )
print(p1)

W = 8
H = 6
ggsave(paste0("points_per_cell.pdf"), width = W, height = H)

################################################################################
p2 = ggplot(geom) + 
  geom_sf(aes(fill = d1)) + 
  coord_sf(datum = NA) +
  geom_point(data = points, aes(x = x, y = y), size = 0.1, alpha = 0.1) +   
  scale_fill_gradient(low = "#FFFFFF", high ="#FF0000", name = "Density") +
  xlab("") + ylab("") +
  theme_minimal() +
  theme(axis.text = element_blank(), 
        legend.position="top",
        legend.key.height = unit(0.25, 'cm'),
        legend.key.width  = unit(0.75, 'cm'),
        legend.title      = element_text(size=5),
        legend.text       = element_text(size=5),
        legend.margin     = margin(10, 0, 0, 0),
        legend.box.margin = margin(0, 0, -20, 0)
  )
print(p2)
ggsave(paste0("density.pdf"), width = W, height = H)

################################################################################
npairs <- read_tsv("npairs.tsv", col_names = c("tinstance", "cid", "npairs"))

geom2 <- geom |> left_join(npairs |> filter(tinstance == time_instant), by = c("cid"))

p3 = ggplot(geom2) + 
  geom_sf(aes(fill = npairs)) + 
  coord_sf(datum = NA) +
  geom_point(data = points, aes(x = x, y = y), size = 0.1, alpha = 0.1) +   
  scale_fill_gradient(low = "#FFFFFF", high ="#FF0000", name = "Pairs per cell") +
  xlab("") + ylab("") +
  theme_minimal() +
  theme(axis.text = element_blank(), 
        legend.position="top",
        legend.key.height = unit(0.25, 'cm'),
        legend.key.width  = unit(0.75, 'cm'),
        legend.title      = element_text(size=5),
        legend.text       = element_text(size=5),
        legend.margin     = margin(10, 0, 0, 0),
        legend.box.margin = margin(0, 0, -20, 0)
  )
print(p3)
ggsave(paste0("pairs_per_cell.pdf"), width = W, height = H)
