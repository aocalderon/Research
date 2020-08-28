library(tidyverse)
library(igraph)

from = c(0,0,0,0,1,1,3)
to   = c(1,2,3,4,2,3,4)
edges = as_tibble(data.frame(from = from, to = to))

