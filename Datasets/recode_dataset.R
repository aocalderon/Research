library(tidyverse)

input  = "/home/and/Research/Datasets/dense.tsv"
output = strsplit(input, "\\.")[[1]][1]
points <- read_tsv(input, col_names = F)

oids <- points |> select(X1) |> distinct() |> arrange(X1) |> mutate(id = row_number())
min_x = min(points$X2)
min_y = min(points$X3)

points |> 
  mutate(X5 = X2 - min_x, X6 = X3 - min_y) |>
  select(X1, X5, X6, X4) |> 
  left_join(oids, by = c("X1")) |> 
  select(id, X5, X6, X4) |> 
  write_tsv(paste0(output, "_recode.tsv"), col_names = F)
