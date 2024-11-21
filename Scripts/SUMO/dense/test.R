library(tidyverse)

data <- read_tsv("dense_14K_crowded.tsv", col_names = c("i", "x", "y", "t"))

data |> group_by(x, y) |> summarise(oid = max(i)) |> mutate(t = 60) |> 
  select(oid, x, y, t) |> write_tsv("dense_14K_crowded_prime.tsv", col_names = F)

