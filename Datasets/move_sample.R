library(tidyverse)

points <- read_tsv("sample_dense.tsv", col_names = F)

summary(points)

points2 <- points |> mutate(X5 = X2 - 1982031, X6 = X3 - 561956) |>
  select(X1, X5, X6, X4)

points2 |> write_tsv("sample_dense2.tsv", col_names = F)
