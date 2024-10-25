library(tidyverse)
library(latex2exp)

fields1 <- c("ts", "host", "tag", "cid", "appId", "n1", "dataset", "epsilon", "mu", "delta", "method", "capacity", "partitions", "sdist", "step", "index", "tag2", "stage", "time")

data <- enframe(read_lines("cells_in_la50K.txt"), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  separate(col = line, into = fields1, sep = "\\|") |>
  mutate(time = as.numeric(time), index = as.numeric(index)) |>
  select(index, appId, stage, time) 

dataSP1 <- data |> filter(stage == "SPartial1")
dataSP2 <- data |> filter(stage == "SPartial2")
dataSP <- dataSP1 |> bind_rows(dataSP2) |> group_by(index, appId) |> 
  summarise(time = sum(time)) |> ungroup() |> mutate(stage = "SPartial") |> 
  select(index, stage, time)

data2 <- data |> filter(stage != "SPartial1") |> filter(stage != "SPartial2") |>
  select(index, stage, time) |> bind_rows(dataSP)
  
  
data3 <- data2 |> group_by(index, stage) |> summarise(time = mean(time)) |> ungroup()

data4 <- data3 |> group_by(index) |> summarise(time = sum(time)) |> ungroup()

cells <- read_tsv("cubes_ids.tsv", col_names = c("cube_id", "cell_id", "time_id"))

data5 <- data4 |> left_join(cells, by = c("index" = "cube_id")) |> arrange(desc(time))
