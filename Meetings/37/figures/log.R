library(tidyverse)

fields1 <- c("ts", "host", "tag", "cid", "appId", "n", "dataset", "epsilon", "mu", "delta", "method", "tag2", "ncells", "step", "index", "stage", "time")
data1 <- enframe(read_lines("log.txt"), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  separate(col = line, into = fields1, sep = "\\|") |>
  mutate(time = as.numeric(time), index = as.numeric(index), step = as.numeric(step)) |>
  select(host, step, index, stage, time)

fields2 <- c("ts", "host", "tag", "cid", "appId", "n0", "dataset", "epsilon", "mu", "delta", "method", "tag2", "ncells", "step", "index", "stage", "n")
data2 <- enframe(read_lines("log.txt"), value = "line") |>
  filter(str_detect(line, 'INFO')) |>
  separate(col = line, into = fields2, sep = "\\|") |>
  mutate(n = as.numeric(n), index = as.numeric(index), step = as.numeric(step)) |>
  select(host, step, index, stage, n)

byPPartials <- data2 |> filter(stage == "Ppartial")
byPStill <- data2 |> filter(stage == "Spartial")
