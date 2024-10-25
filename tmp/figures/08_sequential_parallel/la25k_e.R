library(tidyverse)
library(latex2exp)

fields1 <- c("ts", "epoch", "host", "tag", "n1", "appId", "n2", "dataset", "epsilon", "mu", "delta", "method", "capacity", "partitions", "sdist", "step", "stage", "time")

cubes <- enframe(read_lines("e/pflock6_la25k_e.txt"), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  filter(str_detect(line, 'Total')) |>
  separate(col = line, into = fields1, sep = "\\|") |>
  mutate(time = as.numeric(time), epsilon = as.numeric(epsilon)) |>
  select(epsilon, time) |>
  group_by(epsilon) |> summarise(time = mean(time)) |> ungroup() |>
  mutate(method = "Cube-based")

lca <- enframe(read_lines("e/pflock4_la25k_e.txt"), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  filter(str_detect(line, 'Total')) |>
  separate(col = line, into = fields1, sep = "\\|") |>
  mutate(time = as.numeric(time), epsilon = as.numeric(epsilon)) |>
  select(epsilon, time) |>
  group_by(epsilon) |> summarise(time = mean(time)) |> ungroup() |>
  mutate(method = "LCA")

level <- enframe(read_lines("e/pflock3_la25k_e.txt"), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  filter(str_detect(line, 'Total')) |>
  separate(col = line, into = fields1, sep = "\\|") |>
  mutate(time = as.numeric(time), epsilon = as.numeric(epsilon)) |>
  select(epsilon, time) |>
  group_by(epsilon) |> summarise(time = mean(time)) |> ungroup() |>
  mutate(method = "By-Level")

master <- enframe(read_lines("e/pflock2_la25k_e.txt"), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  filter(str_detect(line, 'Total')) |>
  separate(col = line, into = fields1, sep = "\\|") |>
  mutate(time = as.numeric(time), epsilon = as.numeric(epsilon)) |>
  filter(time < 500) |>
  select(epsilon, time) |>
  group_by(epsilon) |> summarise(time = mean(time)) |> ungroup() |>
  mutate(method = "Master")

data <- cubes |> bind_rows(lca) |> bind_rows(level) |> bind_rows(master)

p = ggplot(data, aes(x = factor(epsilon), y = time, group = method)) + 
  geom_line(aes(linetype = method, color = method)) + 
  geom_point(aes(shape = method, color = method), size = 3) +
  labs(x=TeX("$\\epsilon(m)$"), y="Time(s)") +
  scale_color_discrete("Method") +
  scale_shape_discrete("Method") +
  guides(linetype = "none") +
  theme_bw()
plot(p)

W = 6
H = 4
ggsave(paste0("la25k_e.pdf"), width = W, height = H)

