library(tidyverse)
library(latex2exp)

fields1 <- c("ts", "epoch", "host", "tag", "n1", "appId", "n2", "dataset", "epsilon", "mu", "delta", "method", "capacity", "partitions", "sdist", "step", "stage", "time")

cubes <- enframe(read_lines("la50K.txt"), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  separate(col = line, into = fields1, sep = "\\|") |>
  filter(stage != "Total") |>
  filter(stage != "Part") |>
  mutate(time = as.numeric(time), epsilon = as.numeric(epsilon)) |>
  select(stage, epsilon, time) |>
  group_by(stage, epsilon) |> summarise(time = mean(time)) |> ungroup() |>
  mutate(stage = recode(stage, "parPrune" = "Pruning", "SPartial" = "Partials in Space", "TPartial" = "Partials in Time"))

p = ggplot(cubes, aes(x = factor(epsilon), y = time, fill = stage)) + 
  geom_col(width = 0.7, position="dodge") + 
  labs(x="Epsilon (m)", y="Time (s)") 
plot(p)

W = 8
H = 6
ggsave(paste0("la50k.pdf"), width = W, height = H)