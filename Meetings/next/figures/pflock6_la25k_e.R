library(tidyverse)

fields1 <- c("ts", "epoch", "host", "tag", "n1", "appId", "n2", "dataset", "epsilon", "mu", "delta", "method", "capacity", "partitions", "sdist", "step", "stage", "time")

pflock6 <- enframe(read_lines("pflock6_la25_e.txt"), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  filter(str_detect(line, 'Total')) |>
  separate(col = line, into = fields1, sep = "\\|") |>
  mutate(time = as.numeric(time), epsilon = as.numeric(epsilon)) |>
  select(epsilon, time) |>
  group_by(epsilon) |> summarise(time = mean(time)) |> ungroup() 

e30 <- enframe(read_lines("pflock6_la25k_SvC.txt"), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  filter(str_detect(line, 'Total')) |>
  separate(col = line, into = fields1, sep = "\\|") |>
  mutate(time = as.numeric(time), epsilon = as.numeric(epsilon)) |>
  filter(capacity == 4750) |>
  filter(step == 6) |>
  select(epsilon, time) |>
  group_by(epsilon) |> summarise(time = mean(time)) |> ungroup() 

data <- pflock6 |> bind_rows(e30)

p = ggplot(data, aes(x = factor(epsilon), y = time)) + 
  geom_col(width = 0.7, position="dodge") + 
  labs(x="Epsilon (m)", y="Time (s)") 
plot(p)

W = 8
H = 6
ggsave(paste0("pflock6_la25k_e.pdf"), width = W, height = H)
