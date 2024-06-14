library(tidyverse)

fields1 <- c("ts", "epoch", "host", "tag", "n1", "appId", "n2", "dataset", "epsilon", "mu", "delta", "method", "capacity", "partitions", "sdist", "step", "stage", "time")

pflock6 <- enframe(read_lines("pflock6_la50k_e.txt"), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  filter(str_detect(line, 'Total')) |>
  separate(col = line, into = fields1, sep = "\\|") |>
  mutate(time = as.numeric(time), epsilon = as.numeric(epsilon)) |>
  select(epsilon, time) |>
  group_by(epsilon) |> summarise(time = mean(time)) |> ungroup() 

p = ggplot(pflock6, aes(x = factor(epsilon), y = time)) + 
  geom_col(width = 0.7, position="dodge") + 
  labs(x="Epsilon (m)", y="Time (s)") 
plot(p)

W = 8
H = 6
ggsave(paste0("pflock6_la50k_e.pdf"), width = W, height = H)
