library(tidyverse)

fields1 <- c("ts", "epoch", "host", "tag", "n1", "appId", "n2", "dataset", "epsilon", "mu", "delta", "method", "partitions", "sdist", "step", "stage", "time")
pflock2 <- enframe(read_lines("pflock2.txt"), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  separate(col = line, into = fields1, sep = "\\|") |>
  mutate(time = as.numeric(time)) |>
  select(epsilon, stage, time) |>
  group_by(epsilon, stage) |> summarise(time = mean(time))

p = ggplot(pflock2, aes(x = epsilon, y = time, fill = stage)) + 
  geom_col(width = 0.7, position="dodge") + 
  labs(x="Epsilon (m)", y="Time (s)") 
plot(p)

W = 8
H = 6
ggsave(paste0("pflock2_time.pdf"), width = W, height = H)

fields2 <- c("ts", "epoch", "host", "tag", "n1", "appId", "n2", "dataset", "epsilon", "mu", "delta", "method", "partitions", "sdist", "step", "stage", "n")
pflock2_info <- enframe(read_lines("pflock2.txt"), value = "line") |>
  filter(str_detect(line, 'INFO')) |>
  filter(str_detect(line, 'Safe') | str_detect(line, 'Partial') ) |>
  separate(col = line, into = fields2, sep = "\\|") |>
  mutate(n = as.numeric(n)) |>
  select(epsilon, stage, n) |> distinct()
p = ggplot(pflock2_info, aes(x = epsilon, y = n, fill = stage)) + 
  geom_col(width = 0.7, position="dodge") + 
  labs(x="Epsilon (m)", y="Number of flocks") 
plot(p)  
ggsave(paste0("pflock2_info.pdf"), width = W, height = H)

fields1 <- c("ts", "epoch", "host", "tag", "n1", "appId", "n2", "dataset", "epsilon", "mu", "delta", "method", "partitions", "sdist", "step", "stage", "time")
pflock3 <- enframe(read_lines("pflock3.txt"), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  separate(col = line, into = fields1, sep = "\\|") |>
  mutate(time = as.numeric(time)) |>
  select(epsilon, stage, step, time)

pflock3_safe <- pflock3 |> filter(stage == "Safe") |> select(epsilon, stage, time) |>
  group_by(epsilon, stage) |> summarise(time = mean(time))
pflock3_step <- pflock3 |> filter(stage == "Partial") |> select(epsilon, step, time)
p = ggplot(pflock3_step, aes(x = epsilon, y = time, fill = step)) + 
  geom_col(width = 0.7, position="dodge") + 
  labs(x="Epsilon (m)", y="Time (s)") 
plot(p)  
ggsave(paste0("pflock3_time.pdf"), width = W, height = H)
