library(tidyverse)

fields1 <- c("ts", "epoch", "host", "tag", "n1", "appId", "n2", "dataset", "epsilon", "mu", "delta", "method", "capacity", "partitions", "sdist", "step", "stage", "time")
pflock2 <- enframe(read_lines("pflock2_la50k.txt"), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  separate(col = line, into = fields1, sep = "\\|") |>
  mutate(time = as.numeric(time), epsilon = as.numeric(epsilon)) |>
  select(epsilon, capacity, partitions, stage, time) |>
  mutate(partitions = recode(capacity,"1000" = "600","1500" = "400","2000" = "300","2500" = "250","3000" = "200","5000" = "100","10000" = "50","20000" = "25")) |>
  mutate(partitions = as.numeric(partitions)) |>
  select(epsilon, partitions, stage, time) |>
  group_by(epsilon, partitions, stage) |> summarise(time = mean(time))

p = ggplot(pflock2, aes(x = factor(epsilon), y = time, fill = stage)) + 
  geom_col(width = 0.7, position="dodge") + 
  facet_grid(~partitions) +
  labs(x="Epsilon (m)", y="Time (s)") 
plot(p)

W = 8
H = 6
ggsave(paste0("la50k_time.pdf"), width = W, height = H)

LA50K_E10 <- pflock2 |> filter(epsilon == 10)
p = ggplot(LA50K_E10, aes(x = factor(partitions), y = time, fill = stage)) + 
  geom_col(width = 0.7, position="dodge") + 
  labs(x="Number of cells", y="Time (s)") 
plot(p)
ggsave(paste0("la50kE10_partitions.pdf"), width = W, height = H)

LA50K_E15 <- pflock2 |> filter(epsilon == 15)
p = ggplot(LA50K_E15, aes(x = factor(partitions), y = time, fill = stage)) + 
  geom_col(width = 0.7, position="dodge") + 
  labs(x="Number of cells", y="Time (s)") 
plot(p)
ggsave(paste0("la50kE15_partitions.pdf"), width = W, height = H)

LA50K_E20 <- pflock2 |> filter(epsilon == 20)
p = ggplot(LA50K_E20, aes(x = factor(partitions), y = time, fill = stage)) + 
  geom_col(width = 0.7, position="dodge") + 
  labs(x="Number of cells", y="Time (s)") 
plot(p)
ggsave(paste0("la50kE20_partitions.pdf"), width = W, height = H)
