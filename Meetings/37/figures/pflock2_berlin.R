library(tidyverse)

fields1 <- c("ts", "epoch", "host", "tag", "n1", "appId", "n2", "dataset", "epsilon", "mu", "delta", "method", "partitions", "sdist", "step", "stage", "time")
pflock2 <- enframe(read_lines("pflock2_berlin.txt"), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  separate(col = line, into = fields1, sep = "\\|") |>
  mutate(time = as.numeric(time), epsilon = as.numeric(epsilon)) |>
  select(epsilon, partitions, stage, time) |>
  mutate(partitions = recode(partitions,"46" = "100","94" = "50","97" = "50","238" = "250","241" = "250","478" = "500","481" = "500",
                             "943" = "1000","946" = "1000","2479" = "2500","2503" = "2500")) |>
  mutate(partitions = as.numeric(partitions)) |>
  group_by(epsilon, partitions, stage) |> summarise(time = mean(time))

p = ggplot(pflock2, aes(x = factor(epsilon), y = time, fill = stage)) + 
  geom_col(width = 0.7, position="dodge") + 
  facet_grid(~partitions) +
  labs(x="Epsilon (m)", y="Time (s)") 
plot(p)

W = 8
H = 6
ggsave(paste0("berlin_time.pdf"), width = W, height = H)

Berlin_E50 <- pflock2 |> filter(epsilon == 50)
p = ggplot(Berlin_E50, aes(x = factor(partitions), y = time, fill = stage)) + 
  geom_col(width = 0.7, position="dodge") + 
  labs(x="Number of cells", y="Time (s)") 
plot(p)
ggsave(paste0("berlinE50_partitions.pdf"), width = W, height = H)

Berlin_E40 <- pflock2 |> filter(epsilon == 40)
p = ggplot(Berlin_E40, aes(x = factor(partitions), y = time, fill = stage)) + 
  geom_col(width = 0.7, position="dodge") + 
  labs(x="Number of cells", y="Time (s)") 
plot(p)
ggsave(paste0("berlinE40_partitions.pdf"), width = W, height = H)

Berlin_E30 <- pflock2 |> filter(epsilon == 30)
p = ggplot(Berlin_E30, aes(x = factor(partitions), y = time, fill = stage)) + 
  geom_col(width = 0.7, position="dodge") + 
  labs(x="Number of cells", y="Time (s)") 
plot(p)
ggsave(paste0("berlinE30_partitions.pdf"), width = W, height = H)
