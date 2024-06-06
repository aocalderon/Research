library(tidyverse)

fields1 <- c("ts", "epoch", "host", "tag", "n1", "appId", "n2", "dataset", "epsilon", "mu", "delta", "method", "capacity", "partitions", "sdist", "step", "stage", "time")
pflock2 <- enframe(read_lines("pflock2_la25k.txt"), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  separate(col = line, into = fields1, sep = "\\|") |>
  mutate(time = as.numeric(time), epsilon = as.numeric(epsilon)) |>
  select(epsilon, capacity, partitions, stage, time) |>
  mutate(partitions = recode(capacity,"400" = "2500","500" = "2000","700" = "1500","1000" = "1000","1300" = "750","2000" = "500","2300" = "400","3000" = "300","5000" = "200","10000" = "100","20000" = "50")) |>
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
ggsave(paste0("la25k_time.pdf"), width = W, height = H)

LA25K_E20 <- pflock2 |> filter(epsilon == 20)
p = ggplot(LA25K_E20, aes(x = factor(partitions), y = time, fill = stage)) + 
  geom_col(width = 0.7, position="dodge") + 
  labs(x="Number of cells", y="Time (s)") 
plot(p)
ggsave(paste0("la25kE20_partitions.pdf"), width = W, height = H)

LA25K_E25 <- pflock2 |> filter(epsilon == 25)
p = ggplot(LA25K_E25, aes(x = factor(partitions), y = time, fill = stage)) + 
  geom_col(width = 0.7, position="dodge") + 
  labs(x="Number of cells", y="Time (s)") 
plot(p)
ggsave(paste0("la25kE25_partitions.pdf"), width = W, height = H)

LA25K_E30 <- pflock2 |> filter(epsilon == 30)
p = ggplot(LA25K_E30, aes(x = factor(partitions), y = time, fill = stage)) + 
  geom_col(width = 0.7, position="dodge") + 
  labs(x="Number of cells", y="Time (s)") 
plot(p)
ggsave(paste0("la25kE30_partitions.pdf"), width = W, height = H)
