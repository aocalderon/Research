library(tidyverse)

fields1 <- c("ts", "epoch", "host", "tag", "n1", "appId", "n2", "dataset", "epsilon", "mu", "delta", "method", "partitions", "sdist", "step", "stage", "time")
pflock2 <- enframe(read_lines("pflock2_la25k.txt"), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  separate(col = line, into = fields1, sep = "\\|") |>
  mutate(time = as.numeric(time), epsilon = as.numeric(epsilon)) |>
  select(epsilon, partitions, stage, time) |>
  mutate(partitions = recode(partitions,"49" = "100","94" = "100","97" = "100","187" = "200","478" = "500","481" = "500","484" = "500",
                            "1033" = "1000","1036" = "1000","1039" = "1000","2113" = "2000","4207" = "4000","4228" = "4000","4270" = "4000")) |>
  mutate(partitions = as.numeric(partitions)) |>
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
ggsave(paste0("la25k_partitions.pdf"), width = W, height = H)
