library(tidyverse)

fields1 <- c("ts", "epoch", "host", "tag", "n1", "appId", "n2", "dataset", "epsilon", "mu", "delta", "method", "capacity", "partitions", "sdist", "step", "stage", "n")
pflock2 <- enframe(read_lines("experiments.txt"), value = "line") |>
  filter(str_detect(line, 'LA_50K')) |>
  filter(str_detect(line, 'INFO')) |>
  filter(str_detect(line, 'npartials')) |>
  separate(col = line, into = fields1, sep = "\\|") |>
  mutate(n = as.numeric(n), epsilon = as.numeric(epsilon)) |>
  select(epsilon, capacity, partitions, stage, n) |>
  mutate(partitions = recode(capacity,"1000" = "600","1500" = "400","2000" = "300","2500" = "250","3000" = "200","5000" = "100","10000" = "50","20000" = "25")) |>
  mutate(partitions = as.numeric(partitions)) |>
  select(epsilon, partitions, stage, n) |>
  mutate(partitions = as.numeric(partitions)) |>
  group_by(epsilon, partitions) |> summarise(n = max(n))

p = ggplot(pflock2, aes(x = factor(partitions), y = n)) + 
  geom_col(width = 0.7, position="dodge") + 
  facet_grid(~epsilon) +
  labs(x="Number of cells", y="Number of partial flocks") 
plot(p)

W = 12
H = 6
ggsave(paste0("la50k_npartials.pdf"), width = W, height = H)
