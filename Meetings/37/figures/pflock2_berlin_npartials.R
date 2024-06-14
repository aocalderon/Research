library(tidyverse)

fields1 <- c("ts", "epoch", "host", "tag", "n1", "appId", "n2", "dataset", "epsilon", "mu", "delta", "method", "partitions", "sdist", "step", "stage", "n")
pflock2 <- enframe(read_lines("pflock2_berlin.txt"), value = "line") |>
  filter(str_detect(line, 'INFO')) |>
  filter(str_detect(line, 'npartials')) |>
  separate(col = line, into = fields1, sep = "\\|") |>
  mutate(n = as.numeric(n), epsilon = as.numeric(epsilon)) |>
  select(epsilon, partitions, n) |>
  mutate(partitions = recode(partitions,"46" = "50","94" = "100","97" = "100","238" = "250","241" = "250","478" = "500","481" = "500",
                             "943" = "1000","946" = "1000","2479" = "2500","2503" = "2500")) |>
  mutate(partitions = as.numeric(partitions)) |>
  group_by(epsilon, partitions) |> summarise(n = max(n))

p = ggplot(pflock2, aes(x = factor(partitions), y = n)) + 
  geom_col(width = 0.7, position="dodge") + 
  facet_grid(~epsilon) +
  labs(x="Number of cells", y="Number of partial flocks") 
plot(p)

W = 8
H = 6
ggsave(paste0("berlin_npartials.pdf"), width = W, height = H)
