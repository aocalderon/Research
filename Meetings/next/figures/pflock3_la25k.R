library(tidyverse)

fields1 <- c("ts", "epoch", "host", "tag", "n1", "appId", "n2", "dataset", "epsilon", "mu", "delta", "method", "partitions", "sdist", "step", "stage", "time")
pflock3 <- enframe(read_lines("pflock3_la25k.txt"), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  filter(str_detect(line, 'Partial')) |>
  separate(col = line, into = fields1, sep = "\\|") |>
  mutate(time = as.numeric(time), step = as.numeric(step)) |>
  select(partitions, step, time) |>
  mutate(partitions = recode(partitions,"481" = "500","1039" = "1000","2113" = "2000")) |>
  mutate(partitions = as.numeric(partitions)) |>
  group_by(partitions, step) |> summarise(time = mean(time))

p = ggplot(pflock3, aes(x = factor(partitions), y = time, fill = factor(step))) + 
  geom_col(width = 0.7, position="dodge") + 
  labs(x="Number of cells", y="Time (s)") 
plot(p)

W = 8
H = 6
ggsave(paste0("la25k_steps.pdf"), width = W, height = H)
