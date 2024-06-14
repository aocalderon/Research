library(tidyverse)

fields1 <- c("ts", "epoch", "host", "tag", "n1", "appId", "n2", "dataset", "epsilon", "mu", "delta", "method", "capacity", "partitions", "sdist", "step", "stage", "time")

pflock6 <- enframe(read_lines("pflock6_la25k.txt"), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  filter(str_detect(line, 'Total')) |>
  separate(col = line, into = fields1, sep = "\\|") |>
  mutate(time = as.numeric(time), step = as.numeric(step)) |>
  select(capacity, partitions, step, time) |>
  mutate(partitions = recode(capacity,"5000" = "200")) |>
  mutate(partitions = as.numeric(partitions)) |>
  group_by(partitions, step) |> summarise(time = mean(time)) |> ungroup() |>
  select(partitions, step, time)

p = ggplot(pflock6, aes(x = factor(step), y = time)) + 
  geom_col(width = 0.7, position="dodge") + 
  labs(x="Number of time slices", y="Time (s)") + 
  guides(fill=guide_legend(title="Steps"))
plot(p)

W = 8
H = 6
ggsave(paste0("pflock6_la25k_steps.pdf"), width = W, height = H)
