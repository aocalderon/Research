library(tidyverse)

fields1 <- c("ts", "epoch", "host", "tag", "n1", "appId", "n2", "dataset", "epsilon", "mu", "delta", "method", "capacity", "partitions", "sdist", "step", "stage", "time")

pflock4 <- enframe(read_lines("pflock4_la25k.txt"), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  separate(col = line, into = fields1, sep = "\\|") |>
  mutate(time = as.numeric(time), epsilon = as.numeric(epsilon), capacity = as.numeric(capacity)) |>
  select(epsilon, capacity, partitions, stage, time) |>
  filter(capacity >= 2300 & capacity <= 5000 ) |> 
  mutate(partitions = recode(capacity,"2300" = "400","3000" = "300","5000" = "200")) |>
  mutate(partitions = as.numeric(partitions)) |>
  select(epsilon, partitions, stage, time) |>
  group_by(epsilon, partitions, stage) |> summarise(time = mean(time)) |> ungroup() |>
  filter(epsilon == 30) |>
  filter(stage == "Partial") |> 
  mutate(step = -1) |>
  select(partitions, step, time) 

pflock3 <- enframe(read_lines("pflock3_la25k.txt"), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  filter(str_detect(line, 'Partial')) |>
  separate(col = line, into = fields1, sep = "\\|") |>
  mutate(time = as.numeric(time), step = as.numeric(step)) |>
  select(capacity, partitions, step, time) |>
  mutate(partitions = recode(capacity,"2300" = "400","3000" = "300","5000" = "200")) |>
  mutate(partitions = as.numeric(partitions)) |>
  group_by(partitions, step) |> summarise(time = mean(time)) |> ungroup() |>
  select(partitions, step, time)

pflock2 <- enframe(read_lines("pflock2_la25k.txt"), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  separate(col = line, into = fields1, sep = "\\|") |>
  mutate(time = as.numeric(time), epsilon = as.numeric(epsilon), capacity = as.numeric(capacity)) |>
  select(epsilon, capacity, partitions, stage, time) |>
  filter(capacity >= 2300 & capacity <= 5000 ) |> 
  mutate(partitions = recode(capacity,"2300" = "400","3000" = "300","5000" = "200")) |>
  mutate(partitions = as.numeric(partitions)) |>
  select(epsilon, partitions, stage, time) |>
  group_by(epsilon, partitions, stage) |> summarise(time = mean(time)) |> ungroup() |>
  filter(epsilon == 30) |>
  filter(stage == "Partial") |> 
  mutate(step = 0) |>
  select(partitions, step, time) 

data <- pflock2 |> bind_rows(pflock3) |> bind_rows(pflock4) |> filter(step < 6) |>
  mutate(step = recode(step, "-1" = "MCA", "0" = "Master", "1" = "By 1", "2" = "By 2", "3" = "By 3", "4" = "By 4", "5" = "By 5"))
data$step <- factor(data$step, levels = c("Master", "By 1", "By 2", "By 3", "By 4", "By 5", "MCA"))

p = ggplot(data, aes(x = factor(partitions), y = time, fill = factor(step))) + 
  geom_col(width = 0.7, position="dodge") + 
  labs(x="Number of cells", y="Time (s)") + 
  guides(fill=guide_legend(title="Steps"))
plot(p)

W = 8
H = 6
ggsave(paste0("la25k_steps.pdf"), width = W, height = H)
