library(tidyverse)

fields1 <- c("ts", "epoch", "host", "tag", "n1", "appId", "n2", "dataset", "epsilon", "mu", "delta", "method", "capacity", "partitions", "sdist", "step", "stage", "time")

pflock6 <- enframe(read_lines("pflock6_la25k_SvC.txt"), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  filter(str_detect(line, 'Total')) |>
  separate(col = line, into = fields1, sep = "\\|") |>
  mutate(time = as.numeric(time), step = as.numeric(step), capacity = as.numeric(capacity)) |>
  filter(capacity >= 2300 & capacity <= 4750) |>
  select(capacity, partitions, step, time) |>
  mutate(partitions = recode(partitions, "391" = "400", "295" = "300", "202" = "200")) |>
  mutate(partitions = as.numeric(partitions)) |>
  filter(step == 10) |>
  select(partitions, time) |>
  group_by(partitions) |> summarise(time = mean(time)) |> ungroup() |>
  mutate(variant = "Cubes")

pflock4 <- enframe(read_lines("pflock4_la25k.txt"), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  filter(str_detect(line, 'Total')) |>
  separate(col = line, into = fields1, sep = "\\|") |>
  mutate(time = as.numeric(time), epsilon = as.numeric(epsilon), capacity = as.numeric(capacity)) |>
  filter(epsilon == 30) |>
  select(capacity, partitions, time) |>
  filter(capacity >= 2300 & capacity <= 5000 ) |> 
  mutate(partitions = recode(capacity,"2300" = "400","3000" = "300","5000" = "200")) |>
  mutate(partitions = as.numeric(partitions)) |>
  select(partitions, time) |>
  group_by(partitions) |> summarise(time = mean(time)) |> ungroup() |>
  mutate(variant = "LCA") 

pflock3 <- enframe(read_lines("pflock3_la25k.txt"), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  filter(str_detect(line, 'Total')) |>
  separate(col = line, into = fields1, sep = "\\|") |>
  mutate(time = as.numeric(time), epsilon = as.numeric(epsilon), capacity = as.numeric(capacity)) |>
  filter(epsilon == 30) |>
  filter(step == 2) |>
  select(capacity, partitions, time) |>
  filter(capacity >= 2300 & capacity <= 5000 ) |> 
  mutate(partitions = recode(capacity,"2300" = "400","3000" = "300","5000" = "200")) |>
  mutate(partitions = as.numeric(partitions)) |>
  select(partitions, time) |>
  group_by(partitions) |> summarise(time = mean(time)) |> ungroup() |>
  mutate(variant = "By Level") 

pflock2 <- enframe(read_lines("pflock2_la25k.txt"), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  filter(str_detect(line, 'Total')) |>
  separate(col = line, into = fields1, sep = "\\|") |>
  mutate(time = as.numeric(time), epsilon = as.numeric(epsilon), capacity = as.numeric(capacity)) |>
  filter(epsilon == 30) |>
  select(capacity, partitions, time) |>
  filter(capacity >= 2300 & capacity <= 5000 ) |> 
  mutate(partitions = recode(capacity,"2300" = "400","3000" = "300","5000" = "200")) |>
  mutate(partitions = as.numeric(partitions)) |>
  select(partitions, time) |>
  group_by(partitions) |> summarise(time = mean(time)) |> ungroup() |>
  mutate(variant = "Master") 

data <- pflock6 |> bind_rows(pflock4) |> bind_rows(pflock3) |> bind_rows(pflock2)
data$variant <- factor(data$variant, levels = c("Master", "By Level", "LCA", "Cubes"))
p = ggplot(data, aes(x = factor(partitions), y = time, fill = factor(variant))) + 
  geom_col(width = 0.7, position="dodge") + 
  labs(x="Number of cells", y="Time (s)") + 
  guides(fill=guide_legend(title="Variant"))
plot(p)

W = 8
H = 6
ggsave(paste0("la25k_variants.pdf"), width = W, height = H)
