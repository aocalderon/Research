library(tidyverse)

dataset = "cmbc_test2.txt"
fields = c("ts","start","tag","appId", "partition","epsilon","mu","delta","method","stage", "time")
data0 = enframe(read_lines(dataset), value="line")
data1 = data0 |>  filter(str_detect(line, 'TIME')) |>
  separate(line, into = fields, sep = "\\|") |>
  select(epsilon, method, stage, time) |>
  mutate(epsilon = as.numeric(epsilon), time = as.numeric(time)) |>
  group_by(epsilon, method, stage) |> summarise(time = mean(time)) |>
  group_by(epsilon, method) |> summarise(time = sum(time))

p = ggplot(data1, aes(x = as.factor(epsilon), y = time, fill = method)) + 
  geom_col(width = 0.7, position="dodge") + 
  labs(x="Epsilon", y="Time(s)")
plot(p)

W = 6
H = 4
#ggsave(paste0("bfe_by_time.pdf"), width = W, height = H)

data0 = enframe(read_lines(dataset), value="line")
data1 = data0 |>  filter(str_detect(line, 'TIME')) |>
  filter(!str_detect(line, 'BFE0')) |>
  separate(line, into = fields, sep = "\\|") |>
  select(epsilon, method, stage, time) |>
  filter(!str_detect(stage, 'Count')) |>
  filter(!str_detect(stage, 'Total')) |>
  mutate(epsilon = as.numeric(epsilon), time = as.numeric(time), stage = str_trim(stage)) |>
  #filter(time < 100) |>
  group_by(epsilon, method, stage) |> summarise(time = mean(time)) 
data1$stage <- factor(data1$stage,      # Reordering stage factor levels
                         levels = c("Count","Grid", "Read", "Cliques", "MBCs", "Pairs", "Centers", "Candidates", "Maximals"))

p = ggplot(data1, aes(x = as.factor(epsilon), y = time, fill = method)) + 
  geom_col(width = 0.7, position="dodge") + 
  labs(x="Epsilon", y="Time(s)") +
  facet_wrap(~ stage)
plot(p)

W = 6
H = 4
ggsave(paste0("cmbc_by_time_stage.pdf"), width = W, height = H)

fields2 = c("ts","start","tag","appId", "partition","epsilon","mu","delta","method","metric", "n")
data2 = enframe(read_lines(dataset), value="line") |> filter(str_detect(line, 'INFO')) |>
  filter(str_detect(line, 'Candidates')) |>
  separate(line, into = fields2, sep = "\\|") |>
  mutate(epsilon = as.numeric(epsilon), n = as.numeric(n)) |>
  select(epsilon, method, metric, n) |> distinct()

p = ggplot(data2, aes(x = as.factor(epsilon), y = n, fill = method)) + 
  geom_col(width = 0.7, position="dodge") + 
  labs(x="Epsilon", y="Number of candidates")
plot(p)

W = 6
H = 4
ggsave(paste0("cmbc_by_candidates.pdf"), width = W, height = H)

data3 = enframe(read_lines(dataset), value="line") |> filter(str_detect(line, 'INFO')) |>
  filter(str_detect(line, 'Points')) |>
  separate(line, into = fields2, sep = "\\|") |>
  mutate(epsilon = as.numeric(epsilon), n = as.numeric(n)) |>
  select(epsilon, method, metric, n) |> distinct() 

p = ggplot(data3, aes(x = as.factor(epsilon), y = n, fill = method)) + 
  geom_col(width = 0.7, position="dodge") + 
  labs(x="Epsilon", y="Number of points")
plot(p)

W = 6
H = 4
ggsave(paste0("cmbc_by_points.pdf"), width = W, height = H)
