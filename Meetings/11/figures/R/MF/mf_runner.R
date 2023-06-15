library(tidyverse)

data = enframe(read_lines("mf_runner1.txt"), value="line")

fieldsParams = c("tm", "start", "tag", "appId", "param", "value")
params = data |> filter(str_detect(line, 'PARAMS')) |>
  separate(line, into = fieldsParams, sep = "\\|") |>
  select(appId, param, value)

fieldsTimes = c("ts","start","tag","appId", "partitions","epsilon","mu","delta","method","stage", "time")
times = data |>  filter(str_detect(line, 'TIME')) |>
  separate(line, into = fieldsTimes, sep = "\\|") |>
  select(appId, epsilon, partitions, method, stage, time) |>
  mutate(epsilon = as.numeric(epsilon), partitions = as.numeric(partitions), 
         time = as.numeric(time), stage = str_trim(stage)) |>
  left_join(params, by = c("appId"), multiple = "all") |>
  filter(param == "capacity") |> rename(capacity = value) |> 
  #filter(stage == "Run") |> 
  select(epsilon, partitions, capacity, time) |>
  mutate(capacity = as.numeric(capacity))

mf = times |> select(epsilon, capacity, partitions, time) |>
  group_by(epsilon, capacity, partitions) |> summarise(time = mean(time)) |>
  filter(partitions != 1)   # remove sequential

W = 6
H = 4
for(e in as.numeric(levels(as.factor(mf$epsilon)))) {
  d = mf |> filter(epsilon == e) 
  p = ggplot(d, aes(x = as.factor(partitions), y = time)) + 
    geom_col(width = 0.7, position="dodge") + 
    labs(x="Number of partitions", y="Time(s)", title = paste("Epsilon=",e))
  plot(p)
  ggsave(paste0("pflockE", e,"_by_partitions.pdf"), width = W, height = H)
  
  g = ggplot(d, aes(x = as.factor(capacity), y = time)) + 
    geom_col(width = 0.7, position="dodge") + 
    labs(x="Capacity", y="Time(s)", title = paste("Epsilon=",e))
  plot(g)
  ggsave(paste0("pflockE", e,"_by_capacity.pdf"), width = W, height = H)
}

bestMF = mf |> group_by(epsilon) |> summarise(time = min(time)) |> 
  add_column(method = "PFlock") |> select(epsilon, method, time)

fields = c("ts","start","tag","appId","epsilon","mu","delta","method","capacity","stage", "time")
bfe = enframe(read_lines("mf_test.txt"), value="line") |>  
  filter(str_detect(line, 'TIME')) |>
  separate(line, into = fields, sep = "\\|") |>
  select(epsilon, method, stage, time) |>
  mutate(epsilon = as.numeric(epsilon), time = as.numeric(time), 
         stage = str_trim(stage)) |> 
  filter(method == "BFE" & stage == "Total") |>
  select(epsilon, time, method) |>
  group_by(epsilon, method) |> summarise(time = mean(time))

data = bfe |> bind_rows(bestMF)

g = ggplot(data, aes(x = as.factor(epsilon), y = time, fill = method)) + 
  geom_col(width = 0.7, position="dodge") + 
  labs(x="Epsilon(m)", y="Time(s)")
plot(g)
ggsave(paste0("PFlockVsBFE2.pdf"), width = W, height = H)
