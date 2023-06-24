library(tidyverse)
library(xtable)

fieldsTimes = c("ts","start","tag", "cellId", "appId","partitions","dataset","epsilon","mu","delta","project","stage","time")
perfs = enframe(read_lines( "capacity.out" ), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  separate(col = line, into = fieldsTimes, sep = "\\|") |>
  select(appId, stage, time)

fieldsParams = c("ts","start","tag", "appId","key", "value")
params = enframe(read_lines( "capacity.out" ), value = "line") |>
  filter(str_detect(line, 'PARAMS')) |>
  separate(col = line, into = fieldsParams, sep = "\\|") |>
  select(appId, key, value) |>
  filter(key == "capacity") |>
  pivot_wider(names_from = key, values_from = value)

data = perfs |> left_join(params, by = "appId") |>
  filter(!str_detect(stage, "Read")) |>
  filter(!str_detect(stage, "Stats")) |>
  filter(!str_detect(stage, "Index1")) |>
  filter(!str_detect(stage, "Shuffle2")) |>
  mutate(capacity = as.numeric(capacity), time = as.numeric(time),
         stage = as.factor(stage)) |>
  select(capacity, stage, time) |>
  mutate(stage = recode(stage, Index2 = "Index", Shuffle1 = "Shuffle")) |>
  group_by(capacity, stage) |> summarise(time = mean(time)) |>
  filter(capacity <= 500)
  
p = ggplot(data, aes(x = as.factor(capacity), y = time, fill = stage)) + 
  geom_col(position = "dodge") +
  labs(x="Capacity (points per node)", y="Time (s)") + 
  theme_bw()
plot(p)

W = 6
H = 4
ggsave(paste0("capacityByStage.pdf"), width = W, height = H)
print(xtable(data, digits = c(0,0,0,2)), booktabs = TRUE, include.rownames=FALSE)

data = data |> group_by(capacity) |> summarise(time = sum(time))
p = ggplot(data, aes(x = as.factor(capacity), y = time)) + 
  geom_col(position = "dodge") +
  labs(x="Capacity (points per node)", y="Time (s)") + 
  theme_bw()
plot(p)

ggsave(paste0("capacity.pdf"), width = W, height = H)
