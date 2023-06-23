library(tidyverse)
library(xtable)

fieldsTimes = c("ts","start","tag", "cellId", "appId","partitions","dataset","epsilon","mu","delta","project","stage","time")
perfs = enframe(read_lines( "density.out" ), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  separate(col = line, into = fieldsTimes, sep = "\\|") |>
  select(appId, stage, time)

fieldsParams = c("ts","start","tag", "appId","key", "value")
params = enframe(read_lines( "density.out" ), value = "line") |>
  filter(str_detect(line, 'PARAMS')) |>
  separate(col = line, into = fieldsParams, sep = "\\|") |>
  select(appId, key, value) |>
  filter(key == "density") |>
  pivot_wider(names_from = key, values_from = value)

data = perfs |> left_join(params, by = "appId") |>
  filter(!str_detect(stage, "Read")) |>
  mutate(density = as.numeric(density), time = as.numeric(time),
         stage = as.factor(stage)) |>
  select(density, stage, time) |>
  group_by(density, stage) |> summarise(time = mean(time)) 
  
p = ggplot(data, aes(x = as.factor(density), y = time, fill = stage)) + 
  geom_col(position = "dodge") +
  labs(x="Density (pairs per cell)", y="Time (s)") + 
  theme_bw()
plot(p)

W = 6
H = 4
ggsave(paste0("densityByStage.pdf"), width = W, height = H)
print(xtable(data, digits = c(0,0,0,2)), booktabs = TRUE, include.rownames=FALSE)

data = data |> group_by(density) |> summarise(time = sum(time))
p = ggplot(data, aes(x = as.factor(density), y = time)) + 
  geom_col(position = "dodge") +
  labs(x="Density (pairs per cell)", y="Time (s)") + 
  theme_bw()
plot(p)

ggsave(paste0("density.pdf"), width = W, height = H)
