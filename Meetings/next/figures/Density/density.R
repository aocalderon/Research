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
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
=======
>>>>>>> b555d93a52051f78fad79a02a478e54243a06b8e
  mutate(time = as.numeric(time), stage = as.factor(stage), 
         density = as.factor(density)) |>
  mutate(density = recode(density, "0" = "NA")) |>
  select(density, stage, time) |>
  filter(density != 1000 & density != 1500) |>
  group_by(density, stage) |> summarise(time = mean(time))

data$stage <- factor(data$stage, levels = c("Index1","Shuffle1","Stats","Index2","Shuffle2","Run")) 
dataByStage = data |> mutate(time = case_when(density=="NA"&stage=="Stats" ~ 0, 
                                density=="NA"&stage=="Shuffle2" ~ 0, 
                                density=="NA"&stage=="Index2" ~ 0, 
                                TRUE ~ time))
p = ggplot(dataByStage, aes(x = as.factor(density), y = time, fill = stage)) + 
  geom_col(position = "dodge") +
  scale_fill_discrete("Stage") +
  labs(x="Threshold (pairs per cell)", y="Time (s)") + 
  theme_bw()
plot(p)

W = 8
H = 4
ggsave(paste0("densityByStage.pdf"), width = W, height = H)

print(xtable(dataByStage, digits = c(0,0,0,2)), booktabs = TRUE, include.rownames=FALSE)
<<<<<<< HEAD
>>>>>>> b555d93a52051f78fad79a02a478e54243a06b8e
=======
>>>>>>> b555d93a52051f78fad79a02a478e54243a06b8e

data = data |> group_by(density) |> summarise(time = sum(time))
p = ggplot(data, aes(x = as.factor(density), y = time)) + 
  geom_col(position = "dodge") +
<<<<<<< HEAD
<<<<<<< HEAD
  labs(x="Density (pairs per cell)", y="Time (s)") + 
=======
  labs(x="Threshold (pairs per cell)", y="Time (s)") + 
>>>>>>> b555d93a52051f78fad79a02a478e54243a06b8e
=======
  labs(x="Threshold (pairs per cell)", y="Time (s)") + 
>>>>>>> b555d93a52051f78fad79a02a478e54243a06b8e
  theme_bw()
plot(p)

ggsave(paste0("density.pdf"), width = W, height = H)
