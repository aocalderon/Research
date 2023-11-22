library(tidyverse)
library(latex2exp)

fields <- c("ts","start","host", "tag", "z", "appId","partitions","dataset","epsilon","mu","delta","method","stage","time")  
times <- enframe(read_lines( "uniform01_time.txt" ), value = "line") |>
  separate(col = line, into = fields, sep = "\\|") |>
  mutate(stage = str_trim(stage), epsilon = as.numeric(epsilon), 
         time = as.numeric(time), dataset = as.numeric(dataset)) |>
  select(appId, dataset, method, epsilon, time) 

fields <- c("ts","start","params","appId","tag","capacity")  
capacities <- enframe(read_lines( "uniform01_capacity.txt" ), value = "line") |>
  separate(col = line, into = fields, sep = "\\|") |>
  select(appId, capacity)

data <- times |> inner_join(capacities, by = c("appId")) |> 
  group_by(dataset, method, epsilon, capacity) |> summarise(time = mean(time)) 

p = ggplot(data, aes(x = as.factor(epsilon), y = time, group = method)) +
  geom_line(aes(linetype = method, color = method)) + 
  geom_point(aes(shape = method, color = method), size = 3) +
  labs(x=TeX("$\\epsilon(m)$"), y="Time(s)") +
  scale_color_discrete("Method") +
  scale_shape_discrete("Method") +
  guides(linetype = "none") +
  theme_bw() +
  facet_grid(capacity ~ dataset)
plot(p)  

W = 14
H = 10
ggsave(paste0("uniform_bechmark.pdf"), width = W, height = H)
