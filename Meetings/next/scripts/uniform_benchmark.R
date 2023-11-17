library(tidyverse)
library(latex2exp)

fields <- c("ts","start","host", "tag", "z", "appId","partitions","dataset","epsilon","mu","delta","method","stage","time")  
times <- enframe(read_lines( "~/Datasets/uniform_datasets/uniform02_time.txt" ), value = "line") |>
  separate(col = line, into = fields, sep = "\\|") |>
  mutate(stage = str_trim(stage), epsilon = as.numeric(epsilon)*100, time = as.numeric(time)) |>
  select(appId, dataset, method, epsilon, time) 

fields <- c("ts","start","params","appId","tag","capacity")  
capacities <- enframe(read_lines( "~/Datasets/uniform_datasets/uniform02_capacity.txt" ), value = "line") |>
  separate(col = line, into = fields, sep = "\\|") |>
  select(appId, capacity)

data <- times |> inner_join(capacities, by = c("appId")) |> 
  group_by(dataset, method, epsilon, capacity) |> summarise(time = mean(time)) |>
  filter(epsilon <= 0.5)

p = ggplot(data, aes(x = as.factor(epsilon), y = time, group = method)) +
  geom_line(aes(linetype = method, color = method)) + 
  geom_point(aes(shape = method, color = method), size = 3) +
  labs(x=TeX("$\\epsilon$"), y="Time(s)") +
  scale_color_discrete("Method") +
  scale_shape_discrete("Method") +
  guides(linetype = "none") +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  facet_grid(capacity ~ as.numeric(dataset))
plot(p)  

W = 12
H = 8
ggsave(paste0("uniform_benchmark.pdf"), width = W, height = H)
