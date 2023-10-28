library(tidyverse)
library(latex2exp)

fields <- c("ts","start","host", "tag", "z", "appId","partitions","dataset","epsilon","mu","delta","method","stage","time")  
data0 <- enframe(read_lines( "psi_benchmark2.txt" ), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  separate(col = line, into = fields, sep = "\\|") |>
  mutate(stage = str_trim(stage), epsilon = as.numeric(epsilon), time = as.numeric(time)) |>
  filter(stage == "Total") |>
  filter(epsilon <= 20) |>
  select(dataset, method, epsilon, time) 

data1 <- data0 |> 
  group_by(dataset, method, epsilon) |> summarise(time = mean(time)) 

density <- read_csv("density_per_cell.csv") |>
  #arrange(desc(density)) |>
  arrange(density) |>
  mutate(dataset = paste0("cell", field_2)) |>
  select(dataset, density) |>
  top_n(15, wt = desc(density))

data <- data1 |> inner_join(density, by = "dataset")

p = ggplot(data, aes(x = as.factor(epsilon), y = time, group = method)) +
  geom_line(aes(linetype = method, color = method)) + 
  geom_point(aes(shape = method, color = method), size = 3) +
  labs(x=TeX("$\\epsilon(m)$"), y="Time(s)") +
  scale_color_discrete("Method") +
  scale_shape_discrete("Method") +
  guides(linetype = "none") +
  theme_bw() +
  facet_grid(~dataset)
plot(p)  

W = 20
H = 6
ggsave(paste0("psi_benchmark2.pdf"), width = W, height = H)
