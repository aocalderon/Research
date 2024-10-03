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

times <- data1 |> filter(method == "BFE") |> filter(epsilon == 20) |> ungroup() |>
  select(dataset, time) |>
  mutate(t = time) |>
  arrange(t) |>
  #top_n(15, wt = desc(density))
  top_n(10, wt = t) |> select(dataset)

data <- data1 |> inner_join(times, by = "dataset")

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