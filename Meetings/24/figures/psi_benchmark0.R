library(tidyverse)
library(latex2exp)

fields <- c("ts","start","host", "tag", "z", "appId","partitions","dataset","epsilon","mu","delta","method","stage","time")  
data0 <- enframe(read_lines( "psi_benchmark0.txt" ), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  separate(col = line, into = fields, sep = "\\|") |>
  mutate(stage = str_trim(stage), epsilon = as.numeric(epsilon), time = as.numeric(time)) |>
  filter(stage == "Total") |>
  select(appId, method, epsilon, time) 

data1 <- data0 |> 
  group_by(method, epsilon) |> summarise(time = mean(time)) 

p = ggplot(data, aes(x = as.factor(epsilon), y = time, group = method)) +
  geom_line(aes(linetype = method, color = method)) + 
  geom_point(aes(shape = method, color = method), size = 3) +
  labs(x=TeX("$\\epsilon(m)$"), y="Time(s)") +
  scale_color_discrete("Method") +
  scale_shape_discrete("Method") +
  guides(linetype = "none") +
  theme_bw()
plot(p)  

W = 6
H = 4
ggsave(paste0("psi_benchmark0.pdf"), width = W, height = H)
