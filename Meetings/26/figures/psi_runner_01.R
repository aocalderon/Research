library(tidyverse)
library(latex2exp)

fields <- c("ts","start","host", "tag", "z", "appId","partitions","dataset","epsilon","mu","delta","method","stage","time")  

data <- enframe(read_lines( "psi_runner_01.txt" ), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  separate(col = line, into = fields, sep = "\\|") |>
  mutate(stage = str_trim(stage), epsilon = as.numeric(epsilon), time = as.numeric(time)) |>
  filter(str_detect(stage, 'Total')) |>
  select(epsilon, method, time) |>
  group_by(epsilon, method) |> 
  summarise(time = mean(time)) 

p = ggplot(data, aes(x = epsilon, y = time, shape = method, color = method)) +
  geom_line() +
  geom_point() +
  labs(x=TeX("$\\epsilon$ (m)"), y="Time(s)") +
  theme_bw() 
plot(p)  

W = 8
H = 6
ggsave(paste0("psi_runner_01.pdf"), width = W, height = H)
