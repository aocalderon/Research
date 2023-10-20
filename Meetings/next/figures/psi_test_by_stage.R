library(tidyverse)
library(latex2exp)

fields <- c("ts","start","host", "tag", "z", "appId","partitions","dataset","epsilon","mu","delta","method","stage","time")  
data0 <- enframe(read_lines( "psi_test_v3.txt" ), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  separate(col = line, into = fields, sep = "\\|") |>
  select(appId, epsilon, mu, method, stage, time) |>
  mutate(stage = str_trim(stage), epsilon = as.numeric(epsilon), 
         mu = as.numeric(mu), time = as.numeric(time))

data <- data0 |> 
  filter(method == "PSI") |>
  filter(stage != 'Total' & stage != 'PS' & stage != 'FC') |>
  group_by(appId, epsilon, mu, stage) |> summarise(time = mean(time)) 

dataM3 <- data |> filter(mu == 3) |> select(epsilon, stage, time) |> 
  group_by(epsilon, stage) |> summarise(time = mean(time))

p = ggplot(dataM3, aes(x = as.factor(epsilon), y = time, group = stage)) +
  geom_line(aes(linetype = stage, color = stage)) + 
  geom_point(aes(shape = stage, color = stage), size = 3) +
  labs(x=TeX("$\\epsilon(m)$"), y="Time(s)") +
  scale_color_discrete("Stage") +
  scale_shape_discrete("Stage") +
  guides(linetype = "none") +
  theme_bw()
plot(p)  

W = 6
H = 4
ggsave(paste0("psi_test_by_stage.pdf"), width = W, height = H)
