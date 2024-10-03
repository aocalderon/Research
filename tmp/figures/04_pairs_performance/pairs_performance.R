library(tidyverse)
library(latex2exp)

# fields <- c("ts","start","host", "tag", "z", "appId","partitions","dataset","epsilon","mu","delta","method","stage","time")  
# data0 <- enframe(read_lines( "pairs_performance.txt" ), value = "line") |>
#   filter(str_detect(line, 'TIME')) |>
#   separate(col = line, into = fields, sep = "\\|") |>
#   mutate(stage = str_trim(stage), epsilon = as.numeric(epsilon), time = as.numeric(time)) |>
#   filter(stage == "Total") |>
#   filter(epsilon <= 20) |>
#   select(dataset, method, epsilon, time) |> 
#   group_by(dataset, method, epsilon) |> summarise(time = mean(time)) 
# 
# pairs <- enframe(read_lines( "pairs_performance.txt" ), value = "line") |>
#   filter(str_detect(line, 'INFO')) |>
#   filter(str_detect(line, 'BFE')) |>
#   filter(str_detect(line, 'Pairs')) |>
#   separate(col = line, into = fields, sep = "\\|") |>
#   mutate(stage = str_trim(stage), epsilon = as.numeric(epsilon), pairs = as.numeric(time)) |>
#   filter(epsilon <= 20) |>
#   select(dataset, epsilon, pairs) |> 
#   group_by(dataset, epsilon) |> summarise(pairs = max(pairs)) 
#
# data <- data0 |> inner_join(pairs, by = c("dataset", "epsilon"))

data <- read_tsv("pairs_performance.tsv")

p = ggplot(data, aes(x = pairs, y = time, shape = method, color = method)) +
  geom_point() +
  labs(x=TeX("Pairs per partition"), y="Time(s)") +
  scale_color_discrete("Method") +
  scale_shape_discrete("Method") +
  facet_wrap(~ epsilon) +
  theme_bw() 
plot(p)  

W = 8
H = 6
ggsave(paste0("pairs_performance.pdf"), width = W, height = H)
