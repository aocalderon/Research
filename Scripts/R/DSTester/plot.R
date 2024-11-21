library(tidyverse)
library(latex2exp)

data = enframe(read_lines("~/Research/Scripts/Scala/DSTester/dstester1.txt"), value = "line") |>
  filter(str_detect(line, 'TIME')) |>
  separate(col = line, into = c("ts","epsilon","mu","implementation","clusters","time"), sep = "\\|") |>
  select(epsilon, implementation, clusters, time) |>
  mutate(time = as.numeric(time), 
         epsilon = as.numeric(epsilon), 
         clusters = as.numeric(clusters)) |>
  group_by(epsilon, implementation, clusters) |> summarise(time = mean(time))

p = ggplot(data, aes(x = as.factor(epsilon), y = time, group = implementation)) +
  geom_line(aes(linetype = implementation, color = implementation)) + 
  geom_point(aes(shape = implementation, color = implementation), size = 3) +
  labs(x=TeX("$\\epsilon(m)$"), y="Time(s)") +
  scale_color_discrete("Implementation") +
  scale_shape_discrete("Implementation") +
  guides(linetype = "none") +
  theme_bw()
plot(p)  

W = 6
H = 4
ggsave(paste0("~/Research/Scripts/R/DSTester/DBScanByTime.pdf"), width = W, height = H)