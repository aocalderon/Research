library(tidyverse)
library(latex2exp)

data <- read_tsv("pairs_performance.tsv")

p = ggplot(data, aes(x = n, y = time, shape = method, color = method)) +
  geom_point() +
  ylim(0, 1.5) +
  labs(x=TeX("Pairs per cell"), y="Time(s)") +
  scale_color_discrete("Method") +
  scale_shape_discrete("Method") +
  facet_wrap(~ epsilon) +
  theme_bw() 
plot(p)  

W = 8
H = 6
ggsave(paste0("pairs_performance.pdf"), width = W, height = H)