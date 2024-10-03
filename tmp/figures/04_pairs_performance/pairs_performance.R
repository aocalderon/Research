library(tidyverse)
library(latex2exp)

data <- read_tsv("pairs_performance.tsv")

p = ggplot(data, aes(x = pairs, y = time, shape = method, color = method)) +
  geom_point() +
  labs(x=TeX("Pairs per partition"), y="Time(s)") +
  scale_color_discrete("Method") +
  scale_shape_discrete("Method") +
  facet_wrap(~ epsilon) +
  theme_bw() +
  theme(legend.position="bottom") 
plot(p)  

W = 8
H = 6
ggsave(paste0("pairs_performance.pdf"), width = W, height = H)
