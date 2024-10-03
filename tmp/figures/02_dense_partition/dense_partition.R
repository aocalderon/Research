library(tidyverse)
library(latex2exp)

data = read_tsv("dense_partition.tsv")

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
ggsave(paste0("dense_partition.pdf"), width = W, height = H)
