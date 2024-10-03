library(tidyverse)
library(latex2exp)

data <- read_tsv("uniform_performance.tsv")

p = ggplot(data, aes(x = as.factor(epsilon), y = time, group = method)) +
  geom_line(aes(linetype = method, color = method)) + 
  geom_point(aes(shape = method, color = method), size = 3) +
  labs(x=TeX("$\\epsilon$"), y="Time(s)") +
  scale_color_discrete("Method") +
  scale_shape_discrete("Method") +
  guides(linetype = "none") +
  facet_grid(capacity ~ as.numeric(dataset)) +
  theme_bw()
plot(p)  

W = 12
H = 8
ggsave(paste0("uniform_performance.pdf"), width = W, height = H)
