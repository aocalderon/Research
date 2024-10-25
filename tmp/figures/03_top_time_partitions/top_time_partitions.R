library(tidyverse)
library(latex2exp)

data <- read_tsv("top_time_partitions.tsv")

p = ggplot(data, aes(x = as.factor(epsilon), y = time, group = method)) +
  geom_line(aes(linetype = method, color = method)) + 
  geom_point(aes(shape = method, color = method), size = 3) +
  labs(x=TeX("$\\epsilon(m)$"), y="Time(s)") +
  scale_color_discrete("Method") +
  scale_shape_discrete("Method") +
  guides(linetype = "none") +
  theme_bw() +
  theme(legend.position="bottom", axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  facet_grid(~dataset)
plot(p)  

W = 9
H = 5
ggsave(paste0("top_time_partitions.pdf"), width = W, height = H)
