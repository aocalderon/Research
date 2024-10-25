library(tidyverse)

data <- read_tsv("interval-performance.tsv") 

p = ggplot(data, aes(x = factor(interval), y = time)) + 
  geom_col(width = 0.5, position="dodge") + 
  labs(x=TeX("$\\Interval$"), y="Time (s)") +
  theme_bw() 
plot(p)  

W=6
H=4
ggsave(paste0("interval-performance.pdf"), width = W, height = H)
