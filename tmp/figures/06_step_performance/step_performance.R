library(tidyverse)
library(latex2exp)

data <- read_tsv("step_performance.tsv") |> mutate(Step = as.factor(Step), epsilon = as.factor(epsilon))

p = ggplot(data, aes(x = epsilon, y = time, fill = Step)) + 
  geom_col(width = 0.7, position="dodge") + 
  scale_color_discrete("Step") +
  scale_shape_discrete("Step") +
  labs(x=TeX("$\\epsilon (m)$"), y="Time (s)") +
  theme_bw()
plot(p)  

W=8
H=6
ggsave(paste0("step_performance.pdf"), width = W, height = H)
