library(tidyverse)
library(latex2exp)

data <- read_tsv("step_performance.tsv") |> 
  mutate(Step = as.factor(Step), epsilon = as.factor(epsilon))

p = ggplot(data, aes(x = epsilon, y = time, fill = Step)) + 
  geom_col(width = 0.7, position="dodge") + 
  scale_fill_manual(
    breaks = c("1", "2", "3", "4", "5" , "6"),
    values = c("1" = "#F8766D", "2" = "#C49A00", "3" = "#53B400", "4" = "#00C094", "5" = "#00B6EB", "6" = "#A58AFF", "7" = "#FB61D7")
  ) +
  guides(fill = guide_legend("By-Level\nStep", override.aes = list(shape = NA))) +
  geom_point(data = master, aes(color = "black", shape = NA)) +
  scale_color_manual(labels = c(" "), values = c("#FB61D7")) +
  guides(color = guide_legend("Master", override.aes=list(shape=15, size = 6.5))) +
  labs(x=TeX("$\\epsilon (m)$"), y="Time (s)") +
  theme_bw() 
plot(p)  

W=6
H=4
ggsave(paste0("step_performance.pdf"), width = W, height = H)
