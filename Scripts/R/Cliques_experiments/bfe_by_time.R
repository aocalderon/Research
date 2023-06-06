library(tidyverse)

datafile = "/home/acald013/Research/tmp/RE_BFE_E3-5.tsv"
data = read_tsv( pipe( paste0('ssh hn "cat ', datafile, '"') ) )
data$stage <- factor(data$stage,      # Reordering stage factor levels
                         levels = c("Grid", "Read", "Cliques", "Pairs", "Centers", "Candidates", "Maximals", "Total"))

p = ggplot(data, aes(x = as.factor(epsilon), y = time, fill = method)) + 
  geom_col(width = 0.7, position="dodge") + 
  labs(x="Epsilon(m)", y="Time(s)") +
  facet_wrap(~ stage)
plot(p)

W = 6
H = 4
ggsave(paste0("bfe_by_time_stage2.pdf"), width = W, height = H)

