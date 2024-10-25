library(tidyverse)

mf = read_tsv("optimal_performance.tsv")

W = 6
H = 4
for(e in as.numeric(levels(as.factor(mf$epsilon)))) {
  d = mf |> filter(epsilon == e) 
  p = ggplot(d, aes(x = as.factor(partitions), y = time)) + 
    geom_col(width = 0.7, position="dodge") + 
    labs(x="Number of partitions", y="Time(s)", title = paste("Epsilon=",e))
  plot(p)
  ggsave(paste0("pflockE", e,"_by_partitions.pdf"), width = W, height = H)
  
  g = ggplot(d, aes(x = as.factor(capacity), y = time)) + 
    geom_col(width = 0.7, position="dodge") + 
    labs(x="Capacity", y="Time(s)", title = paste("Epsilon=",e)) +
    theme_bw()
  plot(g)
  ggsave(paste0("pflockE", e,"_by_capacity.pdf"), width = W, height = H)
}

