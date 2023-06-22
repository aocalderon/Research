library(tidyverse)

data = read_tsv( 
  pipe( 'ssh acald013@hn "cat /home/acald013/Research/Scripts/Scala/Cliques/candidates_query.tsv"' ), 
  col_names = c("cellId", "stage", "time")
  )

p = ggplot(data, aes(x = as.factor(cellId), y = time, fill = stage)) +
  geom_col() +
  scale_fill_discrete("Stage") +
  labs(x="Cell ID", y="Time(s)") +
  theme_bw()
plot(p)  

W = 6
H = 4
ggsave(paste0("performance_agg.pdf"), width = W, height = H)
