library(tidyverse)
library(latex2exp)
library(xtable)

PATTERN="_1070"
FILENAME=paste0("logs/spark", PATTERN, ".log")
if(!file.exists(FILENAME)){
    read_lines( pipe(paste0('ssh acald013@hn "/home/acald013/bin/checkNodes -p ', PATTERN, '" ')) ) |>
    write_lines(FILENAME)
} 

fields = c("ts","host","tag","cellId","appId","partitions","dataset","epsilon","mu","delta","project","stage","time")
data = enframe(read_lines(FILENAME), value = "line") |>
  filter(str_detect(line, 'TIME')) |>  
  separate(col = line, into = fields, sep = "\\|") |>
  select(cellId, stage, time) |>
  mutate(time = as.numeric(time), cellId = as.numeric(cellId)) 

p = ggplot(data, aes(x = as.factor(cellId), y = time, fill = stage)) +
  geom_col(position = "dodge") +
  labs(x="Cell ID", y="Time(s)") +
  theme_bw()
plot(p)  

#W = 8
#H = 4
#ggsave(paste0("performanceByCellStage.pdf"), width = W, height = H)
