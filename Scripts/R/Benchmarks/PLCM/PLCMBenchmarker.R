#!/usr/bin/Rscript

require(ggplot2)
require(stringr)
require(tidyverse)

READ_DATA     = T
SAVE_PDF      = F
SEP           = ";"
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))

plcm1 = read.table(paste0(RESEARCH_HOME, 'Scripts/R/Benchmarks/PLCM/plcm1_5-15.log'), sep =  SEP)
plcm1$V1 = rep(5:15, 5) 
plcm2 = read.table(paste0(RESEARCH_HOME, 'Scripts/R/Benchmarks/PLCM/plcm2_8-20.log'), sep =  SEP)
plcm2$V1 = rep(8:20, 5) 
plcm3 = read.table(paste0(RESEARCH_HOME, 'Scripts/R/Benchmarks/PLCM/plcm3_9-20.log'), sep =  SEP)
plcm3$V1 = rep(9:20, 10) 

data = rbind(plcm1, plcm2, plcm3)

data = as.tibble(data, stringAsFactors = F) %>% 
  rename(PartitionsIn = "V1",  PartitionsOut= "V2", Max = "V3", Min = "V4", Avg = "V5", Load = "V6", Time = "V7", Maximal = "V8", Prunned = "V9") %>%
  select(PartitionsIn, Time) %>%
  mutate(Time = as.numeric(Time)) %>%
  group_by(PartitionsIn) %>% summarise(Time = mean(Time))

title = "QUADTREE by Number of Partitions..."
g = ggplot(data=data, aes(x=factor(PartitionsIn), y=Time)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  labs(title=title, y="Time(s)", x="Number of partitions") 
if(SAVE_PDF){
  ggsave("./PLCM.pdf", width = 7, height = 4, dpi = 300, units = "in", device='pdf', g)
} else {
  plot(g)
}
