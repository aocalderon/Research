#!/usr/bin/Rscript

require(ggplot2)
require(stringr)
require(tidyverse)

READ_DATA     = T
SAVE_PDF      = T
SEP           = ","
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))

dataFile = paste0(RESEARCH_HOME, 'Scripts/R/Benchmarks/MultiAndSingleNode/MaximalFinderMultinode.csv')

data = as.tibble(read.table(dataFile, header = F, sep = SEP), stringAsFactors = F) %>% 
  rename(Time = V1, Load = V2, Nodes = V3, Timestamp = V4, Epsilon = V5) %>% 
  select(Nodes, Epsilon, Time, Timestamp) %>%
  mutate(Epsilon = as.numeric(Epsilon), Time = as.numeric(Time)) %>%
  group_by(Nodes, Timestamp, Epsilon) %>% summarise(Time = mean(Time)) %>% filter(Epsilon == 90)

title = "Maximal Finder Multinode Speed up"
g = ggplot(data=data, aes(x=factor(Timestamp), y=Time, fill=factor(Nodes))) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  labs(title=title, y="Time(s)", x="Timestamp") 
if(SAVE_PDF){
  ggsave("./MaximalFinderMultinodeSpeedUp.pdf", width = 7, height = 4, dpi = 300, units = "in", device='pdf', g)
} else {
  plot(g)
}
