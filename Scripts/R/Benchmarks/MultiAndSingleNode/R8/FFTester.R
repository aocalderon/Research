#!/usr/bin/Rscript

require(ggplot2)
require(ggrepel)
require(stringr)
require(tidyverse)

READ_DATA     = T
SAVE_PDF      = F
SEP           = ";"
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))
RESULTS_PATH = "Scripts/R/Benchmarks/MultiAndSingleNode/R8/"
RESULTS_NAME = "FFTester"
dataFile = paste0(RESEARCH_HOME, RESULTS_PATH, RESULTS_NAME, '.csv')

data = readLines(dataFile)

data = as_tibble(as.data.frame(data), stringAsFactors = F) %>% 
  rename(Line = data) %>%
  separate(Line, c("MFX", "MFY", "FFX", "FFY", "Time1", "Time2"), sep = SEP) %>%
  unite(Partitions, MFX, FFX, sep = "-") %>%
  select(Partitions, Time1, Time2) %>%
  mutate(Time1 = as.numeric(Time1), Time2 = as.numeric(Time2)) 

dataTime1 = data %>% filter(Time1 > 0) 
  
title = "Execution time by number of partitions [FF: Flock finder, MF:Maximal finder]"
t1 = ggplot(data=dataTime1, aes(x=Partitions, y=Time1, group=1)) +
  geom_line(color="red") +
  geom_point() + theme(axis.text.x = element_text(hjust=-1, vjust=0.2, angle=90)) +
  labs(title=title, y="Time(s)", x="Partitions [FF-MF]") +
  geom_label_repel(aes(label = Time1), data = subset(dataTime1, Time1 < 283))

#g = ggplot(data=data, aes(x=factor(Epsilon), y=Time, fill=Cores)) +
#  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
#  labs(title=title, y="Time(s)", x=expression(paste(epsilon,"(mts)"))) 
if(SAVE_PDF){
  ggsave(paste0(RESEARCH_HOME, RESULTS_PATH, RESULTS_NAME, '_T1.pdf'), width = 20, height = 5, dpi = 300, units = "in", device='pdf', t1)
} else {
  plot(t1)
}

dataTime2 = data %>% filter(Time2 > 0) 

t2 = ggplot(data=dataTime2, aes(x=Partitions, y=Time2, group=1)) +
  geom_line(color="red") +
  geom_point() + theme(axis.text.x = element_text(hjust=-1, vjust=0.2, angle=90)) +
  labs(title=title, y="Time(s)", x="Partitions [FF-MF]") +
  geom_label_repel(aes(label = Time2), data = subset(dataTime2, Time2 < 560))

if(SAVE_PDF){
  ggsave(paste0(RESEARCH_HOME, RESULTS_PATH, RESULTS_NAME, '_T2.pdf'), width = 20, height = 5, dpi = 300, units = "in", device='pdf', t2)
} else {
  plot(t2)
}
