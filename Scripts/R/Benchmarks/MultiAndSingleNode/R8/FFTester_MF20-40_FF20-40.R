#!/usr/bin/Rscript

require(ggplot2)
require(stringr)
require(tidyverse)

READ_DATA     = T
SAVE_PDF      = F
SEP           = "\t"
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))
RESULTS_PATH = "Scripts/R/Benchmarks/MultiAndSingleNode/R8/"
RESULTS_NAME = "FFTester_MF20-40_FF20-40"
dataFile = paste0(RESEARCH_HOME, RESULTS_PATH, RESULTS_NAME, '.txt')

data = readLines(dataFile)

data = as_tibble(as.data.frame(data), stringAsFactors = F) %>% 
  rename(Line = data) %>%
  separate(Line, c("MFX", "MFY", "FFX", "FFY", "Time1", "Time2"), sep = SEP) %>%
  unite(Partitions, MFX, FFX, sep = "-") %>%
  select(Partitions, Time1, Time2) %>%
  mutate(Time1 = as.numeric(Time1), Time2 = as.numeric(Time2)) 

title = "Execution time by number of partitions [FF: Flock finder, MF:Maximal finder]"
g = ggplot(data=data, aes(x=Partitions, y=Time1, group=1)) +
  geom_line(color="red") +
  geom_point() + theme(axis.text.x = element_text(hjust=-1, vjust=0.2, angle=90)) +
  labs(title=title, y="Time(s)", x="Partitions [FF-MF]")

#g = ggplot(data=data, aes(x=factor(Epsilon), y=Time, fill=Cores)) +
#  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
#  labs(title=title, y="Time(s)", x=expression(paste(epsilon,"(mts)"))) 
if(SAVE_PDF){
  ggsave(paste0(RESEARCH_HOME, RESULTS_PATH, RESULTS_NAME, '.pdf'), width = 14, height = 4, dpi = 300, units = "in", device='pdf', g)
} else {
  plot(g)
}
