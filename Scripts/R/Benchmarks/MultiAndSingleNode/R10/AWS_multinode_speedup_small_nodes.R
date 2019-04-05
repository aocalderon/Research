#!/usr/bin/Rscript

require(ggplot2)
require(stringr)
require(tidyverse)

READ_DATA     = T
SAVE_PDF      = T
SEP           = ";"
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))
RESULTS_PATH = "Scripts/R/Benchmarks/MultiAndSingleNode/R10/"
RESULTS_NAME = "AWS_multinode_speedup_small_nodes"
dataFile = paste0(RESEARCH_HOME, RESULTS_PATH, RESULTS_NAME, '.txt')

data = readLines(dataFile)

data = as_tibble(as.data.frame(data), stringAsFactors = F) %>% 
  rename(Line = data) %>% 
  filter(grepl("PFLOCK;", Line)) %>% 
  separate(Line, c("Bogus", "Cores", "Nodes", "Epsilon", "Mu", "Delta", "Time3", "Load", "Id"), sep = SEP) %>%
  select(Nodes, Epsilon, Time3) %>%
  mutate(Epsilon = as.numeric(Epsilon), Time2 = as.numeric(Time3)) %>%
  group_by(Nodes, Epsilon) %>% summarise(Time = mean(Time2), SD = sd(Time2))
data$Nodes <- factor(data$Nodes, levels = c("1", "2", "4", "8", "16"))
                       
title = "Multinode Speed Up by Epsilon [Berlin_160K, 4 cores per node, 1 thread per core]"
g = ggplot(data=data, aes(x=factor(Epsilon), y=Time, fill=Nodes)) +
    geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
    #geom_errorbar(aes(ymin=Time-SD, ymax=Time+SD), width=.2, position=position_dodge(width = 0.75)) +
    labs(title=title, y="Time(s)", x=expression(paste(epsilon,"(mts)"))) 
if(SAVE_PDF){
  ggsave(paste0(RESEARCH_HOME, RESULTS_PATH, RESULTS_NAME, '.pdf'), width = 14, height = 8.5, dpi = 150, units = "in", device='pdf', g)
} else {
  plot(g)
}
