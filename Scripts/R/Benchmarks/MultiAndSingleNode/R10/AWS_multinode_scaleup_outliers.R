#!/usr/bin/Rscript

require(ggplot2)
require(stringr)
require(lubridate)
require(tidyverse)

op <- options(digits.secs=3)
READ_DATA     = T
SAVE_PDF      = T
RUNS          = 25
NODES         = 4
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))
RESULTS_PATH = "Scripts/R/Benchmarks/MultiAndSingleNode/R9/"
RESULTS_NAME = "AWS_multinode_scaleup_outliers"
dataFile = paste0(RESEARCH_HOME, RESULTS_PATH, RESULTS_NAME, '.txt')

if(READ_DATA){
  data = readLines(dataFile)
  data = as_tibble(as.data.frame(data), stringAsFactors = F) %>% 
    rename(Line = data) %>% 
    filter(grepl("PFLOCK;", Line)) %>% 
    separate(Line, c("Bogus", "Cores", "Nodes", "Epsilon", "Mu", "Delta", "Time", "Load", "Id"), sep = SEP) %>%
    select(Nodes, Epsilon, Time) %>%
    mutate(Epsilon = as.numeric(Epsilon), Time2 = as.numeric(Time)) 
}

stats = data %>% group_by(Nodes, Epsilon) %>% summarise(lower = quantile(Time2)[2], upper = quantile(Time2)[4])
data2 = data %>% inner_join(stats, by = c("Nodes", "Epsilon")) %>% filter(Time2 > lower & Time2 < upper)
data3 = data2 %>% group_by(Nodes, Epsilon) %>% summarise(Time = mean(Time2), SD = sd(Time2))

title = "Execution time without outliers [Berlin_40K - 80K - 120K - 160K, Epsilon=110, Mu=3, Delta=3]"
g = ggplot(data=data3, aes(x=factor(Epsilon), y=Time, fill=Nodes)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) +
  geom_errorbar(aes(ymin=Time-SD, ymax=Time+SD), width=.1, position=position_dodge(width = 0.75)) +
  labs(title=title, y="Time(s)", x=expression(paste(epsilon,"(mts)"))) 

if(SAVE_PDF){
  ggsave(paste0(RESEARCH_HOME, RESULTS_PATH, RESULTS_NAME, '.pdf'), width = 15, height = 8.50, dpi = 150, units = "in", device='pdf', g)
} else {
  plot(g)
}
options(op)