#!/usr/bin/Rscript

require(ggplot2)
require(stringr)
require(tidyverse)

READ_DATA     = T
SAVE_PDF      = T
SEP           = ";"
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))
RESULTS_PATH = "Scripts/R/Benchmarks/MultiAndSingleNode/R11/"
RESULTS_NAME = "DBLab_Singlenode_speedup"
dataFile = paste0(RESEARCH_HOME, RESULTS_PATH, RESULTS_NAME, '.txt')

data = readLines(dataFile)

data = as.tibble(as.data.frame(data), stringAsFactors = F) %>% 
  rename(Line = data) %>% 
  separate(Line, c("Cores", "Epsilon", "Time", "Load"), sep = ";") %>%
  select(Cores, Epsilon, Time, Load) %>%
  mutate(Epsilon = as.numeric(Epsilon), Time = as.numeric(Time), Load = as.numeric(Load)) %>%
  group_by(Cores, Epsilon) %>% summarise(Time = mean(Time))

title = "Singlenode Speed Up by Epsilon [Berlin_120K, 4 cores, 1 threads per core]"
data$Cores = factor(data$Cores, levels=c("1","2","4","8"))
g = ggplot(data=data, aes(x=factor(Epsilon), y=Time, fill=Cores)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  labs(title=title, y="Time(s)", x=expression(paste(epsilon,"(mts)"))) 
if(SAVE_PDF){
  ggsave(paste0(RESEARCH_HOME, RESULTS_PATH, RESULTS_NAME, '.pdf'), width = 15, height = 8.5, dpi = 150, units = "in", device='pdf', g)
} else {
  plot(g)
}
