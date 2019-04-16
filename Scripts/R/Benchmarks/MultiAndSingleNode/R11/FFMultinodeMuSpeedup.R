#!/usr/bin/Rscript

require(ggplot2)
require(stringr)
require(tidyverse)

READ_DATA     = T
SAVE_PDF      = T
SEP           = ";"
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))
RESULTS_PATH = "Scripts/R/Benchmarks/MultiAndSingleNode/R11/"
RESULTS_NAME = "FFMultinodeMuSpeedup"
dataFile = paste0(RESEARCH_HOME, RESULTS_PATH, RESULTS_NAME, '.txt')

data = readLines(dataFile)

data0 = as_tibble(as.data.frame(data), stringAsFactors = F) %>% 
  rename(Line = data) %>% 
  filter(grepl("PFLOCK;", Line)) %>% 
  separate(Line, c("Bogus", "Cores", "Nodes", "Epsilon", "Mu", "Delta", "Time3", "Load", "Id"), sep = SEP) %>%
  select(Nodes, Mu, Time3) %>%
  mutate(Mu = as.numeric(Mu), Time2 = as.numeric(Time3)) 
data0$Nodes <- factor(data0$Nodes, levels = c("1", "2", "3"))

data1 = data0 %>% group_by(Nodes, Mu) %>% summarise(Time = mean(Time2), SD = sd(Time2))

title = "Multinode Speed Up by Mu [Berlin_160K, 4 cores per node, 1 thread per core]"
g = ggplot(data=data1, aes(x=factor(Mu), y=Time, fill=Nodes)) +
    geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.7) +
    geom_errorbar(aes(ymin=Time-SD, ymax=Time+SD), width=.1, position=position_dodge(width = 0.75)) +
    labs(title=title, y="Time(s)", x=expression(paste(mu,"(number of objects)"))) 
if(SAVE_PDF){
  ggsave(paste0(RESEARCH_HOME, RESULTS_PATH, RESULTS_NAME, '_bars.pdf'), width = 15, height = 8.5, dpi = 150, units = "in", device='pdf', g)
} else {
  plot(g)
}

title = "Multinode Speed Up by Mu Boxplot [Berlin_160K, 4 cores per node, 1 thread per core]"
f = ggplot(data=data0, aes(x=factor(Mu), y=Time2, fill=Nodes)) +
  stat_boxplot(geom ='errorbar', width = 0.5, position = position_dodge(1)) +
  geom_boxplot(outlier.size = 0.5, position = position_dodge(1)) +
  labs(title=title, y="Time(s)", x=expression(paste(mu,"(number of objects)")))
if(SAVE_PDF){
  ggsave(paste0(RESEARCH_HOME, RESULTS_PATH, RESULTS_NAME, '_boxplot.pdf'), width = 15, height = 8.5, dpi = 150, units = "in", device='pdf', f)
} else {
  plot(f)
}

op <- options(digits.secs=3)
stats = data0 %>% group_by(Nodes, Mu) %>% summarise(lower = quantile(Time2)[2], upper = quantile(Time2)[4])
data2 = data0 %>% inner_join(stats, by = c("Nodes", "Mu")) %>% filter(Time2 >= lower & Time2 <= upper)
data3 = data2 %>% group_by(Nodes, Mu) %>% summarise(Time = mean(Time2), SD = sd(Time2))

title = "Multinode Speed Up by Mu without outliers [Berlin_160K, 4 cores per node, 1 thread per core]"
h = ggplot(data=data3, aes(x=factor(Mu), y=Time, fill=Nodes)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) +
  geom_errorbar(aes(ymin=Time-SD, ymax=Time+SD), width=.1, position=position_dodge(width = 0.75)) +
  labs(title=title, y="Time(s)", x=expression(paste(mu,"(number of objects)"))) 

if(SAVE_PDF){
  ggsave(paste0(RESEARCH_HOME, RESULTS_PATH, RESULTS_NAME, '_outliers.pdf'), width = 15, height = 8.5, dpi = 150, units = "in", device='pdf', h)
} else {
  plot(h)
}
options(op)