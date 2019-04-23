#!/usr/bin/Rscript

require(ggplot2)
require(stringr)
require(tidyverse)

READ_DATA     = T
SAVE_PDF      = T
SEP           = ";"
YLIM_MAX      = 1000
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))
RESULTS_PATH = "Scripts/R/Benchmarks/MultiAndSingleNode/R11/"
RESULTS_NAME = "FFMultinodeDeltaScaleup"
dataFile = paste0(RESEARCH_HOME, RESULTS_PATH, RESULTS_NAME, '.txt')

data = readLines(dataFile)

data0 = as_tibble(as.data.frame(data), stringAsFactors = F) %>% 
  rename(Line = data) %>% 
  filter(grepl("PFLOCK;", Line)) %>% 
  separate(Line, c("Bogus", "Cores", "Nodes", "Epsilon", "Mu", "Delta", "Time3", "Load", "Id"), sep = SEP) %>%
  select(Nodes, Delta, Time3) %>%
  mutate(Delta = as.numeric(Delta), Time2 = as.numeric(Time3)) 
data0$Nodes <- factor(data0$Nodes, levels = c("1", "2", "3"))

data1 = data0 %>% group_by(Nodes, Delta) %>% summarise(Time = mean(Time2), SD = sd(Time2))

title = "Multinode Scale Up by Delta [Berlin_N40K, Berlin_N80K, Berlin_N120K, 4 cores per node, 1 thread per core]"
g = ggplot(data=data1, aes(x=factor(Delta), y=Time, fill=Nodes)) +
    geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) +
    geom_errorbar(aes(ymin=Time-SD, ymax=Time+SD), width=.1, position=position_dodge(width = 0.75)) + ylim(0, YLIM_MAX) +
    labs(title=title, y="Time(s)", x=expression(paste(delta,"(length in timestamps)"))) 
if(SAVE_PDF){
  ggsave(paste0(RESEARCH_HOME, RESULTS_PATH, RESULTS_NAME, '_bars.pdf'), width = 15, height = 8.5, dpi = 150, units = "in", device='pdf', g)
} else {
  plot(g)
}

title = "Multinode Scale Up by Delta Boxplot [Berlin_N40K, Berlin_N80K, Berlin_N120K, Berlin_N160K, 4 cores per node, 1 thread per core]"
f = ggplot(data=data0, aes(x=factor(Delta), y=Time2, fill=Nodes)) +
  stat_boxplot(geom ='errorbar', width = 0.5, position = position_dodge(1)) +
  geom_boxplot(outlier.size = 0.5, position = position_dodge(1)) + ylim(0, YLIM_MAX) +
  labs(title=title, y="Time(s)", x=expression(paste(delta,"(length in timestamps)")))
if(SAVE_PDF){
  ggsave(paste0(RESEARCH_HOME, RESULTS_PATH, RESULTS_NAME, '_boxplot.pdf'), width = 15, height = 8.5, dpi = 150, units = "in", device='pdf', f)
} else {
  plot(f)
}

op <- options(digits.secs=3)
stats = data0 %>% group_by(Nodes, Delta) %>% summarise(lower = quantile(Time2)[2], upper = quantile(Time2)[4])
data2 = data0 %>% inner_join(stats, by = c("Nodes", "Delta")) %>% filter(Time2 >= lower & Time2 <= upper)
data3 = data2 %>% group_by(Nodes, Delta) %>% summarise(Time = mean(Time2), SD = sd(Time2))

title = "Multinode Scale Up by Delta without outliers [Berlin_N40K, Berlin_N80K, Berlin_N120K, Berlin_N160K, 4 cores per node, 1 thread per core]"
h = ggplot(data=data3, aes(x=factor(Delta), y=Time, fill=Nodes)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) +
  geom_errorbar(aes(ymin=Time-SD, ymax=Time+SD), width=.1, position=position_dodge(width = 0.75)) + ylim(0, YLIM_MAX) +
  labs(title=title, y="Time(s)", x=expression(paste(delta,"(length in timestamps)"))) 

if(SAVE_PDF){
  ggsave(paste0(RESEARCH_HOME, RESULTS_PATH, RESULTS_NAME, '_outliers.pdf'), width = 15, height = 8.5, dpi = 150, units = "in", device='pdf', h)
} else {
  plot(h)
}
options(op)