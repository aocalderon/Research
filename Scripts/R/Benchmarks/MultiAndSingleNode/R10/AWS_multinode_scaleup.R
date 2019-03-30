#!/usr/bin/Rscript

require(ggplot2)
require(stringr)
require(tidyverse)

READ_DATA     = T
SAVE_PDF      = F
SEP           = ";"
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))
RESULTS_PATH = "Scripts/R/Benchmarks/MultiAndSingleNode/R10/"
RESULTS_NAME = "AWS_multinode_scaleup"
dataFile = paste0(RESEARCH_HOME, RESULTS_PATH, RESULTS_NAME, '.txt')

data = readLines(dataFile)

data = as_tibble(as.data.frame(data), stringAsFactors = F) %>% 
  rename(Line = data) %>% 
  filter(grepl("PFLOCK;", Line)) %>% 
  separate(Line, c("Bogus", "Cores", "Nodes", "Epsilon", "Mu", "Delta", "Time", "Load", "Id"), sep = SEP) %>%
  select(Nodes, Epsilon, Time) %>%
  mutate(Epsilon = as.numeric(Epsilon), Time2 = as.numeric(Time)) 

data2 = data %>%
  group_by(Nodes, Epsilon) %>% summarise(Time = mean(Time2), SD = sd(Time2))

title = "Multinode Scale up [Berlin_N40K, Berlin_N80K, Berlin_N120K, Berlin_N160K]"
g = ggplot(data=data2, aes(x=factor(Epsilon), y=Time, fill=Nodes)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) +
  geom_errorbar(aes(ymin=Time-SD, ymax=Time+SD), width=.1, position=position_dodge(width = 0.75)) +
  labs(title=title, y="Time(s)", x=expression(paste(epsilon,"(mts)"))) 
if(SAVE_PDF){
  ggsave(paste0(RESEARCH_HOME, RESULTS_PATH, RESULTS_NAME, '_bars.pdf'), width = 15, height = 8.50, dpi = 150, units = "in", device='pdf', g)
} else {
  plot(g)
}

title = "Boxplot by Stage [Berlin_N40K, Berlin_N80K, Berlin_N120K, Berlin_N160K]"
f = ggplot(data=data, aes(x=factor(Epsilon), y=Time2, fill=Nodes)) +
  stat_boxplot(geom ='errorbar', width = 0.5, position = position_dodge(1)) +
  geom_boxplot(outlier.size = 0.5, position = position_dodge(1)) +
  labs(title=title, y="Time(s)", x="Stage")
if(SAVE_PDF){
  ggsave(paste0(RESEARCH_HOME, RESULTS_PATH, RESULTS_NAME, '_boxplot.pdf'), width = 15, height = 8.50, dpi = 150, units = "in", device='pdf', f)
} else {
  plot(f)
}