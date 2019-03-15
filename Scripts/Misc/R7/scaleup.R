#!/usr/bin/Rscript

require(ggplot2)
require(stringr)
require(tidyverse)

READ_DATA     = T
SAVE_PDF      = F
SEP           = ";"
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))
RESULTS_PATH = "Scripts/R/Benchmarks/MultiAndSingleNode/R/"
RESULTS_NAME = "scaleup"
dataFile = paste0(RESEARCH_HOME, RESULTS_PATH, RESULTS_NAME, '.txt')

data = readLines(dataFile)

data = as.tibble(as.data.frame(data), stringAsFactors = F) %>% 
  rename(Line = data) %>% 
  filter(grepl("PFLOCK;", Line)) %>% 
  separate(Line, c("Bogus", "Cores", "Nodes", "Epsilon", "Mu", "Delta", "Time", "Load", "Id"), sep = ";") %>%
  select(Nodes, Epsilon, Time) %>%
  mutate(Epsilon = as.numeric(Epsilon), Time2 = as.numeric(Time)) %>%
  group_by(Nodes, Epsilon) %>% summarise(Time = mean(Time2), SD = sd(Time2))

title = "Multinode Scale up [Berlin_N40K, Berlin_N80K, Berlin_N120K]"
g = ggplot(data=data, aes(x=factor(Epsilon), y=Time, fill=Nodes)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  labs(title=title, y="Time(s)", x=expression(paste(epsilon,"(mts)"))) 
if(SAVE_PDF){
  ggsave(paste0(RESEARCH_HOME, RESULTS_PATH, RESULTS_NAME, '.pdf'), width = 7, height = 4, dpi = 300, units = "in", device='pdf', g)
} else {
  plot(g)
}
