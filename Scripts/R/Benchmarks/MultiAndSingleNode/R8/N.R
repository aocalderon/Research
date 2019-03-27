#!/usr/bin/Rscript

require(ggplot2)
require(ggrepel)
require(stringr)
require(tidyverse)

READ_DATA     = T
SAVE_PDF      = F
SEP           = "\\|"
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))
RESULTS_PATH = "Scripts/R/Benchmarks/MultiAndSingleNode/R8/"
RESULTS_NAME = "N"
dataFile = paste0(RESEARCH_HOME, RESULTS_PATH, RESULTS_NAME, '.tsv')

data = readLines(dataFile)

data = as_tibble(as.data.frame(data), stringAsFactors = F) %>% 
  rename(Line = data) %>%
  separate(Line, c("Stage", "Time", "Load", "Nodes"), sep = SEP) %>%
  select(Stage, Nodes, Time) %>%
  mutate(Stage = str_trim(Stage), Time = as.numeric(Time)) 

title = "Execution time by Stage [Berlin_40K vs Berlin_80K, Epsilon=110, Mu=3, Delta=3]"
g = ggplot(data=data, aes(x=Stage, y=Time, fill=Nodes)) + 
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  theme(axis.text.x = element_text(hjust=0, vjust=0.2, angle=270)) +
  labs(title=title, y="Time(s)", x="Stage")

if(SAVE_PDF){
  ggsave(paste0(RESEARCH_HOME, RESULTS_PATH, RESULTS_NAME, '_T1.pdf'), width = 8, height = 5, dpi = 300, units = "in", device='pdf', g)
} else {
  plot(g)
}
