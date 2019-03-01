#!/usr/bin/Rscript

require(ggplot2)
require(stringr)
require(tidyverse)

READ_DATA     = T
SAVE_PDF      = T
SEP           = ";"
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))

dataFile = paste0(RESEARCH_HOME, 'Scripts/Misc/MultinodeSpeedUp.txt')

data = readLines(dataFile)

data = as.tibble(as.data.frame(data), stringAsFactors = F) %>% 
  rename(Line = data) %>% 
  filter(grepl("PFLOCK;", Line)) %>% 
  separate(Line, c("Bogus", "Cores", "Nodes", "Epsilon", "Mu", "Delta", "Time", "Load", "Id"), sep = ";") %>%
  select(Nodes, Epsilon, Time) %>%
  mutate(Epsilon = as.numeric(Epsilon), Time = as.numeric(Time)) %>%
  group_by(Nodes, Epsilon) %>% summarise(Time = mean(Time))

title = "Multinode Speed up [berlin_N80K]"
g = ggplot(data=data, aes(x=factor(Epsilon), y=Time, fill=Nodes)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  labs(title=title, y="Time(s)", x=expression(paste(epsilon,"(mts)"))) 
if(SAVE_PDF){
  ggsave("./MultinodeSpeedUp.pdf", width = 7, height = 4, dpi = 300, units = "in", device='pdf', g)
} else {
  plot(g)
}
