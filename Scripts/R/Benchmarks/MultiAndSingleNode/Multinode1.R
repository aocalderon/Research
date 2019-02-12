#!/usr/bin/Rscript

require(ggplot2)
require(stringr)
require(tidyverse)

READ_DATA     = T
SAVE_PDF      = F
SEP           = ";"
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))

dataFile = paste0(RESEARCH_HOME, 'Scripts/R/Benchmarks/MultiAndSingleNode/multinode_Gap50K.txt')

data = readLines(dataFile)

data = as.tibble(as.data.frame(data), stringAsFactors = F) %>% 
  rename(Line = data) %>% 
  filter(grepl("PFLOCK;", Line)) %>% 
  separate(Line, c("Bogus", "Nodes", "Epsilon", "Mu", "Delta", "Time", "Load"), sep = ";") %>%
  select(Nodes, Epsilon, Time, Load) %>%
  mutate(Epsilon = as.numeric(Epsilon), Time = as.numeric(Time), Load = as.numeric(Load)) %>%
  group_by(Nodes, Epsilon) %>% summarise(Time = mean(Time))

title = "Multinode Scale up [berlin_N20K-60K]"
g = ggplot(data=data, aes(x=factor(Epsilon), y=Time, fill=Nodes)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  labs(title=title, y="Time(s)", x=expression(paste(epsilon,"(mts)"))) 
if(SAVE_PDF){
  ggsave("./MultinodeScaleUp.pdf", width = 7, height = 4, dpi = 300, units = "in", device='pdf', g)
} else {
  plot(g)
}
