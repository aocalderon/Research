#!/usr/bin/Rscript

require(ggplot2)
require(stringr)
require(tidyverse)

READ_DATA     = T
SAVE_PDF      = T
SEP           = ";"
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))

dataFile = paste0(RESEARCH_HOME, 'Scripts/R/Benchmarks/MultiAndSingleNode/singlenode3.txt')

data = readLines(dataFile)

data = as.tibble(as.data.frame(data), stringAsFactors = F) %>% 
  rename(Line = data) %>% 
  filter(grepl("PFLOCK;", Line)) %>% 
  separate(Line, c("Bogus", "Cores", "Epsilon", "Mu", "Delta", "Time", "Load"), sep = ";") %>%
  select(Cores, Epsilon, Time, Load) %>%
  mutate(Epsilon = as.numeric(Epsilon), Time = as.numeric(Time), Load = as.numeric(Load)) %>%
  group_by(Cores, Epsilon) %>% summarise(Time = mean(Time))

title = "Execution time by Epsilon"
data$Cores = factor(data$Cores, levels=c("1","2","3","4","5","6","7","8","9","10","11","12"))
g = ggplot(data=data, aes(x=factor(Epsilon), y=Time, fill=Cores)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  labs(title=title, y="Time(s)", x=expression(paste(epsilon,"(mts)"))) 
if(SAVE_PDF){
  ggsave("./Singlenode3.pdf", width = 7, height = 4, dpi = 300, units = "in", device='pdf', g)
} else {
  plot(g)
}
