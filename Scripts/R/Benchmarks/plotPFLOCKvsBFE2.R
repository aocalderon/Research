#!/usr/bin/Rscript

require(ggplot2)
require(stringr)
require(tidyverse)

READ_DATA     = T
SAVE_PDF      = T
SEP           = ";"
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))

dataFile = paste0(RESEARCH_HOME, 'Scripts/R/Benchmarks/pflockvsbfe.txt')

data = read.table(dataFile, header = F, sep = ";")

data = as.tibble(data, stringsAsFactors = F) %>%
  rename(Method = V1, Epsilon = V2, Mu = V3, Delta = V4, Time = V5, Load = V6) %>%
  mutate(Epsilon = as.numeric(Epsilon), Mu = as.numeric(Mu), Delta = as.numeric(Delta), Time = as.numeric(Time)) %>%
  select(Method, Epsilon, Time) %>%  
  group_by(Method, Epsilon) %>% summarise(Time = mean(Time))

title = "Execution time by Epsilon"
g = ggplot(data=data, aes(x=factor(Epsilon), y=Time, fill=Method)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  labs(title=title, y="Time(s)", x=expression(paste(epsilon,"(mts)"))) 
if(SAVE_PDF){
  ggsave("./BFEvsPFLOCK2.pdf", width = 7, height = 4, dpi = 300, units = "in", device='pdf', g)
} else {
  plot(g)
}
