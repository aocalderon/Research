#!/usr/bin/Rscript

require(ggplot2)
require(stringr)
require(tidyverse)

RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))

filename = "PartitionerBenchmark.txt"
dataFile = paste0(RESEARCH_HOME, 'tmp/', filename)

data = readLines(dataFile)

data = as.tibble(as.data.frame(data), stringAsFactors = F) %>% 
  rename(Line = data) %>% 
  separate(Line, c("Bogus", "Partitioner", "Partitions", "Time"), sep = ";") %>%
  select(Partitioner, Partitions, Time) %>%
  mutate(Time = as.numeric(Time))
g = ggplot(data=data, aes(x=factor(Partitions), y=Time, fill=Partitioner)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  labs(title=title, y="Time(s)", x="Number of partitions") 
plot(g)