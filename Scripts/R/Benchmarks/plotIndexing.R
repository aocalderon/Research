#!/usr/bin/Rscript

require(ggplot2)
require(stringr)
require(tidyverse)
require(lubridate)

RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))
RESEARCH_PATH = "Scripts/R/Benchmarks/"
DATASET       = "indexing"
EXTENSION     = "tsv"
SAVE_PDF      = T
options(digits.secs = 6)

dataFile = paste0(RESEARCH_HOME, RESEARCH_PATH, DATASET, ".", EXTENSION)

data = as.tibble(read.table(dataFile, sep = "|")) %>%
  rename(Epsilon = V1, Method = V2, Stage = V3, Time = V4, Load = V5, Tag = V6) %>%
  select(Epsilon, Method, Time) %>%
  mutate(Epsilon = as.numeric(Epsilon), Time = as.numeric(Time)) %>%
  group_by(Epsilon, Method) %>%
  summarise(Time = sum(Time))

title = "Execution time finding disk's centers..."
h = ggplot(data=data, aes(x=factor(Epsilon), y=Time, fill=Method)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  labs(title=title, y="Time(s)", x=expression(paste(epsilon,"(mts)")))
if(SAVE_PDF){
  plotName = paste0(RESEARCH_HOME, RESEARCH_PATH, DATASET, ".pdf")
  ggsave(plotName, width = 7, height = 5, dpi = 300, units = "in", device='pdf', h)
} else {
  plot(h)
}