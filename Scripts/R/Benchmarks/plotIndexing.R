#!/usr/bin/Rscript

require(ggplot2)
require(stringr)
require(tidyverse)
require(lubridate)

SAVE_PDF      = T
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))
options(digits.secs = 6)

dataFile = paste0(RESEARCH_HOME, 'Scripts/R/Benchmarks/indexing.tsv')

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
  ggsave("./Indexing.pdf", h)
} else {
  plot(h)
}