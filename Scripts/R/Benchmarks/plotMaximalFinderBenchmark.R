#!/usr/bin/Rscript

require(ggplot2)
require(stringr)
require(tidyverse)
require(lubridate)

RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))
RESEARCH_PATH = "Scripts/Misc/"
DATASET       = "benchmark_E70-120"
EXTENSION     = "txt"
SAVE_PDF      = T
options(digits.secs = 6)

dataFile = paste0(RESEARCH_HOME, RESEARCH_PATH, DATASET, ".", EXTENSION)

base = as.tibble(readLines(dataFile)) %>% 
  filter(grepl("[A-H]\\.", value)) %>% 
  separate(value, c("Timestamp", "Stage", "Time", "Load", "Epsilon", "Framework", "Run"), 
           sep = "\\s->\\s|\\|") %>% 
  mutate(Stage = str_trim(Stage), Time = as.numeric(str_trim(Time)), 
         Load = as.numeric(str_trim(Load)), Epsilon = as.numeric(Epsilon), 
         Run = as.numeric(Run)) %>% 
  select(Framework, Run, Epsilon, Stage, Time) 
data = base %>% group_by(Framework, Run, Epsilon) %>% summarise(Time = sum(Time)) %>% 
  group_by(Framework, Epsilon) %>% summarise(Time = mean(Time))

title = "Execution time maximal finder..."
h = ggplot(data=data, aes(x=factor(Epsilon), y=Time, fill=Framework)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  labs(title=title, y="Time(s)", x=expression(paste(epsilon,"(mts)")))
if(SAVE_PDF){
  plotName = paste0(RESEARCH_HOME, RESEARCH_PATH, DATASET, ".pdf")
  ggsave(plotName, width = 7, height = 5, dpi = 300, units = "in", device='pdf', h)
} else {
  plot(h)
}

stages = base %>% filter(Framework == "GeoSpark") %>% select(Stage) %>% distinct(Stage) %>% 
  separate(Stage, c("id", NA), sep = "\\.", remove = F) %>% select(id, Stage)
data = base %>% group_by(Framework, Epsilon, Stage) %>% summarise(Time = mean(Time)) %>%
  separate(Stage, c("id", NA), sep = "\\.") %>% 
  inner_join(stages) %>% select(Framework, Epsilon, Stage, Time)

title = expression("Execution time maximal finder by"~epsilon~" (values in meters)...")
g = ggplot(data=data, aes(x=Stage, y=Time, fill=Framework)) + coord_flip() +
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  labs(title=title, y="Time(s)", x="Stages") +
  facet_wrap(vars(factor(Epsilon)), scales = "free_x")
if(SAVE_PDF){
  plotName = paste0(RESEARCH_HOME, RESEARCH_PATH, DATASET, "_ByStage.pdf")
  ggsave(plotName, width = 7, height = 5, dpi = 300, units = "in", device='pdf', g)
} else {
  plot(g)
}
