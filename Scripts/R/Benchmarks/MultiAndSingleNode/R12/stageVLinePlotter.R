library(tidyverse)

RESEARCH_HOME = "/home/and/Documents/PhD/Research/"
FILES_PATH = "Scripts/R/Benchmarks/MultiAndSingleNode/R12/dblab/"
NOHUP_FILE = "nohupDemo.out"
SEPARATOR_ID = "-"

lines = readLines(paste0(RESEARCH_HOME, FILES_PATH, NOHUP_FILE))
lines = lines[grepl("\\|[1-6]\\.", lines)]
nohup = as_tibble(lines) %>%
  separate(value, into=c("Timestamp", "ID", "Time", "Stage", "Duration", "Load", "TS"), sep="\\|") %>%
  separate(ID, into=c(NA, NA, "ID"), sep = SEPARATOR_ID) %>%
  mutate(ID = as.numeric(ID), Time = as.numeric(Time), Duration = as.numeric(Duration)) 
  
nohupStages = nohup %>% mutate(Stage = paste0(TS,".",str_trim(Stage))) %>%
  mutate(Start = Time - Duration, End = Time) %>%
  select(ID, Stage, Start, End, Duration) 

nohupTimeintervals = nohup %>% select(ID, TS, Time, Duration) %>%
  mutate(Start = Time - Duration, End = Time) %>%
  group_by(ID, TS) %>% summarise(Start = min(Start), End = max(End))