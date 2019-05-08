library(tidyverse)
library(plotly)

RESEARCH_HOME = "/home/and/Documents/PhD/Research/"
FILES_PATH = "Scripts/R/Benchmarks/MultiAndSingleNode/R12/dblab/"
MONITOR_FILE = "monitor.log"
NOHUP_FILE = "nohup.out"
SEPARATOR_ID = "-"

lines = readLines(paste0(RESEARCH_HOME, FILES_PATH, MONITOR_FILE))
lines = lines[grepl("\\|TASKS\\|", lines)]
fields = c("Timestamp", "Title", "Time", "ID", "Nodes", "executorID", "executor", "StageID", "Stage", "TaskID", "Locality", "Start", "Duration", "Load", "Status")
tasks = as_tibble(lines) %>%
  separate(value, into=fields, sep="\\|") %>%
  separate(ID, into=c(NA, NA, "ID"), sep = SEPARATOR_ID) 

tasks %>% filter(Duration != "nan") %>% 
  separate(Duration, into=c("Duration", "Unit"), sep=" ") %>% 
  filter(Unit == "s" && Status == "SUCCESS") %>% 
  mutate(Duration=as.numeric(Duration)) %>% 
  select(ID, Nodes, executor, TaskID, StageID, Stage, Load, Duration) %>% 
  distinct() %>% 
  filter(Duration > 20) 
