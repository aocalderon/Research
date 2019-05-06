library(tidyverse)

RESEARCH_HOME = "/home/and/Documents/PhD/Research"

lines = readLines(paste0(RESEARCH_HOME, "/Scripts/R/Benchmarks/MultiAndSingleNode/R12/aws/monitor.txt"))
lines = lines[grepl("\\|TOTAL\\|", lines)]
monitor = as_tibble(lines) %>%
  separate(value, into=c("Timestamp", "Scale", "Time", "ID", "Nodes", "Stage", "RDDs", "Task", "Dura", "Load"), sep="\\|") %>%
  separate(ID, into=c(NA, NA, "ID"), sep="_") %>%
  select(Timestamp, ID) %>%
  mutate(ID = as.numeric(ID), Timestamp = parse_datetime(str_replace(str_replace(Timestamp," ", "T"), ",", "."))) 

lines = readLines(paste0(RESEARCH_HOME, "/Scripts/R/Benchmarks/MultiAndSingleNode/R12/aws/nohup.txt"))
lines = lines[grepl("\\|[A-H]\\.", lines)]
nohup = as_tibble(lines) %>%
  separate(value, into=c("Timestamp", "ID", "Time", "Stage", "Duration", "Load", "Bogus"), sep="\\|") %>%
  separate(ID, into=c(NA, NA, "ID"), sep="_") %>%
  select(ID, Stage, Timestamp) %>%
  mutate(ID = as.numeric(ID), Stage = str_trim(Stage), Timestamp = parse_datetime(str_replace(str_replace(Timestamp," ", "T"), ",", "."))) 


  