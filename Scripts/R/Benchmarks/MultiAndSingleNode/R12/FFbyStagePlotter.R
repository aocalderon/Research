library(tidyverse)

RESEARCH_HOME = "/home/and/Documents/PhD/Research/"
FILES_PATH    = "Scripts/R/Benchmarks/MultiAndSingleNode/R12/dblab/"
MONITOR_FILE  = "monitor.log"
NOHUP_FILE    = "nohup.out"
SEPARATOR_ID  = "-"

lines = readLines(paste0(RESEARCH_HOME, FILES_PATH, NOHUP_FILE))
lines = lines[grepl("\\|[1-6]\\.", lines)]
nohup = as_tibble(lines) %>%
  separate(value, into=c("Timestamp", "ID", "Time", "Stage", "Duration", "Load", "TS"), sep="\\|") %>%
  separate(ID, into=c(NA, NA, "ID"), sep = SEPARATOR_ID) %>%
  mutate(ID = as.numeric(ID), Time = as.numeric(Time), Duration = as.numeric(Duration)) 

nohupTimeintervals = nohup %>% select(ID, TS, Time, Duration) %>%
  mutate(Start = Time - Duration, End = Time) %>%
  group_by(ID, TS) %>% summarise(Start = min(Start), End = max(End))

checkF <- function(ID, Stage, TS, Time, Start, End){ 
  check = Start < Time && Time < End
  x <- as_tibble(check) 
}

getStagePlugTimestamp <- function(monitor){
  join = monitor %>% ungroup() %>% left_join(nohupTimeintervals, by = "ID")
  checks = join %>% select(ID, Stage, TS, Time, Start, End) %>% pmap_dfr(checkF)
  starts = cbind(join, checks) %>% filter(value) %>% 
    select(Stage, Nodes, Time) %>% 
    arrange(Time)
  return(starts)
}

lines = readLines(paste0(RESEARCH_HOME, FILES_PATH, MONITOR_FILE))
lines = lines[grepl("\\|SCALE\\|", lines)]
monitor = as_tibble(lines) %>%
  separate(value, into=c("Timestamp", "Scale", "Time", "ID", "Nodes", "Stage", "RDDs", "Task", "Dura", "Load"), sep="\\|") %>%
  separate(ID, into=c(NA, NA, "ID"), sep = SEPARATOR_ID) %>%
  mutate(ID = as.numeric(ID)) %>%
  select(ID, Time, Nodes, Stage, RDDs, Task, Load) %>%
  mutate(Time=as.numeric(Time), RDDs=as.numeric(RDDs), Tasks=as.numeric(Task), Load=as.numeric(Load)) %>%
  group_by(ID, Time, Nodes, Stage) %>% summarise(RDDs=mean(RDDs), Tasks=mean(Tasks), Load=mean(Load)) %>%
  filter(Stage != "")
head(monitor)

monitor[monitor$ID == 0, "Nodes"] = 1
monitor[monitor$ID == 1, "Nodes"] = 2
monitor[monitor$ID == 2, "Nodes"] = 3

stages = getStagePlugTimestamp(monitor)

nodes = stages %>% ungroup %>% 
  select(Nodes, Stage, Time) %>%
  mutate(Nodes = factor(Nodes)) %>%
  group_by(Nodes, Stage) %>%
  summarise(Time = mean(Time)) %>%
  arrange(Time)

p = ggplot(data = nodes, aes(x = Stage, y = Time, fill = Nodes)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1))
plot(p)
