library(tidyverse)

RESEARCH_HOME = "/home/and/Documents/PhD/Research/"
FILES_PATH = "Scripts/R/Benchmarks/MultiAndSingleNode/R12/dblab/"
MONITOR_FILE = "monitor.log"
NOHUP_FILE = "nohup.out"
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

checkF <- function(ID, Nodes, StageID, Stage, TS, Time, Start, End){ 
  check = Start < Time && Time < End
  x <- as_tibble(check) 
}

getTimeInterval <- function(monitor){
  join = monitor %>% left_join(nohupTimeintervals, by = "ID")
  checks = join %>% select(ID, Nodes, StageID, Stage, TS, Time, Start, End) %>% pmap_dfr(checkF)
  starts = bind_cols(join, checks) %>% filter(value) %>% 
    mutate(Stage = paste0(str_pad(StageID,6,"left","0"),"_",Stage)) %>%
    select(Stage, Nodes, Time, TS) %>% 
    arrange(Time)
  return(starts)
}

lines = readLines(paste0(RESEARCH_HOME, FILES_PATH, MONITOR_FILE))
lines = lines[grepl("\\|SCALE\\|", lines)]
monitor = as_tibble(lines) %>%
  separate(value, into=c("Timestamp", "Scale", "Time", "ID", "Nodes", "StageID", "Stage", "RDDs", "Task", "Dura", "Load"), sep="\\|") %>%
  separate(ID, into=c(NA, NA, "ID"), sep="-") %>%
  mutate(ID = as.numeric(ID)) %>%
  filter(StageID != -1) %>%
  select(ID, Time, Nodes, StageID, Stage, RDDs, Task, Load) %>%
  mutate(Time=as.numeric(Time), RDDs=as.numeric(RDDs), Tasks=as.numeric(Task), Load=as.numeric(Load)) %>%
  group_by(ID, Time, Nodes, StageID, Stage) %>% summarise(RDDs=mean(RDDs), Tasks=mean(Tasks), Load=mean(Load)) 
monitor$Nodes = (monitor$ID %% 3) + 1
monitor = getTimeInterval(monitor)
head(monitor)

nodes = monitor %>% ungroup %>% 
  select(Nodes, StageID, Stage, Time, TS) %>%
  mutate(Nodes = factor(Nodes)) %>%
  group_by(Nodes, TS, StageID, Stage) %>%
  summarise(Time = mean(Time)) %>%
  arrange(TS, Stage)
head(nodes)

unique_stages = nodes %>% group_by(Stage) %>% tally() %>% filter(n == 3)

stages = unique_stages %>% inner_join(nodes, by = "Stage")

p = ggplot(data = stages, aes(x = Stage, y = Time, fill = Nodes)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  facet_grid(~TS) +
  theme(axis.text.x = element_text(angle = 90, hjust = 1))
plot(p)
