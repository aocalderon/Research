#!/usr/bin/Rscript

require(ggplot2)
require(stringr)
require(tidyverse)
require(lubridate)

READ_DATA     = T
SAVE_PDF      = F
SEP           = "###"
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))
options(digits.secs = 6)

dataFile = paste0(RESEARCH_HOME, 'Scripts/Python/Tests/Test006.txt')

lines    = readLines(dataFile)
records  = c()
method   = ""
epsilon  = 0
mu       = 0
delta    = 0
time     = 0

run_id  = 0
collect = F
if(READ_DATA){
  for(line in lines){
    #print(line)
    if(grepl("PFLOCK_START", line)){
      collect = T
    }
    if(grepl("PFLOCK_END", line)){
      collect = F
      run_id = run_id + 1
    }
    if(collect){
      params = str_split_fixed(line, " -> ", 2)
      run_timestamp = str_trim(params[1])
      run_line = str_trim(params[2])
      if(run_line != ""){
        row = paste0(run_id,SEP,run_timestamp,SEP, run_line)
        #print(row)
        records = c(records, row)
      }
    }
  }
  data = as.tibble(str_split_fixed(records,SEP, 3), stringsAsFactors = F) %>%
    rename(run_id = V1, run_datetime = V2, run_line = V3) %>%
    mutate(run_id = as.numeric(run_id), run_datetime = as_datetime(str_replace(run_datetime,",",".")))
  runs = filter(data, grepl("spark-submit", run_line)) %>% 
    select(run_id, run_line) %>%
    separate(run_line, sep = "--", into = letters[1:11]) %>%
    select(run_id, c, e, g) %>%
    separate(c, sep = " ", c(NA, "Epsilon")) %>%
    separate(e, sep = " ", c(NA, "Mu")) %>%
    separate(g, sep = " ", c(NA, "Delta")) %>%
    select(run_id, Epsilon, Mu, Delta) %>%
    mutate_all(as.numeric)
  maximals = filter(data, grepl("[A-J]\\.", run_line)) %>% 
    select(run_id, run_line) %>% 
    separate(run_line, sep = " \\[", into = c("Stage", "a", "b")) %>% 
    separate(a, sep = "s]", into = c("Time", NA)) %>% 
    separate(b, sep = " ", into = c("Load", NA)) %>% 
    select(run_id, Stage, Time, Load) %>%
    mutate(Time = as.numeric(Time), Load = as.numeric(Load)) %>% 
    group_by(run_id, Stage) %>% 
    summarise(Time = mean(Time), Load = mean(Load))
    
  epsilonByStage = runs %>% inner_join(maximals, by = "run_id") %>%
    select(Epsilon, Stage, Time)
}

title = "Execution time Epsilon by Stage..."
g = ggplot(data=epsilonByStage, aes(x=factor(Epsilon), y=Time, fill=Stage)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  labs(title=title, y="Time(s)", x=expression(paste(epsilon,"(mts)")))
if(SAVE_PDF){
  ggsave("./MergeLastEpsilonByStage.pdf", g)
} else {
  plot(g)
}
