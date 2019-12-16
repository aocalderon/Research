library(tidyverse)

summariseFF <- function(data_path, eps){
  fields = c("Timestamp", "Tag", "appId", "Cores", "Executors", "Status", "Timer", "Stage", "Time", "Load", "Instant")
  data = enframe(read_lines(data_path), name = "n", value = "line") %>% select(line) %>%
    separate(line, into = fields, sep = "\\|") %>%
    filter(str_trim(Status) == "END") %>%
    mutate(Instant = as.numeric(Instant), Time = as.numeric(Time)) %>%
    filter(Instant > 0) %>%
    select(Instant, Time) %>%
    group_by(Instant) %>% 
    summarise(Time = sum(Time)) %>% 
    summarise(Time = mean(Time)) %>% 
    mutate(Epsilon = eps, Method = "Time By Time")
  return(data)
}

FF_10 = summariseFF("~/Documents/PhD/Research/Validation/FFvsFE/FF_10a", 10)
FF_15 = summariseFF("~/Documents/PhD/Research/Validation/FFvsFE/FF_15a", 15)
FF_20 = summariseFF("~/Documents/PhD/Research/Validation/FFvsFE/FF_20a", 20)
FF_25 = summariseFF("~/Documents/PhD/Research/Validation/FFvsFE/FF_25a", 25)

FF = rbind(FF_10,FF_15,FF_20,FF_25)