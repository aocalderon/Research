library(tidyverse)

summariseFE <- function(data_path, eps){
  data = enframe(read_lines(data_path), name = "n", value = "line") %>% select(line) %>%
    mutate(i = rep(1:18, 101)) %>%
    filter(i %% 2 == 0) %>%
    mutate(Instant = rep(0:100, each = 9))
  
  fields_mf = c("Timestamp", "Tag", "appId", "Cores", "Executors", "Status", "Timer", "Stage", "Time", "Load", "Dataset", "Instant2")
  data_mf = data %>% filter(grepl("\\|MF\\|", line)) %>% separate(line, into = fields_mf, sep = "\\|") %>% select("Instant", "Time")
  fields_pe = c("Timestamp", "Tag", "Stage", "Time", "Load", "Status")
  data_pe = data %>% filter(grepl("\\|PE\\|", line)) %>% separate(line, into = fields_pe, sep = "\\|") %>% select("Instant", "Time")
  
  data_fe = rbind(data_mf, data_pe) %>% 
    mutate(Instant = as.numeric(Instant), Time = as.numeric(Time)) %>%
    filter(Instant > 0) %>%
    group_by(Instant) %>% 
    summarise(Time = sum(Time)) %>%
    summarise(Time = mean(Time)) %>% 
    mutate(Epsilon = eps, Method = "Window")
  return(data_fe)
}

data_path = "~/Documents/PhD/Research/Validation/FFvsFE/FE_10a"
FE_10 = summariseFE(data_path, 10)
data_path = "~/Documents/PhD/Research/Validation/FFvsFE/FE_15a"
FE_15 = summariseFE(data_path, 15)
data_path = "~/Documents/PhD/Research/Validation/FFvsFE/FE_20a"
FE_20 = summariseFE(data_path, 20)
data_path = "~/Documents/PhD/Research/Validation/FFvsFE/FE_25a"
FE_25 = summariseFE(data_path, 25)

FE = rbind(FE_10,FE_15,FE_20,FE_25)