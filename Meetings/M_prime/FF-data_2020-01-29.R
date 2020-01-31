require(tidyverse)

data = read_tsv("Documents/PhD/Research/Meetings/M_prime/FF-data_2020-01-29.txt", col_names = F) %>%
  rename(appId = X1, Phase = X2, Time = X3, Instant = X4, Coarse = X5, Finer = X6) %>% 
  group_by(appId, Instant, Phase, Coarse, Finer) %>% summarise(Time = sum(Time)) 


data2 =  data %>% ungroup() %>%
  top_n(100, Time) %>% 
  arrange(Time) 

data2 %>% show()

coarse = data2 %>% group_by(Coarse) %>% tally()

finer = data2 %>% group_by(Finer) %>% tally()