require(tidyverse)

data = read_tsv("~/Documents/PhD/Research/Meetings/M03/FF-data_2020-01-25.txt", col_names = F) %>%
  rename(appId = X1, Phase = X2, Time = X3, Instant = X4, Coarse = X5, Finer = X6) %>%
  group_by(appId, Instant, Finer, Coarse) %>% summarise(Time = sum(Time))

top = data %>% ungroup() %>% top_n(-100, Time) %>% arrange(Time)


