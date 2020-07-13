require(tidyverse)

data = read_tsv("pflock4.tsv")

t0 = min(data$launchTime)

data0 = data %>% mutate(t1 = launchTime - t0, t2 = finishTime - t0) %>%
  rowwise() %>% 
  mutate(second = paste(seq(t1, t2), collapse = ' '), taskId = as.factor(index)) %>% 
  separate_rows(second, sep = " ", convert = TRUE) %>%
  select(taskId, host, second)

data1 = data0 %>% group_by(host, second) %>% count()

summary(data1)
print(table(data1$n))
