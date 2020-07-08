require(tidyverse)
require(lubridate)

log = read_tsv("pflock.tsv")

minTime = min(as.numeric(log$launchTime))
timeline = log %>% select(host, index, launchTime, finishTime, duration) %>%
  mutate(host = as.factor(host), taskId = as.factor(index), duration = as.numeric(duration)) %>%
  mutate(start = as_datetime(launchTime / 1000), end = as_datetime(finishTime / 1000)) %>%
  select(host, taskId, start, end, duration) 

t = timeline %>% filter(host == "mr-08")

p =ggplot(t, aes(x=start, xend=end, y=taskId, yend=taskId, color = host)) + 
  geom_segment() +
  facet_wrap(~host)
p