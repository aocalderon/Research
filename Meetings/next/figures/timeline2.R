require(tidyverse)
require(lubridate)

log = read_tsv("mapper.tsv")

minTime = min(as.numeric(log$launchTime))
timeline = log %>%
  mutate(coreId = as.factor(coreId), host = as.factor(host), taskId = as.factor(taskId), duration = as.numeric(duration)) %>%
  mutate(start = as.numeric(launchTime), end = as.numeric(finishTime)) %>%
  select(coreId, host, taskId, start, end, duration) 

t = timeline #%>% filter(host == "mr-08")

p =ggplot(t, aes(x=end, y=coreId)) + 
  geom_segment(aes(x=start, xend=end, y=coreId, yend=coreId, color = host), size=2) +
  geom_point(aes(x=end-7.5, y=coreId), shape=">", size=3, data = t) +
  geom_point(aes(x=start, y=coreId), shape="|", size=2, data = t) +
  facet_wrap(~host, ncol = 1)
p
ggsave("TasksHist.pdf", width = 10, height = 20, device = "pdf")