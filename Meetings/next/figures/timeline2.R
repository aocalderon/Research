require(tidyverse)
require(lubridate)

id = 17
log = read_tsv(paste0("mapper",id,".tsv"))

minTime = min(as.numeric(log$launchTime))
timeline = log %>%
  mutate(coreId = as.factor(coreId), host = as.factor(host), taskId = as.factor(taskId), duration = as.numeric(duration)) %>%
  mutate(start = as_datetime(launchTime/1000), end = as_datetime(finishTime/1000)) %>%
  select(coreId, host, taskId, start, end, duration) 

t = timeline %>% filter(duration > 25)

p =ggplot(t, aes(x=end, y=coreId)) + 
  geom_segment(aes(x=start, xend=end, y=coreId, yend=coreId, color = host), size=2) +
  geom_point(aes(x=end, y=coreId), shape=">", size=3, data = t) +
  geom_point(aes(x=start, y=coreId), shape="|", size=2, data = t) +
  facet_wrap(~host, ncol = 1) + 
  theme(legend.position = "none")
p
ggsave(paste0("TasksHist",id,".pdf"), width = 12, height = 20, device = "pdf")