require(tidyverse)

sjoin = read_tsv("GeoTesterRDD.txt", col_names = F) %>%
  rename(appId = X1, Phase = X2, Time = X3, Grid = X4, Index = X5, Partitions = X6) %>%
  filter(Phase == "Distance self-join" & Grid == "quadtree" & Index == "quadtree" & (Partitions == 64))

tasks = read_tsv("GeoTesterRDD_tasks.txt", col_names = F) %>%
  rename(stageId = X1, Stage = X2, nTasks = X3, taskId = X4, Host = X5, Locality = X6, Time = X7, jobId = X8, appId = X9) 
  
n = 12
data0 %>% group_by(appId) %>% tally() %>% select(n)
p = c(1:50,1:50,1:51,1:53,1:52,1:52,1:51,1:51,1:50,1:50)

data0 = sjoin %>% inner_join(tasks, by = "appId") %>% filter(stageId == 6) %>% select(appId, taskId, Time.y) %>% rename(Time = Time.y) %>%
  group_by(appId) %>% top_n(50, Time) %>% arrange(appId, desc(Time)) %>% ungroup() %>%
  mutate(P = p, appId = as.numeric(appId)) %>% filter(appId > 1000) %>% 
  mutate(appId = replace(appId, appId == 1053, 1)) %>%
  mutate(appId = replace(appId, appId == 1376, 2)) %>%
  mutate(appId = replace(appId, appId == 1420, 3)) %>%
  mutate(appId = replace(appId, appId == 1464, 4)) %>%
  mutate(appId = replace(appId, appId == 1508, 5)) %>%
  mutate(appId = replace(appId, appId == 1552, 6)) 
  
p = ggplot(data = data0, aes(x = factor(P), y = Time)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  facet_wrap(~appId, ncol = 1) +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
  labs(x="Top longest partitions", y="Time [s]", title="Execution time for partitions during a distance self-join", subtitle = "Showing 6 of 10 runs...") 
plot(p)
ggsave("~/Documents/PhD/Research/Meetings/M04/figures/Tasks.pdf", width = 8, height = 7, device = "pdf")

