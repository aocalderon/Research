require(tidyverse)

sjoin = read_tsv("GeoTesterRDD.txt", col_names = F) %>%
  rename(appId = X1, Phase = X2, Time = X3, Grid = X4, Index = X5, Partitions = X6) %>%
  filter(Phase == "Distance self-join" & Grid == "quadtree" & Index == "quadtree" & (Partitions == 64))

tasks = read_tsv("GeoTesterRDD_tasks.txt", col_names = F) %>%
  rename(stageId = X1, Stage = X2, nTasks = X3, taskId = X4, Host = X5, Locality = X6, Time = X7, rRead = X8, bRead = X9, rWritten = X10, bWritten = X11, rShuffleRead = X12, rShuffleWritten = X13, jobId = X14, appId = X15) 

p = c(1:50,1:50,1:51,1:53,1:52,1:52,1:51,1:51,1:50,1:50)

data0 = sjoin %>% inner_join(tasks, by = "appId") %>% filter(stageId == 6) %>% select(appId, taskId, Time.y) %>% rename(Time = Time.y) %>%
  group_by(appId) %>% top_n(50, Time) %>% arrange(appId, desc(Time)) %>% ungroup() %>%
  mutate(P = p) 

data1 = data0 %>% inner_join(tasks, by = c("appId", "taskId")) 

write.csv(data1, file = "GeoTesterTaskInfo.csv")