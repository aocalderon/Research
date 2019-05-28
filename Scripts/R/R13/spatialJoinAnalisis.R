source("/home/and/Documents/PhD/Research/Scripts/R/R13/tasksMetrics.R")

app270 = "/home/and/Documents/PhD/Research/Validation/Scaleup/logs/app-0270_info.tsv"
tasks270 = getTasks(app270)
summary(tasks270)

stageByShuffle270 = tasks270 %>% select(StageId, ShuffleRecordsRead) %>% group_by(StageId) %>% summarise(ShuffleRecordsRead = sum(ShuffleRecordsRead))
p = ggplot(data = stageByShuffle270, aes(x = StageId, y = ShuffleRecordsRead)) + geom_point()
plot(p)

app272 = "/home/and/Documents/PhD/Research/Validation/Scaleup/logs/app-0272_info.tsv"
tasks272 = getTasks(app272)
summary(tasks272)

stageByShuffle272 = tasks272 %>% select(StageId, ShuffleRecordsRead) %>% group_by(StageId) %>% summarise(ShuffleRecordsRead = sum(ShuffleRecordsRead))
p = ggplot(data = stageByShuffle272, aes(x = StageId, y = ShuffleRecordsRead)) + geom_point()
plot(p)

stages270 = getStages(app270)
stages272 = getStages(app272)

stages = stages270 %>% inner_join(stages272, by = c("StageId", "Stage")) %>% 
  select(StageId, Stage, Executors.x, Executors.y, executorRuntime.x, executorRuntime.y, Duration.x, Duration.y, shuffleReadRecords.x, shuffleReadRecords.y)

print(stages)
