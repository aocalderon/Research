require(tidyverse)

taskFields1 = c("Title", "StageId", "Stage", "TaskId", "Executors", "Cores", "Partitions", "Duration", "Start", "Host", "Locality", "executorRuntime", "resultSize",
               "BytesRead",           "RecordsRead",        "BytesWritten",        "RecordsWritten",
               "ShuffleBytesRead",    "ShuffleRecordsRead", "ShuffleBytesWritten", "ShuffleRecordsWritten", "appID")
getTasks <- function(filename){
  tasks = enframe(read_lines(filename), name = "n", value = "line") %>% select(line) %>%
    filter(grepl("TASKS", line)) %>%
    separate(line, taskFields1, sep = "\\|") %>%
    separate(appID, c(NA,NA,"appID"), sep="-") %>%
    mutate(Stage = str_trim(Stage), Duration = as.numeric(Duration), Partitions = as.numeric(Partitions)) %>%
    mutate(executorRuntime = as.numeric(executorRuntime), resultSize = as.numeric(resultSize)) %>%
    mutate(BytesRead=as.numeric(BytesRead),RecordsRead=as.numeric(RecordsRead),BytesWritten=as.numeric(BytesWritten),RecordsWritten=as.numeric(RecordsWritten)) %>%
    mutate(ShuffleBytesRead=as.numeric(ShuffleBytesRead),ShuffleRecordsRead=as.numeric(ShuffleRecordsRead),ShuffleBytesWritten=as.numeric(ShuffleBytesWritten),ShuffleRecordsWritten=as.numeric(ShuffleRecordsWritten)) %>%
    select(appID, StageId, Stage, TaskId, Duration, Host, Locality, executorRuntime, resultSize,
           BytesRead, BytesWritten, RecordsRead, RecordsWritten, 
           ShuffleBytesRead, ShuffleBytesWritten, ShuffleRecordsRead, ShuffleRecordsWritten)  
  return(tasks)
}

tasks1 = getTasks("~/tmp/t187_1.txt") %>% filter(as.numeric(StageId) == 45 & Duration > 100) %>% group_by(Host) %>% tally()

p = ggplot(data = tasks1, aes(x = Host, y = n)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1))
plot(p)

tasks3 = getTasks("~/tmp/t187_3.txt") %>% filter(as.numeric(StageId) == 45 & Duration > 100) %>% group_by(Host) %>% tally()

p = ggplot(data = tasks3, aes(x = Host, y = n)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1))
plot(p)
