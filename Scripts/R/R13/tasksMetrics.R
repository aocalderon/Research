library(tidyverse)

taskFields = c("Title", "StageId", "Stage", "TaskId", "Executors", "Cores", "Partitions", "Duration", "Start", "Host", "Locality", "executorRuntime", "resultSize",
           "BytesRead",           "RecordsRead",        "BytesWritten",        "RecordsWritten",
           "ShuffleBytesRead",    "ShuffleRecordsRead", "ShuffleBytesWritten", "ShuffleRecordsWritten", "appID")
stageFields = c("STAGES","StageId","Stage","Executors","Cores","Start","End","Partitions","executorRuntime","executorCputime",
              "inputBytes","inputRecords","shuffleReadBytes","shuffleReadRecords","appID"
)

getStages <- function(filename){
  stages = as_tibble(readLines(filename)) %>%
    filter(grepl("STAGES", value)) %>%
    separate(value, stageFields, sep = "\\|") %>%
    separate(appID, c(NA,NA,"appID"), sep="-") %>%
    mutate(executorRuntime = as.numeric(executorRuntime), executorCputime = as.numeric(executorCputime), Partitions = as.numeric(Partitions),
            inputBytes = as.numeric(inputBytes), inputRecords = as.numeric(inputRecords),
            shuffleReadBytes = as.numeric(shuffleReadBytes), shuffleReadRecords = as.numeric(shuffleReadRecords),
            Start = parse_datetime(str_replace(str_replace(Start, "GMT", ""), "T", " ")),
            End   = parse_datetime(str_replace(str_replace(End,   "GMT", ""), "T", " "))) %>%
    mutate(Duration = as.numeric(End - Start))
  return(stages)
}

getTasks <- function(filename){
  tasks = as_tibble(readLines(filename)) %>%
    filter(grepl("TASKS", value)) %>%
    separate(value, taskFields, sep = "\\|") %>%
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
  select(StageId, Stage, Executors.x, Executors.y, executorRuntime.x, executorRuntime.y, Duration.x, Duration.y)
