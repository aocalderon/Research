library(tidyverse)

stageFields = c("STAGES","StageId","Stage","Executors","Cores","Start","End","Partitions","executorRuntime","executorCputime",
              "inputBytes","inputRecords","shuffleReadBytes","shuffleReadRecords","appID")
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

taskFields = c("Title", "StageId", "Stage", "TaskId", "Executors", "Cores", "Partitions", "Duration", "Start", "Host", "Locality", "executorRuntime", "resultSize",
               "BytesRead",           "RecordsRead",        "BytesWritten",        "RecordsWritten",
               "ShuffleBytesRead",    "ShuffleRecordsRead", "ShuffleBytesWritten", "ShuffleRecordsWritten", "appID")
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

customStageFields = c("Timestamp", "Part", "appID", "Executors", "Cores", "Status", "Time", "Stage", "Duration", "Load", "Interval")
getCustomStages <- function(filename, appID){
  as_tibble(readLines(filename)) %>%
    filter(grepl(paste0("app-\\d{14}-0", appID), value)) %>%
    filter(grepl("\\|\\d\\.", value)) %>%
    filter(grepl("END\\|", value)) %>%
    separate(value, customStageFields, sep = "\\|")  %>%
    separate(appID, c(NA,NA,"appID"), sep="-") %>%
    mutate(Stage = str_trim(Stage, side = "both"), Duration = as.numeric(Duration), Load = as.numeric(Load)) %>%
    mutate(Status = str_trim(Status, side = "both"), Time = as.numeric(Time))
}

customStageFields2 = c("TimestampPart", "appID", "Executors", "Cores", "Status", "Time", "Stage", "Duration", "Load", "Interval")
getCustomStages2 <- function(filename, appID){
  as_tibble(readLines(filename)) %>%
    filter(grepl(paste0("app-\\d{14}-0", appID), value)) %>%
    filter(grepl("\\|\\d\\.", value)) %>%
    filter(grepl("END\\|", value)) %>%
    separate(value, customStageFields2, sep = "\\|")  %>%
    separate(appID, c(NA,NA,"appID"), sep="-") %>%
    mutate(Stage = str_trim(Stage, side = "both"), Duration = as.numeric(Duration), Load = as.numeric(Load)) %>%
    mutate(Status = str_trim(Status, side = "both"), Time = as.numeric(Time))
}

customExecutionTimeFields = c("TimestampPart", "appID", "Cores", "Executors", "Epsilon", "Mu", "Delta", "Duration", "Load")
customExecutionTime <- function(filename){
  as_tibble(readLines(filename)) %>%
    filter(grepl("PFLOCK", value)) %>%
    separate(value, customExecutionTimeFields, sep = "\\|")  %>%
    mutate(Duration = as.numeric(Duration), Load = as.numeric(Load))
}
