library(tidyverse)

stageFields = c("STAGES","StageId","Stage","Executors","Cores","Start","End","Partitions","executorRuntime","executorCputime",
              "inputBytes","inputRecords","shuffleReadBytes","shuffleReadRecords","appID")
getStages <- function(filename){
  stages = as_tibble(readLines(filename)) %>%
    filter(grepl("STAGES", value)) %>%
    separate(value, stageFields, sep = "\\|") %>%
    separate(appID, c(NA,NA,"appID"), sep="-") %>%
    mutate(executorRuntime = as.numeric(executorRuntime), executorCputime = as.numeric(executorCputime), Partitions = as.numeric(Partitions),
            Stage = str_trim(Stage, side = "both"),
            StageId = as.numeric(StageId), inputBytes = as.numeric(inputBytes), inputRecords = as.numeric(inputRecords),
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
    separate(value, taskFields1, sep = "\\|") %>%
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

taskFields2 = c("Title", "StageId", "Stage", "TaskId", "Executors", "Cores", "Partitions", "Duration", "Start", "Host", "Locality", "executorRuntime", 
               "BytesRead", "RecordsRead", "appID")
getTasks2 <- function(filename){
  tasks = as_tibble(readLines(filename)) %>%
    filter(grepl("TASKS", value)) %>%
    separate(value, taskFields2, sep = "\\|") %>%
    separate(appID, c(NA,NA,"appID"), sep="-") %>%
    mutate(Stage = str_trim(Stage), Duration = as.numeric(Duration), Partitions = as.numeric(Partitions)) %>%
    mutate(executorRuntime = as.numeric(executorRuntime)) %>%
    mutate(StageId = as.numeric(StageId), TaskId = as.numeric(TaskId)) %>%
    mutate(BytesRead=as.numeric(BytesRead),RecordsRead=as.numeric(RecordsRead)) %>%
    select(appID, Executors, StageId, Stage, TaskId, Duration, Host, Locality, executorRuntime, BytesRead, RecordsRead)  
  return(tasks)
}

getTasksStats <- function(nohup, cores, executors, epsilon){
  bind_rows(getAppIDs(nohup, cores, executors, epsilon) %>% map(getTasks2)) %>%
    group_by(appID, StageId, Stage, Executors, Host) %>% 
    summarise(N=n(), 
              Duration = sum(Duration)/1000.0, 
              Bytes = sum(BytesRead)/1024.0, 
              Records = sum(RecordsRead)) %>%
    ungroup() %>%
    select(appID, StageId, Stage, Executors, Host, N, Duration)
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

getAppIDs <- function(nohup, cores, executors, epsilon){
  pattern = paste0("\\|",cores,"\\|",executors,"\\|",epsilon)
  prefix = "/home/and/Documents/PhD/Research/Scripts/R/R14/apps/app-"
  sufix  = "_info.tsv"
  apps = as_tibble(readLines(nohup)) %>%
    filter(grepl("PFLOCK", value)) %>%
    filter(grepl(pattern,value)) %>%
    separate(value, customExecutionTimeFields, sep = "\\|")  %>%
    separate(appID, c(NA,NA,"appID"), sep="-") %>%
    select(appID) %>%
    map(function(x){ paste0(prefix,x,sufix)})
  return(as.vector(apps$appID))
}

