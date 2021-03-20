require(tidyverse)
require(rvest)

parseTime <- function(str){
  if(str == ""){
    return(0)
  } 
  arr = str_split(str, " ")
  d = as.numeric(arr[[1]][1])
  if(arr[[1]][2] == "ms"){
    d = d / 1000.0
  }
  if(arr[[1]][2] == "min"){
    d = d * 60
  }
  
  return(d)
}

parseSize <- function(str){
  if(str == ""){
    return(0)
  } 
  arr = str_split(str, " ")
  d = as.numeric(arr[[1]][1])
  if(arr[[1]][2] == "KB"){
    d = d * 1e3
  } else if(arr[[1]][2] == "MB"){
    d = d * 1e6
  } 
  return(d)
}

getTableData <- function(appId, stageId) {
  theurl = paste0("http://localhost:18081/history/", appId, "/stages/stage/?id=", stageId, "&attempt=0&task.sort=Duration&task.desc=true&task.pageSize=250")
  webpage <- read_html(theurl)
  tbls <- html_nodes(webpage, "table")
  
  fields = c("index", "Id", "Attempt", "Status", "Locality", "Executor", "Host", "Launch", "Duration", "ScheduleDelay", "Deserialization", "GC", "Serialization", "GettingResults", "PeakExecutionMemory", "FetchWait", "SizeAndRecords", "SuffleReadRemote", "Errors")
  tasks0 = webpage %>%
    html_nodes("#task-table") %>% 
    html_table(fill = TRUE) %>% .[[1]] 
  names(tasks0) = fields

  tasks = tasks0 %>% separate(Host, into = c("Host", NA, NA), sep = "\n") %>%
    separate(SizeAndRecords, into = c("Size", "Records"), sep = " / ") %>%
    mutate(Duration = Duration %>% map(parseTime), ScheduleDelay = ScheduleDelay %>% map(parseTime)) %>%
    mutate(Deserialization = Deserialization %>% map(parseTime), Serialization = Serialization %>% map(parseTime)) %>%
    mutate(GC = GC %>% map(parseTime), FetchWait = FetchWait %>% map(parseTime), GettingResults = GettingResults %>% map(parseTime)) %>%
    mutate(Size = Size %>% map(parseSize), SuffleReadRemote = SuffleReadRemote %>% map(parseSize)) %>%
    unite("Executor", Host:Executor, sep = ":") %>% 
    mutate(Duration = as.numeric(Duration))

  return(tasks)  
}

getCellsData <- function(appId){
  log = enframe(readLines("~/Research/Scripts/Scala/PFlocks/cells.txt"))
  params = log %>% filter(grepl(value, pattern = appId)) %>% filter(grepl(value, pattern = "E=")) %>%
    separate(value, into = c(NA, NA, NA, "params"), sep = "\\|") %>%
    separate(params, into = c("E", "M", "P", "ME", "I"), sep = "\t") %>%
    separate(E, into = c(NA, "epsilon"), sep = "=") %>% 
    separate(epsilon, into = c("epsilon", NA), sep = "\\.") %>%
    separate(M, into = c(NA, "mu"), sep = "=") %>%
    separate(P, into = c(NA, "partitions"), sep = "=") %>%
    separate(ME, into = c(NA, "entries"), sep = "=") %>%
    separate(I, into = c(NA, "input"), sep = "=") %>%
    select(epsilon, mu, partitions, entries, input) %>%
    slice(1) %>% 
    unlist(., use.names=FALSE)
  filename = paste0("edgesGrids_", params[5],"_ME", params[4], "_E", params[1], "_M", params[2], "_P", params[3], ".wkt")
  print(filename)
  cells = read_tsv(paste0("~/tmp/grids/", filename), col_names = c("wkt", "n", "index"))
  return(cells)
}

appId = "application_1615435002078_0370"
stageId = 5
tasksBFE = getTableData(appId, stageId) %>% select(index, Duration) %>% rename(BFE_time = Duration)

appId = "application_1615435002078_0371"
stageId = 5
tasksCMBC = getTableData(appId, stageId) %>% select(index, Duration) %>% rename(CMBC_time = Duration)

cells = getCellsData(appId)

data = cells %>% inner_join(tasksBFE, by = c("index") ) %>% inner_join(tasksCMBC, by = c("index")) 

# p = ggplot(data = data %>% filter(BFE_time > 1), aes(x = factor(index), y = Duration)) +
#   geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
#   theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
#   labs(x="Top longest partitions", y="Time [s]", title="Execution time for partitions during a distance self-join") 
# plot(p)

data %>% write_delim(path = paste0(appId, ".wkt"), delim = "\t")
