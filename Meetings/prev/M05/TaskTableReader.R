require(tidyverse)
require(rvest)


appId = "application_1579408195629_1587"
theurl = paste0("http://localhost:18081/history/", appId, "/stages/stage/?id=6&attempt=0&task.sort=Duration&task.desc=true&task.pageSize=250")
webpage <- read_html(theurl)
tbls <- html_nodes(webpage, "table")

fields = c("Index", "Id", "Attempt", "Status", "Locality", "Executor", "Host", "Launch", "Duration", "ScheduleDelay", "Deserialization", "GC", "Serialization", "GettingResults", "PeakExecutionMemory", "FetchWait", "SizeAndRecords", "SuffleReadRemote", "Errors")
tasks0 = webpage %>%
  html_nodes("#task-table") %>% 
  html_table(fill = TRUE) %>% .[[1]] 
names(tasks0) = fields

parseTime <- function(str){
  if(str == ""){
    return(0)
  } 
  arr = str_split(str, " ")
  d = as.numeric(arr[[1]][1])
  if(arr[[1]][2] == "ms"){
    d = d / 1000.0
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

tasks = tasks0 %>% separate(Host, into = c("Host", NA, NA), sep = "\n") %>%
  separate(SizeAndRecords, into = c("Size", "Records"), sep = " / ") %>%
  mutate(Duration = Duration %>% map(parseTime), ScheduleDelay = ScheduleDelay %>% map(parseTime)) %>%
  mutate(Deserialization = Deserialization %>% map(parseTime), Serialization = Serialization %>% map(parseTime)) %>%
  mutate(GC = GC %>% map(parseTime), FetchWait = FetchWait %>% map(parseTime), GettingResults = GettingResults %>% map(parseTime)) %>%
  mutate(Size = Size %>% map(parseSize), SuffleReadRemote = SuffleReadRemote %>% map(parseSize)) %>%
  unite("Executor", Host:Executor, sep = ":") %>% 
  mutate(Duration = as.numeric(Duration))
  
head(tasks)

p = ggplot(data = tasks, aes(x = factor(Index), y = Duration)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
  labs(x="Top longest partitions", y="Time [s]", title="Execution time for partitions during a distance self-join") 
plot(p)