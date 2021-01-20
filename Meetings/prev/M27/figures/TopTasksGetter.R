require(tidyverse)
require(ssh)

###############
# Functions...
###############

getData <- function(ids){
  session <- ssh_connect("acald013@hn")
  for (id in ids){
    ssh_exec_wait(session, 
                  command = paste0("hdfs dfs -cat logs/pflock-", id, "/part*"),
                  std_out = paste0(getwd(), "/data/pflock-", id, ".tsv"))
  }  
  ssh_disconnect(session)
}

getTimeArray <- function(log){
  d1 = log$schedulerDelay 
  d2 = log$gettingResultTime 
  d3 = log$executorRunTime  
  d4 = log$executorDeserializeTime 
  d5 = log$resultSerializationTime 
  d6 = log$shuffleWriteTime 
  d7 = log$shuffleFetchWaitTime
  log$Time = paste(d1,d2,d3,d4,d5,d6,d7)
  log$Task = paste("Scheduler","Result","Compute","Deserialize","Serialize","ShuffleWrite","ShuffleRead")
  return(log)
}

################
# Parameters...
################

GETTING_DATA = T
ID1=1052
ID2=1221
  
ids = str_pad(seq(ID1, ID2), 4, pad = "0")

if(GETTING_DATA){
  getData(ids)
}

for (id in ids){
  filename = paste0("data/pflock-",id,".tsv")
  log = read_tsv(filename)
  
  top = log %>%
    mutate(duration = as.numeric(duration), index = as.factor(index)) %>% 
    slice_max(duration, n = 100)
    #filter(duration >= 1000)
  
  log = getTimeArray(top)
  data = log %>% separate_rows(Time, Task, sep = " ", convert = TRUE) %>%
    select(appId, jobId, stageId, index, host, duration, phaseName, Task, Time) 
  
  data0 = data
  indexOrder = data0 %>% select(index) %>% distinct() %>% mutate(nIndex = as.numeric(index)) %>% 
    arrange(nIndex) %>% select(index) %>% as.list()
  data0$index = factor(data0$index, levels = indexOrder$index)
  
  p = ggplot(data=data0, aes(x = index, y = Time, fill = Task)) +
    geom_bar(stat="identity",colour="black", size=0.1) +
    coord_flip() +
    facet_grid(host ~ ., scales = "free", space = "free") + 
    scale_fill_manual(values = c("blue", "green", "cyan", "purple", "orange", "red", "yellow")) +
    theme(strip.text.y = element_text(angle = 0)) +
    ylab("Time [ms]") + xlab("Task Id") + labs(fill = "Time to")
  
  plot(p)
  ggsave(paste0("hist/TopTasksHist_",id,".pdf"), width = 15, height = 20, device = "pdf")
}
