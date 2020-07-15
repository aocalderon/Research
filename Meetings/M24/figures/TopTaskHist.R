require(tidyverse)

filename = "pflock17"
log = read_tsv(paste0(filename,".tsv"))

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

top = log %>%
  mutate(duration = as.numeric(duration), index = as.factor(index)) %>% 
  slice_max(duration, n = 1000)

log = getTimeArray(top)
data = log %>% separate_rows(Time, Task, sep = " ", convert = TRUE) %>%
  select(appId, jobId, stageId, index, host, duration, phaseName, Task, Time) 

data0 = data #%>% filter(phaseName == "Merge DCELs" & duration > 500)
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
ggsave(paste0("TopTasksHist_",filename,".pdf"), width = 10, height = 12, device = "pdf")
