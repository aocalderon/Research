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

################
# Parameters...
################

GETTING_DATA = F
READING_DATA = F
PATH=paste0(getwd(), "/data")
ID1=1052
ID2=1221

ids = str_pad(seq(ID1, ID2), 4, pad = "0")

if(GETTING_DATA){
  getData(ids)
}

if(READING_DATA){
  # files = list.files(path = PATH, pattern = "pflock-0[0-9]*.tsv", full.names = T)
  # tasks0 = sapply(files, read_tsv, simplify = FALSE) %>%
  #   bind_rows() %>%
  #   select(appId, jobId, stageId, index, host, launchTime, finishTime) 
  files = list.files(path = PATH, pattern = "pflock-[0-9]*.tsv", full.names = T)
  tasks0 = sapply(files, read_tsv, simplify = FALSE) %>%
    bind_rows() %>%
    select(appId, jobId, stageId, index, host, launchTime, finishTime)  %>%
    mutate(appId = as.character(appId))
}

tasks = tasks0 %>% group_by(appId, host) %>% 
  summarise(start = min(launchTime), finish = max(finishTime)) %>%
  mutate(duration = (finish - start) / 1000.0)

log = enframe(readLines("factor2.tsv"))
paramsPattern = "factor|npartition"
getParams <- function(command){
  params = str_trim(str_split(command, "--")[[1]])
  params = params[grepl(paramsPattern, params)]
  return(paste(params, collapse = " "))
}

spark = log %>% filter(grepl(value, pattern = "SparkSubmit ")) %>%
  separate(value, into = c(NA, "appId", "command"), sep = "\\|")
spark$params = spark$command %>% map(getParams)
spark = spark %>% separate(params, into = c(NA,"Factor",NA,"Partitions"), sep = " ") %>%
  select(appId, Partitions, Factor)

data = spark %>% inner_join(tasks, by = c("appId")) %>%
  filter(Factor != "0") %>%
  mutate(Factor = replace(Factor, Factor == "-1", "0")) %>%
  mutate(Factor = factor(Factor, levels = c("0", "1",  "2", "3", "4", "5", "6","7", "8", "9", "10", "12", "14", "16", "18", "20")))

data1 = data %>%
  group_by(Factor, host) %>% summarise(duration = mean(duration))
p = ggplot(data = data1, aes(x = Factor, y = duration, fill = host)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  labs(x="Factor", y="Time [s]", title="Execution time by Nodes") +
  scale_fill_discrete(name="Nodes")
plot(p)
ggsave(paste0("TimePerNode.pdf"), width = 12, height = 8, device = "pdf")

data2 = data %>%
  group_by(Factor) %>% summarise(time = mean(duration), sd = sd(duration))
q = ggplot(data = data2, aes(x = Factor, y = time)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) +
  geom_errorbar(aes(x = Factor, ymin = time - sd, ymax = time + sd), colour="red", width = 0.4) +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  labs(x="Factor", y="Time [s]", title="Execution time by Nodes (Avg)")
plot(q)
ggsave(paste0("TimePerNodeAvg.pdf"), width = 12, height = 8, device = "pdf")
