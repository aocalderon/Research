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
ID1=744
ID2=862

ids = str_pad(seq(ID1, ID2), 4, pad = "0")

if(GETTING_DATA){
  getData(ids)
}

if(READING_DATA){
  files = list.files(path = PATH, pattern = "*.tsv", full.names = T)
  tasks0 = sapply(files, read_tsv, simplify=FALSE) %>% 
    bind_rows() %>%
    select(appId, jobId, stageId, index, duration) 
}

tasks = tasks0 %>%
  group_by(appId) %>% summarise(duration = max(duration)) %>%
  arrange(duration)

log = enframe(readLines("factor.tsv"))
paramsPattern = "partition|factor"
getParams <- function(command){
  params = str_trim(str_split(command, "--")[[1]])
  params = params[grepl(paramsPattern, params)]
  return(paste(params, collapse = " "))
}

spark = log %>% filter(grepl(value, pattern = "SparkSubmit ")) %>% 
  separate(value, into = c(NA, "appId", "command"), sep = "\\|")
spark$params = spark$command %>% map(getParams)
spark = spark %>% separate(params, into = c(NA,"Partitions",NA,"Factor", NA, NA), sep = " ") %>%
  select(appId, Partitions, Factor)

log %>% filter(grepl(value, pattern = "Disks phase\\|Time"))

data = tasks %>% inner_join(spark, by = c("appId")) %>%
  group_by(Factor, Partitions) %>% summarise(Duration = mean(duration)) %>%
  mutate(Partitions = factor(Partitions, levels = c("864",  "972", "1080", "1188", "1296"))) %>%
  mutate(Factor = factor(Factor, levels = c("1",  "2", "3", "4", "5"))) %>%
  mutate(Duration = as.numeric(Duration))

p = ggplot(data = data, aes(x = Partitions, y = Duration, fill = Factor)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
  labs(x="Partitions", y="Time [ms]", title="Execution time by Longest Task duration") 
plot(p)

ggsave(paste0("TopTaskDuartion.pdf"), width = 12, height = 8, device = "pdf")

  


