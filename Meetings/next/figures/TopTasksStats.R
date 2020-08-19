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

getCells <- function(ids){
  session <- ssh_connect("acald013@hn")
  for (id in ids){
    ssh_exec_wait(session, 
                  command = paste0("cat /tmp/edgesCells", id, ".tsv"),
                  std_out = paste0(getwd(), "/data/cells-", id, ".tsv"))
  }
  ssh_disconnect(session)
}

################
# Parameters...
################

PATH = paste0(getwd(), "/data/")
GETTING_DATA  = F
READING_DATA  = F
GETTING_CELLS = F
READING_CELLS = F
ID1=1242
ID2=1321
  
# for(id in seq(ID1, ID2)){
#   filename = paste0(getwd(), "/data/cells-", id, ".tsv")
#   d = read_tsv(filename, col_names = F)
#   d$appId = id
#   write_tsv(d, filename, col_names = F)
# }

ids = str_pad(seq(ID1, ID2), 4, pad = "0")

# Getting cell info...
if(GETTING_CELLS){
  getCells(ids)
}
if(READING_CELLS){
  files = list.files(path = PATH, pattern = "cells-[0-9]*.tsv", full.names = T)
  cells = sapply(files, read_tsv, col_names = F, simplify = FALSE) %>%
    bind_rows()
  names(cells) = c("wkt", "partitionId", "count", "appId")
  cells = cells %>% mutate(appId = as.character(appId), partitionId = as.character(partitionId))
}

# Getting task table...
if(GETTING_DATA){
  getData(ids)
}
if(READING_DATA){
  files = list.files(path = PATH, pattern = "pflock-[0-9]*.tsv", full.names = T)
  tasks0 = sapply(files, read_tsv, simplify = FALSE) %>%
    bind_rows() %>%
    mutate(appId = as.character(appId))
}
tasks = tasks0 %>% 
  select(appId, jobId, stageId, index, host, launchTime, finishTime, duration, resultSize, taskLocality) %>%
  mutate(index = as.character(index))

# Getting parameters table...
log = enframe(readLines("stats.txt"))
paramsPattern = "partition"
getParams <- function(command){
  params = str_trim(str_split(command, "--")[[1]])
  params = params[grepl(paramsPattern, params)]
  return(paste(params, collapse = " "))
}
spark = log %>% filter(grepl(value, pattern = "SparkSubmit ")) %>% 
  separate(value, into = c(NA, "appId", "command"), sep = "\\|")
spark$params = spark$command %>% map(getParams)
spark = spark %>% separate(params, into = c(NA,"Partitions",NA, "NPartitions"), sep = " ") %>%
  select(appId, Partitions, NPartitions)

# Getting partitions general stats...
fields = c("timestamp", "tag", "appId", "max", "min", "avg", "sd")
gstats = log %>% filter(grepl(value, pattern = "\\|STATS\\|")) %>%
  separate(value, into = fields, sep = "\\|") %>%
  select(appId, max, min, avg, sd)

# Getting partitions individual stats...
fields = c("tag", "appId", "partitionId", "index", "pointsIn", "pointsOut", "centersIn", "centeresOut")
stats = log %>% filter(grepl(value, pattern = "CELLSTATS")) %>%
  separate(value, into = fields, sep = "\t") 

####

data = spark %>% 
  inner_join(cells, by = c("appId")) %>% 
  inner_join(gstats, by = c("appId")) %>%
  inner_join(stats, by = c("appId", "partitionId")) %>%
  inner_join(tasks, by = c("appId", "index"))

data1 = data %>% 
  group_by(wkt, partitionId, Partitions, NPartitions, pointsIn, pointsOut, centersIn, centeresOut) %>% 
  summarise(duration = mean(duration))

write_tsv(x = data1, path = "partitions.wkt")
P = unique(data1$Partitions)
for(p in P){
  write_tsv(x = data1 %>% filter(Partitions == p), path = paste0("partitions-",str_pad(p,3,pad="0"),".wkt"))
  write_tsv(x = data1 %>% filter(Partitions == p & duration >= 1000), path = paste0("partitionsM-",str_pad(p,3,pad="0"),".wkt"))
}