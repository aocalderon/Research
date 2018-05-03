#!/usr/bin/Rscript

if (!require("pacman")) install.packages("pacman")
pacman::p_load(ggplot2, data.table, foreach, sqldf, tidyr, stringr, dplyr)
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))

dataFile = paste0(RESEARCH_HOME, 'Scripts/Python/MergeLastRuns.out')

lines = readLines(dataFile)
records = c()
method = ""
epsilon = 0
mu = 0
delta = 0
stage = ""
time = 0
readData = T

if(readData){
  for(line in lines){
    if(grepl("method=", line)){
      params = str_split_fixed(line, " -> ", 2)[2]
      params = str_split_fixed(params, ",", 6)
      
      method  = strsplit(params[1], "=")[[1]][2]
      cores   = strsplit(params[2], "=")[[1]][2]
      epsilon = strsplit(params[3], "=")[[1]][2]
      mu      = strsplit(params[4], "=")[[1]][2]
      delta   = strsplit(params[5], "=")[[1]][2]
    } else if(grepl("Getting maximal disks", line)){
      info = str_split_fixed(line, "->", 2)[2]
      params = str_split_fixed(info, "\\|", 3)
      stage = str_trim(str_split_fixed(params[1], ":", 2)[1])
      time = str_trim(str_split_fixed(params[2], "s", 2)[1])
      n = str_trim(str_split_fixed(params[3], "disks", 2)[1])
      row = paste0(method,",",cores,",", epsilon,",", mu,",", delta,",",stage,",",time,",",n)
      print(row)
      records = c(records, row)
    } else if(grepl("Checking internal timestamps", line)){
      info = str_split_fixed(line, "->", 2)[2]
      params = str_split_fixed(info, "\\|", 3)
      stage = str_trim(str_split_fixed(params[1], "\\.", 2)[1])
      time = str_trim(str_split_fixed(params[2], "s", 2)[1])
      n = str_trim(str_split_fixed(params[3], "flocks", 2)[1])
      row = paste0(method,",",cores,",", epsilon,",", mu,",", delta,",",stage,",",time,",",n)
      print(row)
      records = c(records, row)
    }    
  }
  data = as.data.frame(str_split_fixed(records, ",", 8))
  names(data) = c("Method", "Cores", "Epsilon", "Mu", "Delta", "Stage", "Time", "N")
  data$Cores   = as.numeric(as.character(data$Cores))
  data$Epsilon = as.numeric(as.character(data$Epsilon))
  data$Mu      = as.numeric(as.character(data$Mu))
  data$Delta   = as.numeric(as.character(data$Delta))
  data$Time    = as.numeric(as.character(data$Time))
}

data = data[, c("Method", "Stage", "Cores", "Epsilon", "Mu", "Delta", "Time", "N")]
data = sqldf("SELECT Method, Stage, Cores, Epsilon, Mu, Delta, AVG(Time), MAX(N) AS Time FROM data GROUP BY 1, 2, 3, 4, 5, 6 ORDER BY Cores DESC, Method, Epsilon, Mu, Delta")
#write.table(data, paste0(RESEARCH_HOME,"Scripts/R/SJvsML_Analysis/output.csv"), row.names = F, col.names = F, sep = ',')
