#!/usr/bin/Rscript

if (!require("pacman")) install.packages("pacman")
pacman::p_load(ggplot2, data.table, foreach, sqldf, tidyr, stringr, dplyr)
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))

dataFile = paste0(RESEARCH_HOME, 'Scripts/Python/SpatialJoinRuns.out')

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
    if(grepl("master=spark", line)){
      params = str_split_fixed(line, " -> ", 2)[2]
      params = str_split_fixed(params, ",", 6)
      
      cores   = strsplit(params[1], "=")[[1]][2]
      epsilon = strsplit(params[3], "=")[[1]][2]
      mu      = strsplit(params[4], "=")[[1]][2]
      delta   = strsplit(params[5], "=")[[1]][2]
    } else if(grepl("=SpatialJoin Start=", line)){
      params = str_split_fixed(line, "->", 2)[2]
      value = str_split_fixed(params, "Start", 2)
      method = str_sub(str_trim(value[1]), 2)
    } else if(grepl("Running SpatialJoin...", line, perl = T)){
      info = str_split_fixed(line, "->", 2)[2]
      params = str_split_fixed(info, "\\|", 3)
      time = str_trim(str_split_fixed(params[2], "s", 2)[1])
      n = str_trim(str_split_fixed(params[3], "flocks", 2)[1])
      row = paste0(method,",",cores,",", epsilon,",", mu,",", delta,",",time,",",n)
      print(row)
      records = c(records, row)
    }
  }
  data = as.data.frame(str_split_fixed(records, ",", 7))
  names(data) = c("Method", "Cores", "Epsilon", "Mu", "Delta", "Time", "N")
  data$Cores   = as.numeric(as.character(data$Cores))
  data$Epsilon = as.numeric(as.character(data$Epsilon))
  data$Mu      = as.numeric(as.character(data$Mu))
  data$Delta   = as.numeric(as.character(data$Delta))
  data$Time    = as.numeric(as.character(data$Time))
}

data = data[, c("Method", "Cores", "Epsilon", "Mu", "Delta", "Time")]
data = sqldf("SELECT Method, Cores, Epsilon, Mu, Delta, AVG(Time) AS Time FROM data GROUP BY 1, 2, 3, 4, 5 ORDER BY Cores DESC, Method, Epsilon, Mu, Delta")
write.table(data, paste0(RESEARCH_HOME,"Scripts/R/SJvsML_Analysis/SpatialJoinTimes.csv"), row.names = F, col.names = F, sep = ',')
