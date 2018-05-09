#!/usr/bin/Rscript

if (!require("pacman")) install.packages("pacman")
pacman::p_load(ggplot2, data.table, foreach, sqldf, tidyr, stringr, dplyr)
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))

dataFile = paste0(RESEARCH_HOME, 'Scripts/Python/nohup.out')

lines = readLines(dataFile)
records = c()
method = ""
epsilon = 0
mu = 0
delta = 0
stage = ""
time = 0
readData = T
f1s = c(0)
f2s = c(0)
stages = c()

if(readData){
  for(line in lines){
    if(grepl("method=", line)){
      params = str_split_fixed(line, " -> ", 2)[2]
      params = str_split_fixed(params, ",", 7)
      
      method  = strsplit(params[1], "=")[[1]][2]
      cores   = strsplit(params[2], "=")[[1]][2]
      epsilon = strsplit(params[3], "=")[[1]][2]
      mu      = strsplit(params[4], "=")[[1]][2]
      delta   = strsplit(params[5], "=")[[1]][2]
      time    = strsplit(params[6], "=")[[1]][2]
      f1_mean = mean(f1s)
      f2_mean = mean(f2s)
      row = paste0(method,",",cores,",", epsilon,",", mu,",", delta,",",time,",",n,",",f1_mean,",",f2_mean)
      for(stage in stages){
        print(paste0(row,",",stage))
        records = c(records, paste0(row,",",stage))
      }
      #print(row)
      records = c(records, row)
      f1s = c(0)
      f2s = c(0)
      stages = c()
    } else if(grepl("Running MergeLast...", line, perl = T)){
      info = str_split_fixed(line, "->", 2)[2]
      params = str_split_fixed(info, "\\|", 3)
      #time = str_trim(str_split_fixed(params[2], "s", 2)[1])
      n = str_trim(str_split_fixed(params[3], "flocks", 2)[1])
    } else if(grepl("Running Spatial...", line, perl = T)){
      info = str_split_fixed(line, "->", 2)[2]
      params = str_split_fixed(info, "\\|", 3)
      #time = str_trim(str_split_fixed(params[2], "s", 2)[1])
      n = str_trim(str_split_fixed(params[3], "flocks", 2)[1])
    }else if(grepl(" -\\> \\d+\\.", line, perl = T)){
      #print(line)
      info = str_split_fixed(line, "->", 2)[2]
      params = str_split_fixed(info, "\\|", 3)
      stage = str_trim(params[1])
      timeStage = str_trim(str_split_fixed(params[2], "s", 2)[1])
      m = str_split_fixed(str_trim(params[3]), " ", 2)[1]
      stageStr = paste0(stage,",",timeStage,",",m)
      stages = c(stages, stageStr)
      #print(stageStr)
    } else if(grepl("F1Count=", line, perl = T)){
      info = str_split_fixed(line, "->", 2)[2]
      params = str_split_fixed(info, ",", 2)
      f1 = as.numeric(str_trim(str_split_fixed(params[1], "=", 2)[2]))
      f2 = as.numeric(str_trim(str_split_fixed(params[2], "=", 2)[2]))
      f1s = c(f1s, f1)
      f2s = c(f2s, f2)
      #print(paste0("F1:",f1," F2:",f2))
    }
  }
  data = as.data.frame(str_split_fixed(records, ",", 12))
  names(data) = c("Method", "Cores", "Epsilon", "Mu", "Delta", "Time", "N", "F1_avg", "F2_avg","Stage","TimeStage","NStage")
  data$Cores   = as.numeric(as.character(data$Cores))
  data$Epsilon = as.numeric(as.character(data$Epsilon))
  data$Mu      = as.numeric(as.character(data$Mu))
  data$Delta   = as.numeric(as.character(data$Delta))
  data$Time    = as.numeric(as.character(data$Time))
}

data = data[, c("Method", "Cores", "Epsilon", "Mu", "Delta", "Stage", "TimeStage")]
data = sqldf("SELECT Method, Cores, Epsilon, Mu, Delta, Stage, AVG(TimeStage) AS Time FROM data GROUP BY 1, 2, 3, 4, 5, 6 ORDER BY Cores DESC, Method, Stage, Epsilon, Mu, Delta")
write.table(data, paste0(RESEARCH_HOME,"Scripts/R/SJvsML_Analysis/outputStages.csv"), row.names = F, col.names = F, sep = ',')
