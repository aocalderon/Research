#!/usr/bin/Rscript

if (!require("pacman")) install.packages("pacman")
pacman::p_load(ggplot2, data.table, foreach, sqldf, tidyr, stringr, dplyr)
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))

dataFile = '/home/and/Documents/PhD/Research/Experiments/FlockFinder_v1.0/nohup.out'

lines = readLines(dataFile)
records = c()
method = ""
epsilon = 0
mu = 0
delta = 0
stage = ""
time = 0
readData = F

if(readData){
  for(line in lines){
    if(grepl("\\*\\*\\* Epsilon=", line)){
      array = str_split_fixed(line, "==", 2)
      params = array[1]
      method = str_split_fixed(array[2], "==", 2)[1]
      array = str_split_fixed(params, "=", 4)
      epsilon = str_split_fixed(array[2],", ", 2)[1]
      mu = str_split_fixed(array[3]," and ", 2)[1]
      delta = str_trim(array[4])
      #print(paste0(method,",", epsilon,",", mu,",", delta))
    } else if(grepl("\\[(.*?)s\\]", line)){
      info = str_split_fixed(line, "->", 2)[2]
      array = str_split_fixed(info, "\\[", 2)
      stage = str_trim(array[1])
      time = array[2]
      time = str_split_fixed(time, "s\\]", 2)[1]
      if(!grepl("^[0-9]*[.]",stage, perl = TRUE)){
        records = c(records, paste0(method,",", epsilon,",", mu,",", delta,",",stage,",",time))
      }
    }
  }
  data = as.data.frame(str_split_fixed(records, ",", 6))
  names(data) = c("Method", "Epsilon", "Mu", "Delta", "Stage", "Time")
  data$Stage = as.character(data$Stage)
  data[grepl("Reported location for trajectories in time [0-9]*...", data$Stage), 5] = "Extracting locations..."
  data[grepl("Set of disks for timestamp [0-9]*...", data$Stage), 5] = "Set of disks for current timestamp..."
  data[grepl("Adding new disks to D ", data$Stage), 5] = "Adding new disks to D..."
  data[grepl("Distance Join and filtering phase at timestamp [0-9]*...", data$Stage), 5] = "Distance Join and filtering phase..."
  data$Time = as.numeric(as.character(data$Time))
}

data = sqldf("SELECT Method, Stage, Epsilon, Mu, Delta, AVG(Time) AS Time FROM data GROUP BY 1, 2, 3, 4, 5")

stagesFiles = '/home/and/Documents/PhD/Research/Experiments/FlockFinder_v1.0/stagesNames2.txt'
stages = read.csv(stagesFiles)
data1 = sqldf("SELECT d.Method AS Method,Epsilon,Mu,Delta,Stage2 AS Stage,StageId,Time FROM data d JOIN stages s ON d.method=s.method AND d.stage=s.stage")
data1 = sqldf("SELECT Method,Epsilon,Mu,Delta,Stage,StageId,SUM(Time) AS Time FROM data1 GROUP BY 1,2,3,4,5,6")
data1 = sqldf("SELECT Method,Epsilon,Mu,Delta,Stage,Time FROM data1 WHERE StageId >= 6 AND StageId != 11")

muDefault = '4'
deltaDefault = '4'

dataEpsilon = data1[data1$Method == 'SpatialJoin' & data1$Mu == muDefault & data1$Delta == deltaDefault, ]  
Totals = sqldf("SELECT Method, Epsilon, Mu, Delta, '00.Total' AS Stage, SUM(Time) AS Time FROM dataEpsilon GROUP BY 1,2,3,4,5")
dataEpsilon = rbind(dataEpsilon, Totals)

title = "Execution time SpatiaJoin method."
g = ggplot(data=dataEpsilon, aes(x=factor(Epsilon), y=Time, fill=Stage)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  labs(title=title, y="Time(s)", x=expression(paste(epsilon,"(mts)"))) +
  ylim(0,35)
plot(g)

dataEpsilon = data1[data1$Method == 'MergeLast' & data1$Mu == muDefault & data1$Delta == deltaDefault, ]  
Totals = sqldf("SELECT Method, Epsilon, Mu, Delta, '00.Total' AS Stage, SUM(Time) AS Time FROM dataEpsilon GROUP BY 1,2,3,4,5")
dataEpsilon2 = rbind(dataEpsilon, Totals)
dataEpsilon2 = sqldf("SELECT Method, Epsilon, Stage, Time FROM dataEpsilon2 ORDER BY Method, Epsilon, Stage")
title = "Execution time MergeLast method."
g = ggplot(data=dataEpsilon2, aes(x=factor(Epsilon), y=Time, fill=Stage)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  labs(title=title, y="Time(s)", x=expression(paste(epsilon,"(mts)"))) +
  ylim(0,35)
plot(g)
