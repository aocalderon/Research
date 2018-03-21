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
    if(grepl("method=", line)){
      params = str_split_fixed(line, " -> ", 2)[2]
      params = str_split_fixed(params, ",", 5)
      
      dataset = strsplit(params[1], "=")[[1]][2]
      method  = strsplit(params[2], "=")[[1]][2]
      epsilon = strsplit(params[3], "=")[[1]][2]
      mu      = strsplit(params[4], "=")[[1]][2]
      delta   = strsplit(params[5], "=")[[1]][2]
      # print(paste0(method,",", epsilon,",", mu,",", delta))
    } else if(grepl("\\|", line, perl = T)){
      info = str_split_fixed(line, "->", 2)[2]
      array = str_split_fixed(info, "\\|", 3)
      stage = str_trim(array[1])
      time  = str_trim(str_split_fixed(array[2], "s", 2)[1])
      if(grepl("^[A|B|C|D]\\.", stage, perl = T)){
        records = c(records, paste0(method,",", epsilon,",", mu,",", delta,",",stage,",",time))
      }
    }
  }
  data = as.data.frame(str_split_fixed(records, ",", 6))
  names(data) = c("Method", "Epsilon", "Mu", "Delta", "Stage", "Time")
  data$Stage = as.character(data$Stage)
  data$Time = as.numeric(as.character(data$Time))
}

data = sqldf("SELECT Method, Stage, Epsilon, Mu, Delta, AVG(Time) AS Time FROM data GROUP BY 1, 2, 3, 4, 5 ORDER BY Method, Epsilon, Stage, Mu, Delta")

deltaDefault   = "5"
muDefault      = "3"
epsilonDefault = "50.0"

dataSJ = data[data$Method=="SpatialJoin" & data$Delta==deltaDefault, ]
title = "Execution time SpatialJoin by epsilon value."
g = ggplot(data=dataSJ, aes(x=factor(Epsilon), y=Time, fill=Stage)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  labs(title=title, y="Time(s)", x=expression(paste(epsilon,"(mts)"))) +
  ylim(0,25)
W = 12
H = 9
svg("/home/and/Documents/PhD/Research/Experiments/FlockFinder_v1.0/EpsilonSJ_stages.svg", width=W, height=H)
plot(g)
dev.off()

dataSJ = data[data$Method=="SpatialJoin" & data$Epsilon==epsilonDefault, ]
title = "Execution time SpatialJoin by delta value."
g = ggplot(data=dataSJ, aes(x=factor(Delta), y=Time, fill=Stage)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  labs(title=title, y="Time(s)", x=expression(paste(delta,"(timestamps)"))) +
  ylim(0,25)
svg("/home/and/Documents/PhD/Research/Experiments/FlockFinder_v1.0/DeltaSJ_stages.svg", width=W, height=H)
plot(g)
dev.off()

dataML = data[data$Method=="MergeLast" & data$Delta==deltaDefault, ]
title = "Execution time MergeLast by epsilon value."
g = ggplot(data=dataML, aes(x=factor(Epsilon), y=Time, fill=Stage)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  labs(title=title, y="Time(s)", x=expression(paste(epsilon,"(mts)"))) +
  ylim(0,25)
svg("/home/and/Documents/PhD/Research/Experiments/FlockFinder_v1.0/EpsilonML_stages.svg", width=W, height=H)
plot(g)
dev.off()

dataML = data[data$Method=="MergeLast" & data$Epsilon==epsilonDefault, ]
title = "Execution time MergeLast by delta value"
g = ggplot(data=dataML, aes(x=factor(Delta), y=Time, fill=Stage)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  labs(title=title, y="Time(s)", x=expression(paste(delta,"(timestamps)"))) +
  ylim(0,25)
svg("/home/and/Documents/PhD/Research/Experiments/FlockFinder_v1.0/DeltaML_stages.svg", width=W, height=H)
plot(g)
dev.off()
