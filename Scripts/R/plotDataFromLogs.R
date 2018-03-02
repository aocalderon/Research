#!/usr/bin/Rscript

if (!require("pacman")) install.packages("pacman")
pacman::p_load(ggplot2, data.table, foreach, sqldf, tidyr, stringr, dplyr)
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))

dataFile = '/home/and/Documents/PhD/Research/Experiments/FlockFinder_v1.0/nohup.out'

getDataByMethod <- function(dataFile){
  lines = readLines(dataFile)
  sj = c()
  ml = c()
  for(line in lines){
    if(grepl("SpatialJoin,", line)){
      sj = c(sj, line)
    }
    if(grepl("MergeLast,", line)){
      ml = c(ml, line)
    }
  }
  dataSJ = as.data.frame(str_split_fixed(sj, ",", 6))
  dataML = as.data.frame(str_split_fixed(ml, ",", 6))
  data = rbind(dataSJ, dataML)
  data = data[, c(1, 2, 3, 4, 6)]
  names(data) = c("Method", "Epsilon", "Mu", "Delta", "Time")
  data$Time = as.numeric(as.character(data$Time))
  data = sqldf("SELECT Method, Epsilon, Mu, Delta, avg(Time) AS Time FROM data GROUP BY 1, 2, 3, 4")

  return(data)  
}

data = getDataByMethod(dataFile) 

dataEpsilon = data[data$Mu == '3' & data$Delta == '3', ]  
temp_title = paste("(radius of disk in mts) in Berlin dataset.")
title = substitute(paste("Execution time by ", epsilon) ~ temp_title, list(temp_title = temp_title))
g = ggplot(data=dataEpsilon, aes(x=factor(Epsilon), y=Time, fill=Method)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  labs(title=title, y="Time(s)", x=expression(paste(epsilon,"(mts)")))
plot(g)

dataMu = data[data$Epsilon == '20.0' & data$Delta == '4', ]  
temp_title = paste("(number of moving objects) in Berlin dataset.")
title = substitute(paste("Execution time by ", mu) ~ temp_title, list(temp_title = temp_title))
g = ggplot(data=dataMu, aes(x=factor(Mu), y=Time, fill=Method)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  labs(title=title, y="Time(s)", x=expression(paste(mu," (# of objects)")))
plot(g)

dataDelta = data[data$Epsilon == '10.0' & data$Mu == '4', ]  
temp_title = paste("(consecutive timestamps) in Berlin dataset.")
title = substitute(paste("Execution time by ", delta) ~ temp_title, list(temp_title = temp_title))
g = ggplot(data=dataDelta, aes(x=factor(Delta), y=Time, fill=Method)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  labs(title=title, y="Time(s)", x=expression(paste(delta," (# consecutive timestamps)")))
plot(g)
