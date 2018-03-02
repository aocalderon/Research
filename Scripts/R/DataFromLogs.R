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
