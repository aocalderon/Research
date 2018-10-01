#!/usr/bin/Rscript

if (!require("pacman")) install.packages("pacman")
pacman::p_load(ggplot2, stringr)
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))

dataFile = paste0(RESEARCH_HOME, 'Validation/LCM_max/nohup.csv')
readData = TRUE

if(readData){
  lines = readLines(dataFile)
  print(lines)
  data = as.data.frame(str_split_fixed(lines, ",", 3))
  names(data) = c("Dataset", "Method", "Time")
}
data$Time = as.numeric(levels(data$Time))[data$Time]
nDataset = nlevels(data$Dataset)
data$ID = rep(1:nDataset, each = 3)

data = data[40 < data$ID & data$ID < 45,]
g = ggplot(data=data, aes(x=ID, y=Time, color=Method)) + geom_line()
plot(g)