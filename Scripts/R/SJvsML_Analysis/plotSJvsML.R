#!/usr/bin/Rscript

if (!require("pacman")) install.packages("pacman")
pacman::p_load(ggplot2, data.table, foreach, sqldf, tidyr, stringr, dplyr)
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))
PATH = "Scripts/R/SJvsML_Analysis/"
SAVE_PDF = T
W = 800
H = 600

dataSJ = read.csv(paste0(RESEARCH_HOME, PATH, 'SpatialJoinTimes.csv'), header = F)
dataML = read.csv(paste0(RESEARCH_HOME, PATH, 'MergeLastTimes.csv'), header = F)
data = rbind(dataSJ, dataML) 
names(data) = c("Method", "Cores", "Epsilon", "Mu", "Delta", "Time")
data$Cores   = as.numeric(as.character(data$Cores))
data$Epsilon = as.numeric(as.character(data$Epsilon))
data$Mu      = as.numeric(as.character(data$Mu))
data$Delta   = as.numeric(as.character(data$Delta))
data$Time    = as.numeric(as.character(data$Time))

data = data[data$Cores == 28, ]

title = "Execution time by delta"
g = ggplot(data=data, aes(x=factor(Epsilon), y=Time, fill=Method)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  labs(title=title, y="Time(s)", x=expression(paste(epsilon,"(mts)"))) +
  facet_wrap(~Delta)
if(SAVE_PDF){
  ggsave("./SJvsMLbyDelta.pdf", g)
} else {
  plot(g)
}
 
title = "Execution time by mu"
g = ggplot(data=data, aes(x=factor(Epsilon), y=Time, fill=Method)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  labs(title=title, y="Time(s)", x=expression(paste(epsilon,"(mts)"))) +
  facet_wrap(~Mu)
if(SAVE_PDF){
  ggsave("./SJvsMLbyMu.pdf", g)
} else {
  plot(g)
}

