#!/usr/bin/Rscript

if (!require("pacman")) install.packages("pacman")
pacman::p_load(ggplot2, data.table, foreach, sqldf, tidyr, stringr, dplyr)
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))
PATH = "Scripts/R/SJvsML_Analysis/"
SAVE_PDF = F
W = 6
H = 6

data = read.csv(paste0(RESEARCH_HOME, PATH, 'outputStages.csv'), header = F)
names(data) = c("Method", "Cores", "Epsilon", "Mu", "Delta", "Stage", "Time")
data$Cores   = as.numeric(as.character(data$Cores))
data$Epsilon = as.numeric(as.character(data$Epsilon))
data$Mu      = as.numeric(as.character(data$Mu))
data$Delta   = as.numeric(as.character(data$Delta))
data$Time    = as.numeric(as.character(data$Time))
data = data[data$Time > 0,]

muDefault      = 3
deltaDefault   = 5
epsilonDefault = 10
coresDefault   = 28

dataDelta = data[data$Cores == coresDefault & data$Mu == muDefault & data$Method == "MergeLast" & data$Delta < 7
                 , c("Epsilon", "Stage", "Time", "Delta")]
title = "Execution time by delta"
g = ggplot(data=dataDelta, aes(x=factor(Epsilon), y=Time, fill=Stage)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  labs(title=title, y="Time(s)", x=expression(paste(epsilon,"(mts)"))) +
  facet_wrap(~Delta)
if(SAVE_PDF){
  ggsave("./MergeLastStagebyDelta.pdf", g)
} else {
  plot(g)
}
