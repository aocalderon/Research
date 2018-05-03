#!/usr/bin/Rscript

if (!require("pacman")) install.packages("pacman")
pacman::p_load(ggplot2, data.table, foreach, sqldf, tidyr, stringr, dplyr)
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))
PATH = "Scripts/R/SJvsML_Analysis/"
SAVE_PDF = 
W = 6
H = 6

data = read.csv(paste0(RESEARCH_HOME, PATH, 'output.csv'), header = F)
names(data) = c("Method", "Stage", "Cores", "Epsilon", "Mu", "Delta", "Time", "N")
data$Cores   = as.numeric(as.character(data$Cores))
data$Epsilon = as.numeric(as.character(data$Epsilon))
data$Mu      = as.numeric(as.character(data$Mu))
data$Delta   = as.numeric(as.character(data$Delta))
data$Time    = as.numeric(as.character(data$Time))
data$N       = as.numeric(as.character(data$N))

muDefault      = 4
deltaDefault   = 4
epsilonDefault = 35
coresDefault   = 21

dataInternal = data[data$Cores == coresDefault & data$Mu == muDefault & data$Delta == deltaDefault, c("Stage", "Epsilon", "Time", "N")]
title = "Execution time..."
g = ggplot(data=dataInternal, aes(x=factor(Epsilon), y=Time, fill=Stage)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  labs(title=title, y="Time(s)", x=expression(paste(epsilon,"(mts)")))
if(SAVE_PDF){
  ggsave("./MLInternalTime.pdf", g)
} else {
  plot(g)
}
 
title = "Number of evaluated candidates..."
g = ggplot(data=dataInternal, aes(x=factor(Epsilon), y=N, fill=Stage)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  labs(title=title, y="Time(s)", x=expression(paste(epsilon,"(mts)")))
if(SAVE_PDF){
  ggsave("./MLInternalN.pdf", g)
} else {
  plot(g)
}
