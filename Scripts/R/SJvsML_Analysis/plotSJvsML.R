#!/usr/bin/Rscript

if (!require("pacman")) install.packages("pacman")
pacman::p_load(ggplot2, data.table, foreach, sqldf, tidyr, stringr, dplyr)
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))
PATH = "Scripts/R/SJvsML_Analysis/"
SAVE_PDF = F
W = 6
H = 6

data = read.csv(paste0(RESEARCH_HOME, PATH, 'output.csv'), header = F)
names(data) = c("Method", "Cores", "Epsilon", "Mu", "Delta", "Time")
data$Cores   = as.numeric(as.character(data$Cores))
data$Epsilon = as.numeric(as.character(data$Epsilon))
data$Mu      = as.numeric(as.character(data$Mu))
data$Delta   = as.numeric(as.character(data$Delta))
data$Time    = as.numeric(as.character(data$Time))

muDefault      = 3
deltaDefault   = 5
epsilonDefault = 10
coresDefault   = 28

dataDelta = data[data$Cores == coresDefault & data$Mu == muDefault, ]
title = "Execution time by delta"
g = ggplot(data=dataDelta, aes(x=factor(Epsilon), y=Time, fill=Method)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  labs(title=title, y="Time(s)", x=expression(paste(epsilon,"(mts)"))) +
  facet_wrap(~Delta)
if(SAVE_PDF){
  ggsave("./SJvsMLbyDelta.pdf", g)
} else {
  plot(g)
}
 
dataMu = data[data$Cores == coresDefault & data$Delta == deltaDefault, ]
title = "Execution time by mu"
g = ggplot(data=dataMu, aes(x=factor(Epsilon), y=Time, fill=Method)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  labs(title=title, y="Time(s)", x=expression(paste(epsilon,"(mts)"))) +
  facet_wrap(~Mu)
if(SAVE_PDF){
  ggsave("./SJvsMLbyMu.pdf", g)
} else {
  plot(g)
}

dataSpeedup = data[data$Mu == muDefault & data$Delta == deltaDefault, ]
title = "Speedup"
g = ggplot(data=dataSpeedup, aes(x=factor(Cores), y=Time, fill=Method)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  labs(title=title, y="Time(s)", x=expression(paste(epsilon,"(mts)"))) +
  facet_wrap(~Epsilon)
if(SAVE_PDF){
  ggsave("./SJvsMLbyCores.pdf", g)
} else {
  plot(g)
}
