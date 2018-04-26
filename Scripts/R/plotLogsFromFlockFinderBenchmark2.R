#!/usr/bin/Rscript

if (!require("pacman")) install.packages("pacman")
pacman::p_load(ggplot2, data.table, foreach, sqldf, tidyr, stringr, dplyr)
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))

dataFile = '/home/and/Documents/PhD/Research/Scripts/Python/nohup.out'

lines = readLines(dataFile)
records = c()
method = ""
epsilon = 0
mu = 0
delta = 0
stage = ""
time = 0
readData = T

if(readData){
  for(line in lines){
    if(grepl("master=spark", line)){
      params = str_split_fixed(line, " -> ", 2)[2]
      params = str_split_fixed(params, ",", 6)
      
      cores   = strsplit(params[1], "=")[[1]][2]
      epsilon = strsplit(params[3], "=")[[1]][2]
      mu      = strsplit(params[4], "=")[[1]][2]
      delta   = strsplit(params[5], "=")[[1]][2]
    } else if(grepl("Start=", line)){
      params = str_split_fixed(line, "->", 2)[2]
      value = str_split_fixed(params, "Start", 2)
      method = str_sub(str_trim(value[1]), 2)
      print(print(paste0(method,",",cores,",", epsilon,",", mu,",", delta)))
        #records = c(records, paste0(method,",", epsilon,",", mu,",", delta,",",stage,",",time))
    } else if(grepl("Running ", line, perl = T)){
      info = str_split_fixed(line, "->", 2)[2]
      print(info)
      # array = str_split_fixed(info, "\\|", 3)
      # stage = str_trim(array[1])
      # time  = str_trim(str_split_fixed(array[2], "s", 2)[1])
      # if(grepl("variant", stage, perl = T)){
      #   print(line)
      #   records = c(records, paste0(method,",", epsilon,",", mu,",", delta,",",stage,",",time))
      # }
    }
  }
  # dataT = as.data.frame(str_split_fixed(records, ",", 6))
  # names(dataT) = c("Method", "Epsilon", "Mu", "Delta", "Stage", "Time")
  # dataT$Stage = as.character(dataT$Stage)
  # dataT$Time = as.numeric(as.character(dataT$Time))
}

# dataT = dataT[, c("Method", "Epsilon", "Mu", "Delta", "Time")]
# dataT = sqldf("SELECT Method, Epsilon, Mu, Delta, AVG(Time) AS Time FROM dataT GROUP BY 1, 2, 3, 4 ORDER BY Method, Epsilon, Mu, Delta")
# 
# title = "Execution time by delta value."
# g = ggplot(data=dataT, aes(x=factor(Epsilon), y=Time, fill=Method)) +
#   geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
#   labs(title=title, y="Time(s)", x=expression(paste(epsilon,"(mts)"))) +
#   facet_wrap(~Delta) +
#   ylim(0,400)
# svg("/home/and/Documents/PhD/Research/Experiments/FlockFinder_v1.0/SJvsMLEpsilon.svg", width=W, height=H)
# plot(g)
# dev.off()
# 
# title = "Execution time by epsilon value."
# g = ggplot(data=dataT, aes(x=factor(Delta), y=Time, fill=Method)) +
#   geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
#   labs(title=title, y="Time(s)", x=expression(paste(delta,"(timestamps)"))) +
#   facet_wrap(~Epsilon) +
#   ylim(0,400)
# svg("/home/and/Documents/PhD/Research/Experiments/FlockFinder_v1.0/SJvsMLDelta.svg", width=W, height=H)
# plot(g)
# dev.off()
